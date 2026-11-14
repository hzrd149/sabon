package docker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/davidborzek/sabon/internal/discovery"
	"github.com/davidborzek/sabon/internal/engine"
	"github.com/davidborzek/sabon/internal/mover"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"golang.org/x/sys/unix"
)

var _ engine.ColdAcquirer = (*Quiescer)(nil)
var _ engine.ColdPreflighter = (*Quiescer)(nil)
var _ engine.Recoverer = (*Quiescer)(nil)

type coldRecord struct {
	Version    int      `json:"version"`
	Operation  string   `json:"operation"`
	Container  string   `json:"container"`
	WasRunning bool     `json:"was_running"`
	Sources    []string `json:"sources"`
	Phase      string   `json:"phase"`
	MoverName  string   `json:"mover_name,omitempty"`
}

type coldLease struct {
	q        *Quiescer
	dir      string
	record   coldRecord
	lock     *os.File
	mu       sync.Mutex
	released bool
	result   error
}

// PreflightCold checks durable storage and daemon identity without touching
// containers or recovering existing operations. AcquireCold validates again.
func (q *Quiescer) PreflightCold(ctx context.Context) error {
	_, err := q.coldDir(ctx)
	return err
}

// coldDir deliberately has no ephemeral fallback. Daemon identity, rather than
// Instance, prevents distinct instance selectors from bypassing ownership.
func (q *Quiescer) coldDir(ctx context.Context) (string, error) {
	if q.stateDir == "" {
		return "", fmt.Errorf("standalone cold operations require SABON_STATE_DIR on shared durable local storage")
	}
	if !filepath.IsAbs(q.stateDir) {
		return "", fmt.Errorf("SABON_STATE_DIR must be absolute")
	}
	info, err := q.cli.Info(ctx)
	if err != nil {
		return "", fmt.Errorf("identify Docker daemon: %w", err)
	}
	if info.ID == "" {
		return "", fmt.Errorf("docker daemon has no stable identity")
	}
	dir := filepath.Join(q.stateDir, "cold-v1", coldKey(info.ID))
	// Sync every created directory's parent as well as the leaf.
	for _, p := range []string{q.stateDir, filepath.Join(q.stateDir, "cold-v1"), dir} {
		if err := os.Mkdir(p, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return "", err
		}
		if err := syncColdDir(filepath.Dir(p)); err != nil {
			return "", err
		}
	}
	return dir, syncColdDir(dir)
}

func coldSources(insp container.InspectResponse) []string {
	var sources []string
	for _, m := range insp.Mounts {
		if m.Name != "" {
			sources = append(sources, "volume:"+m.Name)
		} else if m.Source != "" {
			sources = append(sources, "bind:"+filepath.Clean(m.Source))
		}
	}
	return sources
}

func coldOverlap(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
			if strings.HasPrefix(x, "bind:") && strings.HasPrefix(y, "bind:") && (strings.HasPrefix(x, strings.TrimRight(y, "/")+"/") || strings.HasPrefix(y, strings.TrimRight(x, "/")+"/")) {
				return true
			}
		}
	}
	return false
}

func (q *Quiescer) AcquireCold(ctx context.Context, ref string, extra []discovery.Source) (engine.ColdLease, error) {
	dir, err := q.coldDir(ctx)
	if err != nil {
		return nil, err
	}
	// A best-effort sweep resolves abandoned records, including removed containers.
	// Any unresolved overlap is rejected below, rather than overwriting intent.
	_, _ = q.RecoverQuiesced(ctx)
	insp, err := q.cli.ContainerInspect(ctx, ref)
	if err != nil {
		return nil, err
	}
	if insp.ContainerJSONBase == nil || insp.ID == "" {
		return nil, fmt.Errorf("container identity unavailable")
	}
	lock, err := coldLock(ctx, filepath.Join(dir, coldKey(insp.ID)+".lock"), true)
	if err != nil {
		return nil, err
	}
	owned := false
	defer func() {
		if !owned {
			_ = lock.Close()
		}
	}()
	// Serialise admission across container IDs: a recreated container or another
	// container sharing a volume must not bypass an outstanding source hold.
	gate, err := coldLock(ctx, filepath.Join(dir, "admission.lock"), true)
	if err != nil {
		return nil, err
	}
	// Successful admission explicitly closes the gate below and checks its
	// error. This fallback closes it on earlier failures or after that close.
	defer func() { _ = gate.Close() }()
	insp, err = q.cli.ContainerInspect(ctx, insp.ID)
	if err != nil {
		return nil, err
	}
	if insp.ContainerJSONBase == nil || insp.State == nil || insp.State.Paused || insp.State.Restarting || insp.State.Dead || (insp.State.Status != "running" && insp.State.Status != "created" && insp.State.Status != "exited") {
		return nil, fmt.Errorf("container has unsupported cold state")
	}
	records, err := readColdRecords(dir)
	if err != nil {
		return nil, err
	}
	sources := coldSources(insp)
	for _, src := range extra {
		switch src.Type {
		case mount.TypeVolume:
			sources = append(sources, "volume:"+src.Ref)
		case mount.TypeBind:
			sources = append(sources, "bind:"+filepath.Clean(src.Ref))
		}
	}
	for _, r := range records {
		if r.Container == insp.ID || coldOverlap(sources, r.Sources) {
			return nil, fmt.Errorf("cold recovery pending for operation %s", r.Operation)
		}
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, err
	}
	l := &coldLease{q: q, dir: dir, lock: lock, record: coldRecord{Version: 1, Operation: hex.EncodeToString(token[:]), Container: insp.ID, WasRunning: insp.State.Running, Sources: sources, Phase: "stop-intent"}}
	if err := writeColdRecord(dir, &l.record); err != nil {
		return nil, err
	}
	// Admission is now durable. Independent containers can proceed while this
	// container drains; only its lifetime lock remains held.
	if err := gate.Close(); err != nil {
		return nil, err
	}
	if insp.State.Running {
		if err := q.cli.ContainerStop(ctx, insp.ID, container.StopOptions{}); err != nil {
			return nil, fmt.Errorf("stop failed; durable recovery pending: %w", err)
		}
	}
	insp, err = q.cli.ContainerInspect(ctx, insp.ID)
	if err != nil {
		return nil, err
	}
	if insp.ContainerJSONBase == nil || insp.State == nil || insp.State.Running || insp.State.Restarting || (insp.State.Status != "created" && insp.State.Status != "exited") {
		return nil, fmt.Errorf("container not confirmed stopped; durable recovery pending")
	}
	l.record.Phase = "stopped"
	if err := writeColdRecord(dir, &l.record); err != nil {
		return nil, err
	}
	owned = true
	return l, nil
}

func (l *coldLease) AttachMover(ctx context.Context, req *mover.Request) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.released || l.record.Phase != "stopped" {
		return fmt.Errorf("cold lease cannot attach another mover")
	}
	l.record.MoverName = "sabon-cold-" + l.record.Operation
	l.record.Phase = "mover-intent"
	if err := writeColdRecord(l.dir, &l.record); err != nil {
		return err
	}
	req.Name = l.record.MoverName
	if req.Labels == nil {
		req.Labels = map[string]string{}
	}
	req.Labels[mover.LabelColdOperation] = l.record.Operation
	req.Labels[mover.LabelColdContainer] = l.record.Container
	req.KeepOnShutdown = false
	return nil
}

func (l *coldLease) Release(ctx context.Context) (resultErr error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return l.result
	}
	l.released = true
	defer func() {
		resultErr = errors.Join(resultErr, l.lock.Close())
		l.result = resultErr
	}()
	return l.q.recoverCold(ctx, l.dir, &l.record)
}

func (l *coldLease) CheckIdle(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return l.q.coldMoverIdle(ctx, &l.record)
}

func (q *Quiescer) coldMoverIdle(ctx context.Context, r *coldRecord) error {
	if r.MoverName != "" {
		list, err := q.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: filters.NewArgs(filters.Arg("label", mover.LabelColdOperation+"="+r.Operation))})
		if err != nil {
			return err
		}
		// Also inspect the reserved name: a mismatch is not evidence of absence.
		named, err := q.cli.ContainerInspect(ctx, r.MoverName)
		if err != nil && !cerrdefs.IsNotFound(err) {
			return err
		}
		if err == nil {
			if named.ContainerJSONBase == nil || named.Config == nil || named.Config.Labels[mover.LabelColdOperation] != r.Operation || named.Config.Labels[mover.LabelColdContainer] != r.Container {
				return fmt.Errorf("cold mover identity mismatch")
			}
			found := false
			for _, c := range list {
				if c.ID == named.ID {
					found = true
				}
			}
			if !found {
				list = append(list, container.Summary{ID: named.ID})
			}
		}
		for _, c := range list {
			insp, err := q.cli.ContainerInspect(ctx, c.ID)
			if cerrdefs.IsNotFound(err) {
				continue
			}
			if err != nil {
				return err
			}
			if insp.ContainerJSONBase == nil || insp.State == nil || insp.State.Running || insp.State.Restarting || insp.State.Paused || insp.State.Status == "removing" {
				return fmt.Errorf("cold mover %s still active or uncertain", c.ID)
			}
			// Remove never-started movers to fence an outstanding Start request.
			if insp.State.Status == "created" {
				if err := q.cli.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true}); err != nil && !cerrdefs.IsNotFound(err) {
					return err
				}
				if _, err := q.cli.ContainerInspect(ctx, c.ID); !cerrdefs.IsNotFound(err) {
					return fmt.Errorf("cold mover removal not confirmed: %v", err)
				}
			} else if insp.State.Status != "exited" && insp.State.Status != "dead" {
				return fmt.Errorf("cold mover has uncertain state %q", insp.State.Status)
			}
		}
	}
	return nil
}

func (q *Quiescer) recoverCold(ctx context.Context, dir string, r *coldRecord) error {
	// Bound fresh cleanup contexts supplied by orchestrators.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, checker := range q.hookCheckers {
		if err := checker.CheckHookCompletion(ctx); err != nil {
			return fmt.Errorf("cold recovery deferred until hook exit: %w", err)
		}
	}
	if err := q.coldMoverIdle(ctx, r); err != nil {
		return err
	}
	insp, err := q.cli.ContainerInspect(ctx, r.Container)
	if cerrdefs.IsNotFound(err) {
		return removeColdRecord(dir, r)
	}
	if err != nil {
		return err
	}
	if insp.ContainerJSONBase == nil || insp.State == nil || insp.State.Restarting || insp.State.Paused || (insp.State.Status != "running" && insp.State.Status != "created" && insp.State.Status != "exited") {
		return fmt.Errorf("original container state uncertain")
	}
	// An unacknowledged stop still showing running may finish later. Do not
	// discard its intent or issue a competing Start based on one observation.
	if r.Phase == "stop-intent" && r.WasRunning && insp.State.Running {
		return fmt.Errorf("unacknowledged stop remains ambiguous; recovery pending")
	}
	if r.WasRunning && !insp.State.Running {
		r.Phase = "resume-intent"
		if err := writeColdRecord(dir, r); err != nil {
			return err
		}
		if err := q.cli.ContainerStart(ctx, r.Container, container.StartOptions{}); err != nil {
			return err
		}
		insp, err = q.cli.ContainerInspect(ctx, r.Container)
		if err != nil {
			return err
		}
		if insp.ContainerJSONBase == nil || insp.State == nil || !insp.State.Running || insp.State.Restarting || insp.State.Paused {
			return fmt.Errorf("original container restart not confirmed")
		}
	}
	return removeColdRecord(dir, r)
}

func (q *Quiescer) RecoverQuiesced(ctx context.Context) (int, error) {
	if q.stateDir == "" {
		return 0, nil
	} // non-cold installations need no state
	dir, err := q.coldDir(ctx)
	if err != nil {
		return 0, err
	}
	records, err := readColdRecords(dir)
	if err != nil {
		return 0, err
	}
	n := 0
	var errs []error
	for _, r := range records {
		lock, err := coldLock(ctx, filepath.Join(dir, coldKey(r.Container)+".lock"), false)
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// Reload after ownership acquisition; the previous owner may have finished
		// or replaced the record between the directory scan and this lock.
		current, readErr := readColdRecords(dir)
		if readErr != nil {
			errs = append(errs, readErr)
		} else {
			for _, live := range current {
				if live.Container == r.Container {
					if err := q.recoverCold(ctx, dir, &live); err != nil {
						errs = append(errs, fmt.Errorf("cold operation %s: %w", live.Operation, err))
					} else {
						n++
					}
				}
			}
		}
		_ = lock.Close()
	}
	return n, errors.Join(errs...)
}
