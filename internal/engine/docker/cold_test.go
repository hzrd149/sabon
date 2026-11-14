package docker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/davidborzek/sabon/internal/discovery"
	"github.com/davidborzek/sabon/internal/mover"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/system"
	"github.com/docker/docker/client"
)

type coldClient struct {
	client.APIClient
	mu                                    sync.Mutex
	containers                            map[string]container.InspectResponse
	aliases                               map[string]string
	starts, stops                         int
	startErr, stopErr, listErr, removeErr error
	beforeStop                            func()
}

func newColdClient(running bool) *coldClient {
	f := &coldClient{containers: map[string]container.InspectResponse{}, aliases: map[string]string{"app": "original"}}
	f.put("original", running, nil)
	return f
}
func (f *coldClient) put(id string, running bool, labels map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	status := "exited"
	if running {
		status = "running"
	}
	f.containers[id] = container.InspectResponse{ContainerJSONBase: &container.ContainerJSONBase{ID: id, State: &container.State{Running: running, Status: status}}, Config: &container.Config{Labels: labels}, Mounts: []container.MountPoint{{Type: mount.TypeVolume, Name: "data"}}}
}
func (f *coldClient) Info(context.Context) (system.Info, error) {
	return system.Info{ID: "test-daemon"}, nil
}
func (f *coldClient) ContainerInspect(_ context.Context, id string) (container.InspectResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if alias := f.aliases[id]; alias != "" {
		id = alias
	}
	c, ok := f.containers[id]
	if !ok {
		return container.InspectResponse{}, cerrdefs.ErrNotFound
	}
	b := *c.ContainerJSONBase
	s := *c.State
	b.State = &s
	c.ContainerJSONBase = &b
	return c, nil
}
func (f *coldClient) ContainerStop(_ context.Context, id string, _ container.StopOptions) error {
	if f.beforeStop != nil {
		f.beforeStop()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops++
	if f.stopErr != nil {
		return f.stopErr
	}
	c := f.containers[id]
	c.State.Running = false
	c.State.Status = "exited"
	return nil
}
func (f *coldClient) ContainerStart(_ context.Context, id string, _ container.StartOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts++
	if f.startErr != nil {
		return f.startErr
	}
	c, ok := f.containers[id]
	if !ok {
		return cerrdefs.ErrNotFound
	}
	c.State.Running = true
	c.State.Status = "running"
	return nil
}
func (f *coldClient) ContainerList(_ context.Context, opts container.ListOptions) ([]container.Summary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []container.Summary
	for _, c := range f.containers {
		matches := true
		for _, filter := range opts.Filters.Get("label") {
			key, value, _ := strings.Cut(filter, "=")
			if c.Config.Labels[key] != value {
				matches = false
			}
		}
		if matches {
			out = append(out, container.Summary{ID: c.ID, State: c.State.Status, Labels: c.Config.Labels})
		}
	}
	return out, nil
}
func (f *coldClient) ContainerRemove(_ context.Context, id string, _ container.RemoveOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removeErr != nil {
		return f.removeErr
	}
	delete(f.containers, id)
	return nil
}

func acquireTestCold(t *testing.T, q *Quiescer) *coldLease {
	t.Helper()
	l, err := q.AcquireCold(context.Background(), "app", nil)
	if err != nil {
		t.Fatal(err)
	}
	lease := l.(*coldLease)
	t.Cleanup(func() { _ = lease.lock.Close() })
	return lease
}

func TestColdOriginalStateAndWriteAhead(t *testing.T) {
	for _, running := range []bool{false, true} {
		t.Run(fmt.Sprint(running), func(t *testing.T) {
			f := newColdClient(running)
			q := NewDurableQuiescer(f, t.TempDir())
			f.beforeStop = func() {
				dir, err := q.coldDir(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				records, err := readColdRecords(dir)
				if err != nil || len(records) != 1 || !records[0].WasRunning || records[0].Phase != "stop-intent" {
					t.Fatalf("Stop without durable intent: %+v %v", records, err)
				}
			}
			l := acquireTestCold(t, q)
			if err := l.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := l.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := 0
			if running {
				want = 1
			}
			if f.starts != want || f.stops != want {
				t.Fatalf("starts=%d stops=%d want=%d", f.starts, f.stops, want)
			}
			if n, err := q.RecoverQuiesced(context.Background()); n != 0 || err != nil {
				t.Fatalf("repeat recovery: %d %v", n, err)
			}
		})
	}
}

func TestColdSurvivorAndRestartRetry(t *testing.T) {
	f := newColdClient(true)
	dir := t.TempDir()
	q := NewDurableQuiescer(f, dir)
	l := acquireTestCold(t, q)
	req := mover.Request{Labels: map[string]string{mover.LabelColdOperation: "untrusted", "sabon.source-container": "app"}}
	if err := l.AttachMover(context.Background(), &req); err != nil {
		t.Fatal(err)
	}
	if req.Name != "sabon-cold-"+l.record.Operation || req.Labels[mover.LabelColdOperation] != l.record.Operation {
		t.Fatal("missing reserved identity")
	}
	if req.Labels["sabon.source-container"] != "app" {
		t.Fatal("attachment changed lifecycle source identity")
	}
	f.put("survivor", true, req.Labels)
	f.aliases[req.Name] = "survivor"
	if err := l.Release(context.Background()); err == nil {
		t.Fatal("released while mover active")
	}
	q = NewDurableQuiescer(f, dir)
	if _, err := q.RecoverQuiesced(context.Background()); err == nil || f.starts != 0 {
		t.Fatal("restarted with survivor")
	}
	f.containers["survivor"].State.Running = false
	f.containers["survivor"].State.Status = "exited"
	f.startErr = errors.New("start unavailable")
	if _, err := q.RecoverQuiesced(context.Background()); err == nil {
		t.Fatal("lost restart failure")
	}
	f.startErr = nil
	if n, err := q.RecoverQuiesced(context.Background()); n != 1 || err != nil {
		t.Fatalf("retry: %d %v", n, err)
	}
	if n, err := q.RecoverQuiesced(context.Background()); n != 0 || err != nil {
		t.Fatalf("idempotence: %d %v", n, err)
	}
}

func TestColdAbandonedBeforeMoverAndReplacement(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprint(replace), func(t *testing.T) {
			f := newColdClient(true)
			q := NewDurableQuiescer(f, t.TempDir())
			l := acquireTestCold(t, q)
			_ = l.lock.Close() // process death: no Release
			if replace {
				delete(f.containers, "original")
				f.put("replacement", false, nil)
				f.aliases["app"] = "replacement"
			}
			if n, err := q.RecoverQuiesced(context.Background()); n != 1 || err != nil {
				t.Fatalf("recover: %d %v", n, err)
			}
			if replace && f.starts != 0 {
				t.Fatal("started replacement")
			}
			if !replace && f.starts != 1 {
				t.Fatal("did not recover original")
			}
		})
	}
}

func TestColdLiveOwnerAndCancelledWait(t *testing.T) {
	f := newColdClient(true)
	dir := t.TempDir()
	q := NewDurableQuiescer(f, dir)
	l := acquireTestCold(t, q)
	other := NewDurableQuiescer(f, dir)
	if n, err := other.RecoverQuiesced(context.Background()); n != 0 || err != nil {
		t.Fatalf("live recovery: %d %v", n, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := other.AcquireCold(ctx, "app", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait: %v", err)
	}
	if f.starts != 0 {
		t.Fatal("live owner restarted")
	}
	if err := l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	next := acquireTestCold(t, other)
	if err := next.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestColdMoverUncertainty(t *testing.T) {
	for _, mode := range []string{"list-error", "remove-error", "paused", "restarting", "created", "missing"} {
		t.Run(mode, func(t *testing.T) {
			f := newColdClient(true)
			q := NewDurableQuiescer(f, t.TempDir())
			l := acquireTestCold(t, q)
			req := mover.Request{}
			if err := l.AttachMover(context.Background(), &req); err != nil {
				t.Fatal(err)
			}
			if mode != "missing" {
				f.put("m", false, req.Labels)
				f.aliases[req.Name] = "m"
			}
			switch mode {
			case "list-error":
				f.listErr = errors.New("list failed")
			case "remove-error":
				f.containers["m"].State.Status = "created"
				f.removeErr = errors.New("remove failed")
			case "created":
				f.containers["m"].State.Status = "created"
			case "paused":
				f.containers["m"].State.Paused = true
			case "restarting":
				f.containers["m"].State.Restarting = true
			}
			err := l.Release(context.Background())
			if mode == "created" || mode == "missing" {
				if err != nil || f.starts != 1 {
					t.Fatalf("safe recovery: %v starts=%d", err, f.starts)
				}
			} else if err == nil || f.starts != 0 {
				t.Fatalf("unsafe recovery: %v starts=%d", err, f.starts)
			}
		})
	}
}

func TestColdStopIntentAmbiguousThenObservedStopped(t *testing.T) {
	f := newColdClient(true)
	f.stopErr = errors.New("lost response")
	q := NewDurableQuiescer(f, t.TempDir())
	if _, err := q.AcquireCold(context.Background(), "app", nil); err == nil {
		t.Fatal("stop error ignored")
	}
	if _, err := q.RecoverQuiesced(context.Background()); err == nil || f.starts != 0 {
		t.Fatal("ambiguous stop cleared")
	}
	f.containers["original"].State.Running = false
	f.containers["original"].State.Status = "exited"
	if n, err := q.RecoverQuiesced(context.Background()); n != 1 || err != nil {
		t.Fatalf("settled stop: %d %v", n, err)
	}
}

func TestColdSharedSourceAdmission(t *testing.T) {
	f := newColdClient(true)
	q := NewDurableQuiescer(f, t.TempDir())
	l := acquireTestCold(t, q)
	f.put("replacement", false, nil)
	if _, err := q.AcquireCold(context.Background(), "replacement", nil); err == nil {
		t.Fatal("overlap admitted")
	}
	if err := l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Explicit sources not mounted by the original application also participate.
	l2, err := q.AcquireCold(context.Background(), "app", []discovery.Source{{Type: mount.TypeBind, Ref: "/shared"}})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Release(context.Background())
	f.containers["replacement"] = container.InspectResponse{ContainerJSONBase: &container.ContainerJSONBase{ID: "replacement", State: &container.State{Status: "exited"}}, Config: &container.Config{}}
	if _, err := q.AcquireCold(context.Background(), "replacement", []discovery.Source{{Type: mount.TypeBind, Ref: "/shared/child"}}); err == nil {
		t.Fatal("nested bind overlap admitted")
	}
}

func TestColdStateFailuresPreventStop(t *testing.T) {
	for _, mode := range []string{"unset", "relative", "not-directory", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			f := newColdClient(true)
			dir := t.TempDir()
			q := NewDurableQuiescer(f, dir)
			switch mode {
			case "unset":
				q.stateDir = ""
			case "relative":
				q.stateDir = "relative"
			case "not-directory":
				q.stateDir = filepath.Join(dir, "file")
				if err := os.WriteFile(q.stateDir, []byte("x"), 0600); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				d, err := q.coldDir(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(d, "broken.json"), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := q.AcquireCold(context.Background(), "app", nil); err == nil {
				t.Fatal("invalid state accepted")
			}
			if f.stops != 0 {
				t.Fatal("stopped without durable state")
			}
		})
	}
}

func TestColdRestartAcknowledgementLost(t *testing.T) {
	f := newColdClient(true)
	q := NewDurableQuiescer(f, t.TempDir())
	l := acquireTestCold(t, q)
	l.record.Phase = "resume-intent"
	if err := writeColdRecord(l.dir, &l.record); err != nil {
		t.Fatal(err)
	}
	if err := f.ContainerStart(context.Background(), "original", container.StartOptions{}); err != nil {
		t.Fatal(err)
	}
	_ = l.lock.Close() // crash after Docker Start, before clearing the journal
	if n, err := q.RecoverQuiesced(context.Background()); n != 1 || err != nil {
		t.Fatalf("recovery: %d %v", n, err)
	}
	if f.starts != 1 {
		t.Fatal("already restored container started twice")
	}
}

// A real child process holds the flock. SIGKILL must release ownership without
// deleting the stable inode, and a waiter must remain context-cancellable.
func TestColdLockProcessDeath(t *testing.T) {
	if path := os.Getenv("SABON_TEST_COLD_LOCK"); path != "" {
		f, err := coldLock(context.Background(), path, true)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		fmt.Println("locked")
		select {}
	}
	path := filepath.Join(t.TempDir(), "owner.lock")
	cmd := exec.Command(os.Args[0], "-test.run=^TestColdLockProcessDeath$")
	cmd.Env = append(os.Environ(), "SABON_TEST_COLD_LOCK="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	ready := make(chan error, 1)
	go func() {
		var s string
		_, err := fmt.Fscanln(stdout, &s)
		if err == nil && s != "locked" {
			err = fmt.Errorf("unexpected %q", s)
		}
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("child did not lock")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if f, err := coldLock(ctx, path, true); !errors.Is(err, context.DeadlineExceeded) {
		if f != nil {
			f.Close()
		}
		t.Fatalf("contended lock: %v", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	f, err := coldLock(context.Background(), path, false)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
}
