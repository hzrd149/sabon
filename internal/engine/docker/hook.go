package docker

// Standalone hooks: pre/post backup actions run against the local Docker
// daemon — exec into the app container (exec mode) or a fresh one-shot
// container (run mode).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/davidborzek/sabon/internal/engine"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

var _ engine.Hooks = (*Hooks)(nil)
var _ engine.Execer = (*Hooks)(nil)

// Hooks executes hooks against the Docker engine.
type Hooks struct {
	cli     client.APIClient
	pending engine.HookTracker
}

func (r *Hooks) CheckHookCompletion(ctx context.Context) error { return r.pending.Check(ctx) }

// NewHooks returns a Docker hook runner.
func NewHooks(cli client.APIClient) *Hooks { return &Hooks{cli: cli} }

// Exec runs a command inside an existing container (exec mode) and returns an
// error, with captured output, if it cannot start or exits non-zero.
func (r *Hooks) Exec(ctx context.Context, containerID string, cmd, env []string, user string) (retErr error) {
	if len(cmd) == 0 {
		return fmt.Errorf("hook: empty command")
	}
	create, err := r.cli.ContainerExecCreate(ctx, containerID, container.ExecOptions{
		Cmd:          cmd,
		Env:          env,
		User:         user,
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return fmt.Errorf("exec create in %s: %w", containerID, err)
	}
	defer func() {
		r.pending.Add("docker-exec", create.ID, func(checkCtx context.Context) error {
			insp, err := r.cli.ContainerExecInspect(checkCtx, create.ID)
			// Running=false with PID=0 may be an exec whose asynchronous start
			// has not happened yet (e.g. an interrupted attach), not an exit.
			if err == nil && !insp.Running && insp.Pid > 0 {
				return nil
			}
			// A removed/stopped source proves its exec cannot still write. An
			// unavailable exec record alone does not establish termination.
			source, sourceErr := r.cli.ContainerInspect(checkCtx, containerID)
			if cerrdefs.IsNotFound(sourceErr) || (sourceErr == nil && source.State != nil && !source.State.Running && (source.State.Status == "exited" || source.State.Status == "dead")) {
				return nil
			}
			if err != nil {
				return errors.Join(err, sourceErr)
			}
			return errors.Join(fmt.Errorf("exec termination not confirmed in container %s", containerID), sourceErr)
		})
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, r.CheckHookCompletion(checkCtx))
	}()
	att, err := r.cli.ContainerExecAttach(ctx, create.ID, container.ExecAttachOptions{})
	if err != nil {
		return fmt.Errorf("exec attach in %s: %w", containerID, err)
	}
	defer att.Close()

	// StdCopy over the hijacked exec stream does not observe ctx; close the
	// connection on cancel/timeout so it unblocks and we surface the ctx error
	// instead of wedging the caller's per-repository lock forever.
	streamDone := make(chan struct{})
	defer close(streamDone)
	go func() {
		select {
		case <-ctx.Done():
			att.Close()
		case <-streamDone:
		}
	}()

	var out bytes.Buffer
	if _, err := stdcopy.StdCopy(&out, &out, att.Reader); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("exec in %s: %w", containerID, ctx.Err())
		}
		return fmt.Errorf("exec stream in %s: %w", containerID, err)
	}
	if ctx.Err() != nil {
		return fmt.Errorf("exec in %s: %w", containerID, ctx.Err())
	}
	insp, err := r.cli.ContainerExecInspect(ctx, create.ID)
	if err != nil {
		return fmt.Errorf("exec inspect in %s: %w", containerID, err)
	}
	if insp.ExitCode != 0 {
		return fmt.Errorf("hook in %s exited with code %d: %s", containerID, insp.ExitCode, out.String())
	}
	return nil
}

// RunOneShot spawns a fresh container from spec.Image, runs the command, waits
// for it, and removes it. Non-zero exit (with logs) is an error.
func (r *Hooks) RunOneShot(ctx context.Context, spec engine.RunSpec) (retErr error) {
	if spec.Image == "" {
		return fmt.Errorf("hook: run mode needs an image")
	}
	mounts, err := parseMounts(spec.Volumes)
	if err != nil {
		return err
	}
	if err := r.ensureImage(ctx, spec.Image); err != nil {
		return err
	}

	cfg := &container.Config{
		Image:  spec.Image,
		Cmd:    spec.Command,
		User:   spec.User,
		Env:    spec.Env,
		Labels: map[string]string{engine.HookLabelKey: spec.Name},
	}
	host := &container.HostConfig{Mounts: mounts}
	if spec.Network != "" {
		host.NetworkMode = container.NetworkMode(spec.Network)
	}

	created, err := r.cli.ContainerCreate(ctx, cfg, host, nil, nil, spec.Name)
	if err != nil {
		return fmt.Errorf("create hook container: %w", err)
	}
	id := created.ID
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := r.cli.ContainerRemove(cleanupCtx, id, container.RemoveOptions{Force: true}); err != nil && !cerrdefs.IsNotFound(err) {
			retErr = errors.Join(retErr, fmt.Errorf("remove hook container %s: %w", id, err))
			r.pending.Add("docker-container", id, func(checkCtx context.Context) error {
				insp, err := r.cli.ContainerInspect(checkCtx, id)
				if cerrdefs.IsNotFound(err) {
					return nil
				}
				if err != nil {
					return err
				}
				if insp.State != nil && !insp.State.Running && (insp.State.Status == "exited" || insp.State.Status == "dead") {
					return nil
				}
				return fmt.Errorf("container not confirmed stopped")
			})
			checkCtx, checkCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer checkCancel()
			retErr = errors.Join(retErr, r.CheckHookCompletion(checkCtx))
		}
	}()

	if err := r.cli.ContainerStart(ctx, id, container.StartOptions{}); err != nil {
		return fmt.Errorf("start hook container: %w", err)
	}
	statusCh, errCh := r.cli.ContainerWait(ctx, id, container.WaitConditionNotRunning)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return fmt.Errorf("wait hook container: %w", err)
	case st := <-statusCh:
		if st.Error != nil {
			return fmt.Errorf("wait hook container: %s", st.Error.Message)
		}
		if st.StatusCode != 0 {
			return fmt.Errorf("hook container %s exited with code %d:\n%s", spec.Image, st.StatusCode, r.logs(ctx, id))
		}
	}
	return nil
}

// Reap removes EXITED leftover one-shot hook containers (from a crash or an
// aborted run). Running ones are left alone. Returns how many were removed.
func (r *Hooks) Reap(ctx context.Context) (int, error) {
	list, err := r.cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("label", engine.HookLabelKey), filters.Arg("status", "exited")),
	})
	if err != nil {
		return 0, fmt.Errorf("list hook containers: %w", err)
	}
	n := 0
	for _, c := range list {
		if err := r.cli.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true}); err == nil {
			n++
		}
	}
	return n, nil
}

// AppProject returns the container's Docker Compose project label, or "".
func (r *Hooks) AppProject(ctx context.Context, id string) (string, error) {
	insp, err := r.cli.ContainerInspect(ctx, id)
	if err != nil {
		return "", err
	}
	return insp.Config.Labels["com.docker.compose.project"], nil
}

func (r *Hooks) logs(ctx context.Context, id string) string {
	rc, err := r.cli.ContainerLogs(ctx, id, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return ""
	}
	defer func() { _ = rc.Close() }()
	var buf bytes.Buffer
	_, _ = stdcopy.StdCopy(&buf, &buf, rc)
	return buf.String()
}

func (r *Hooks) ensureImage(ctx context.Context, ref string) error {
	if _, err := r.cli.ImageInspect(ctx, ref); err == nil {
		return nil
	}
	rc, err := r.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull hook image %s: %w", ref, err)
	}
	defer func() { _ = rc.Close() }()
	_, _ = io.Copy(io.Discard, rc)
	return nil
}

// parseMounts turns "source:/target[:ro]" strings into Docker mounts. A source
// beginning with "/" is a bind mount; otherwise it is a named volume.
func parseMounts(vols []string) ([]mount.Mount, error) {
	var ms []mount.Mount
	for _, v := range vols {
		parts := strings.Split(v, ":")
		if len(parts) < 2 || len(parts) > 3 {
			return nil, fmt.Errorf("hook volume %q: want source:/target[:ro]", v)
		}
		m := mount.Mount{Source: parts[0], Target: parts[1]}
		if strings.HasPrefix(parts[0], "/") {
			m.Type = mount.TypeBind
		} else {
			m.Type = mount.TypeVolume
		}
		if len(parts) == 3 {
			if parts[2] != "ro" {
				return nil, fmt.Errorf("hook volume %q: third field must be 'ro'", v)
			}
			m.ReadOnly = true
		}
		ms = append(ms, m)
	}
	return ms, nil
}

// AppEnv returns the container's environment as a KEY->VALUE map.
func (r *Hooks) AppEnv(ctx context.Context, id string) (map[string]string, error) {
	insp, err := r.cli.ContainerInspect(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(insp.Config.Env))
	for _, kv := range insp.Config.Env {
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[k] = v
		}
	}
	return out, nil
}
