//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/davidborzek/sabon/api"
	"github.com/davidborzek/sabon/internal/backup"
	"github.com/davidborzek/sabon/internal/config"
	"github.com/davidborzek/sabon/internal/discovery"
	"github.com/davidborzek/sabon/internal/engine"
	dockerengine "github.com/davidborzek/sabon/internal/engine/docker"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
)

// Explicitly opt in on a reserved isolated daemon; does not use the legacy
// suite's fixed image/container names or run any global reap/prune operations.
// SABON_E2E_LIFECYCLE=1 DOCKER_HOST=... go test -tags e2e ./test/e2e -run '^TestDockerHookLifecycle$'
func TestDockerHookLifecycle(t *testing.T) {
	if os.Getenv("SABON_E2E_LIFECYCLE") != "1" || os.Getenv("DOCKER_HOST") == "" {
		t.Skip("requires SABON_E2E_LIFECYCLE=1 and explicit isolated DOCKER_HOST")
	}
	docker := func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	mustDocker := func(t *testing.T, args ...string) string {
		t.Helper()
		out, err := docker(args...)
		if err != nil {
			t.Fatalf("docker %v: %v\n%s", args, err, out)
		}
		return out
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	unique := fmt.Sprintf("sabon-lifecycle-%d-%d", os.Getpid(), time.Now().UnixNano())
	image := unique + ":test"
	mustDocker(t, "build", "-f", filepath.Join(root, "Dockerfile.mover"), "-t", image, root)
	t.Cleanup(func() { _, _ = docker("image", "rm", image) })
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	t.Setenv("SABON_E2E_LIFECYCLE_PASSWORD", "synthetic-test-password")
	for _, phase := range []string{"pre", "post", "pre-timeout", "post-timeout"} {
		t.Run(phase, func(t *testing.T) {
			name := unique + "-" + phase
			source, cache := name+"-data", name+"-cache"
			mustDocker(t, "volume", "create", source)
			mustDocker(t, "volume", "create", cache)
			t.Cleanup(func() { _, _ = docker("volume", "rm", source, cache) })
			id := mustDocker(t, "run", "-d", "--name", name, "--entrypoint", "sh", "--mount", "type=volume,source="+source+",target=/data", image, "-c", "sleep 1800")
			t.Cleanup(func() { _, _ = docker("rm", "-f", id) })
			mustDocker(t, "exec", id, "sh", "-c", "echo synthetic-payload >/data/payload")
			eng := dockerengine.New(cli)
			hooks := dockerengine.NewHooks(cli)
			o := backup.New(&config.Config{CacheVolume: cache, MoverUser: "0:0"}, image, eng, hooks, nil, dockerengine.NewHost(cli), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				runs, err := eng.ListRuns(ctx, name, "")
				if err != nil {
					t.Errorf("list owned movers for cleanup: %v", err)
					return
				}
				for _, run := range runs {
					_, _ = docker("rm", "-f", run.ID)
				}
			})
			pre := "touch /data/prepared"
			post := "rm -f /data/prepared; touch /data/cleaned"
			if phase == "pre" {
				pre += "; exit 23"
			} else {
				post += "; exit 24"
			}
			job := discovery.Job{App: name, Container: id, Sources: []discovery.Source{{Name: "data", Type: mount.TypeVolume, Ref: source}}, Spec: api.Spec{
				PreHooks:  []api.Hook{{Command: []string{"sh", "-c", pre}}},
				PostHooks: []api.Hook{{Command: []string{"sh", "-c", post}}, {Command: []string{"sh", "-c", "touch /data/later-cleanup"}}},
			}}
			if strings.HasSuffix(phase, "-timeout") {
				writer := api.Hook{Timeout: "1s", Command: []string{"sh", "-c", "while [ ! -e /data/finish-writer ]; do echo writing >>/data/writes; sleep 0.1; done; touch /data/writer-exited"}}
				job.Spec.PostHooks[0] = api.Hook{Command: []string{"sh", "-c", "rm -f /data/prepared; touch /data/cleaned"}}
				if phase == "pre-timeout" {
					job.Spec.PreHooks[0] = writer
				} else {
					job.Spec.PostHooks[0] = writer
				}
			}
			// Repository is inside the unique cache volume: no client-host bind
			// path assumptions, so the test also works against an isolated DinD.
			target := api.Target{Name: "test", Repo: "/cache/repository", PasswordEnv: "SABON_E2E_LIFECYCLE_PASSWORD"}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			res, err := o.RunTarget(ctx, job, target)
			if err == nil || !strings.Contains(err.Error(), strings.TrimSuffix(phase, "-timeout")+"-hook") {
				t.Fatalf("missing real hook failure: result=%+v err=%v", res, err)
			}
			if strings.HasSuffix(phase, "-timeout") {
				var uncertain *engine.HookUncertain
				if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &uncertain) || uncertain.ID == "" {
					t.Fatalf("lost timeout/exec identity: %v", err)
				}
				// Prove the real exec still writes after the deadline, and that
				// neither recovery nor the next target can mutate its artifacts.
				mustDocker(t, "exec", id, "sh", "-c", "before=$(wc -l </data/writes); sleep 0.3; after=$(wc -l </data/writes); test \"$after\" -gt \"$before\" && test ! -e /data/cleaned && test ! -e /data/later-cleanup")
				if phase == "pre-timeout" && res.SnapshotID != "" {
					t.Fatal("backup ran after uncertain prehook")
				}
				if phase == "post-timeout" && res.SnapshotID == "" {
					t.Fatal("missing snapshot before uncertain recovery")
				}
				retry := job
				retry.Spec.PreHooks = []api.Hook{{Command: []string{"sh", "-c", "touch /data/next-pre"}}}
				retry.Spec.PostHooks = []api.Hook{{Command: []string{"sh", "-c", "touch /data/cleaned"}}}
				nextTarget := target
				nextTarget.Name = "second-target"
				if _, err := o.RunTarget(ctx, retry, nextTarget); err == nil {
					t.Fatal("next target ran while exec still writes")
				}
				mustDocker(t, "exec", id, "sh", "-c", "test ! -e /data/next-pre")
				mustDocker(t, "exec", id, "touch", "/data/finish-writer")
				waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
				defer waitCancel()
				for hooks.CheckHookCompletion(waitCtx) != nil {
					select {
					case <-waitCtx.Done():
						t.Fatal("writer exit was not confirmed")
					case <-time.After(50 * time.Millisecond):
					}
				}
				mustDocker(t, "exec", id, "test", "-e", "/data/writer-exited")
				if _, err := o.RunTarget(ctx, retry, nextTarget); err != nil {
					t.Fatalf("confirmed hook exit did not unblock backup: %v", err)
				}
				mustDocker(t, "exec", id, "sh", "-c", "test -e /data/next-pre && test -e /data/cleaned")
				return
			}
			mustDocker(t, "exec", id, "sh", "-c", "test ! -e /data/prepared && test -e /data/cleaned && test -e /data/later-cleanup")
			runs, listErr := eng.ListRuns(ctx, name, "")
			if listErr != nil {
				t.Fatal(listErr)
			}
			if phase == "pre" {
				if len(runs) != 0 || res.SnapshotID != "" {
					t.Fatalf("failed prehook launched backup: runs=%v result=%+v", runs, res)
				}
				return
			}
			if res.SnapshotID == "" || len(runs) != 1 || runs[0].Status != "succeeded" {
				t.Fatalf("expected successful mover with failed recovery: runs=%v result=%+v", runs, res)
			}
			var snapshots bytes.Buffer
			if err := o.RunSnapshots(ctx, name, target, &snapshots); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(snapshots.String(), res.SnapshotID[:8]) {
				t.Fatalf("snapshot absent from real repository: %s", snapshots.String())
			}
		})
	}
}
