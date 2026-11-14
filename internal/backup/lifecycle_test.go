package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/davidborzek/sabon/api"
	"github.com/davidborzek/sabon/internal/config"
	"github.com/davidborzek/sabon/internal/discovery"
	"github.com/davidborzek/sabon/internal/engine"
	"github.com/davidborzek/sabon/internal/mover"
)

type lifecycleEngine struct {
	engine.Engine
	run     func(context.Context, mover.Request) (mover.Result, bool, error)
	runs    []engine.RunInfo
	listErr error
}

func (e *lifecycleEngine) RunMover(ctx context.Context, req mover.Request) (mover.Result, bool, error) {
	return e.run(ctx, req)
}
func (e *lifecycleEngine) ListRuns(context.Context, string, string) ([]engine.RunInfo, error) {
	return e.runs, e.listErr
}

type lifecycleHooks struct {
	engine.Hooks
	exec func(context.Context, string) error
}

func (h lifecycleHooks) Exec(ctx context.Context, _ string, cmd, _ []string, _ string) error {
	return h.exec(ctx, cmd[0])
}

type lifecycleQuiescer struct {
	stop, start func(context.Context) error
}

func (q lifecycleQuiescer) Stop(ctx context.Context, _ string) error  { return q.stop(ctx) }
func (q lifecycleQuiescer) Start(ctx context.Context, _ string) error { return q.start(ctx) }

func lifecycleFixture(t *testing.T) (*Orchestrator, discovery.Job, api.Target, *lifecycleEngine) {
	t.Helper()
	t.Setenv("SABON_TEST_LIFECYCLE_PASSWORD", "synthetic")
	e := &lifecycleEngine{run: func(context.Context, mover.Request) (mover.Result, bool, error) {
		return mover.Result{SnapshotID: "synthetic-snapshot"}, false, nil
	}}
	o := New(&config.Config{}, "synthetic-mover", e, lifecycleHooks{exec: func(context.Context, string) error { return nil }}, lifecycleQuiescer{stop: func(context.Context) error { return nil }, start: func(context.Context) error { return nil }}, fakeHost{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	job := discovery.Job{App: "synthetic-app", Container: "synthetic-container", Spec: api.Spec{PreHooks: []api.Hook{{Command: []string{"pre"}}}, PostHooks: []api.Hook{{Command: []string{"post"}}, {Command: []string{"post2"}}}}}
	return o, job, api.Target{Name: "one", Path: "/synthetic-repo", PasswordEnv: "SABON_TEST_LIFECYCLE_PASSWORD"}, e
}

func requireCleanupContext(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if ctx.Err() != nil || !ok || time.Until(deadline) > 5*time.Minute {
		t.Errorf("cleanup must have independent bounded context: err=%v deadline=%v", ctx.Err(), deadline)
	}
}

func TestLifecycleFailures(t *testing.T) {
	primary, cleanup, restart := errors.New("primary"), errors.New("cleanup"), errors.New("restart")
	for _, phase := range []string{"pre", "stop", "source", "mover", "post"} {
		t.Run(phase, func(t *testing.T) {
			o, job, target, e := lifecycleFixture(t)
			job.Spec.Stop = true
			var calls []string
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			o.hooks = lifecycleHooks{exec: func(ctx context.Context, cmd string) error {
				calls = append(calls, cmd)
				if cmd == "pre" {
					if phase == "pre" {
						cancel()
						return primary
					}
					return nil
				}
				requireCleanupContext(t, ctx)
				if cmd == "post" {
					return cleanup
				}
				return nil
			}}
			o.quiesce = lifecycleQuiescer{stop: func(context.Context) error {
				calls = append(calls, "stop")
				if phase == "stop" {
					cancel()
					return primary
				}
				return nil
			}, start: func(ctx context.Context) error {
				requireCleanupContext(t, ctx)
				calls = append(calls, "start")
				return restart
			}}
			if phase == "source" {
				job.Spec.Snapshot = "unavailable"
			}
			e.run = func(context.Context, mover.Request) (mover.Result, bool, error) {
				calls = append(calls, "mover")
				cancel()
				if phase == "mover" {
					return mover.Result{}, false, primary
				}
				return mover.Result{SnapshotID: "synthetic"}, false, nil
			}
			res, err := o.RunTarget(ctx, job, target)
			if !errors.Is(err, cleanup) {
				t.Errorf("cleanup error lost: %v", err)
			}
			if phase == "pre" || phase == "stop" || phase == "mover" {
				if !errors.Is(err, primary) {
					t.Errorf("primary error lost: %v", err)
				}
			}
			if phase == "source" && err == nil {
				t.Error("source failure lost")
			}
			if phase == "post" && res.SnapshotID != "synthetic" {
				t.Errorf("snapshot result lost: %+v", res)
			}
			want := []string{"pre"}
			if phase != "pre" {
				want = append(want, "stop")
			}
			if phase == "mover" || phase == "post" {
				want = append(want, "mover")
			}
			// Restart precedes post-hooks, which may exec in the app.
			if phase != "pre" {
				want = append(want, "start")
				if !errors.Is(err, restart) {
					t.Errorf("restart error lost: %v", err)
				}
			}
			want = append(want, "post", "post2")
			if !reflect.DeepEqual(calls, want) {
				t.Errorf("calls=%v want=%v", calls, want)
			}
		})
	}
}

func TestRestoreJoinsRestartFailure(t *testing.T) {
	o, job, target, e := lifecycleFixture(t)
	primary, restart := errors.New("restore"), errors.New("restart")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.run = func(context.Context, mover.Request) (mover.Result, bool, error) {
		cancel()
		return mover.Result{}, false, primary
	}
	o.quiesce = lifecycleQuiescer{stop: func(context.Context) error { return nil }, start: func(ctx context.Context) error { requireCleanupContext(t, ctx); return restart }}
	err := o.RunRestore(ctx, job.App, &job, target, RestoreOptions{Stop: true}, io.Discard)
	if !errors.Is(err, primary) || !errors.Is(err, restart) {
		t.Fatalf("want restore and restart errors, got %v", err)
	}
}

func TestSharedDumpLifecycleAcrossTargets(t *testing.T) {
	o, job, target, e := lifecycleFixture(t)
	synctest.Test(t, func(t *testing.T) {
		var preparing, reading atomic.Int32
		release := make(chan struct{})
		done := make(chan error, 2)
		o.hooks = lifecycleHooks{exec: func(_ context.Context, cmd string) error {
			if reading.Load() != 0 {
				t.Errorf("%s mutated dump while mover reads it", cmd)
			}
			if cmd == "pre" {
				preparing.Add(1)
			}
			return nil
		}}
		e.run = func(_ context.Context, req mover.Request) (mover.Result, bool, error) {
			if req.KeepOnShutdown {
				t.Error("hook-bearing mover allowed to survive shutdown")
			}
			reading.Add(1)
			<-release
			reading.Add(-1)
			return mover.Result{}, false, nil
		}
		go func() { _, err := o.RunTarget(context.Background(), job, target); done <- err }()
		synctest.Wait()
		other := target
		other.Name = "two"
		go func() { _, err := o.RunTarget(context.Background(), job, other); done <- err }()
		synctest.Wait()
		if preparing.Load() != 1 {
			t.Fatalf("second target overwrote shared dump: prehooks=%d", preparing.Load())
		}
		close(release)
		for range 2 {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		if preparing.Load() != 2 {
			t.Fatal("second target never ran")
		}
	})
}

func TestLifecycleWaitCancellation(t *testing.T) {
	o, job, target, e := lifecycleFixture(t)
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		done := make(chan error, 2)
		e.run = func(context.Context, mover.Request) (mover.Result, bool, error) {
			<-release
			return mover.Result{}, false, nil
		}
		go func() { _, err := o.RunTarget(context.Background(), job, target); done <- err }()
		synctest.Wait()
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _, err := o.RunTarget(ctx, job, target); done <- err }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting run ignored cancellation: %v", err)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestSurvivingMoverDefersCleanupAndBlocksNewHooks(t *testing.T) {
	for _, kept := range []bool{true, false} {
		t.Run(fmt.Sprint("kept=", kept), func(t *testing.T) {
			o, job, target, e := lifecycleFixture(t)
			var calls []string
			o.hooks = lifecycleHooks{exec: func(_ context.Context, cmd string) error { calls = append(calls, cmd); return nil }}
			e.run = func(context.Context, mover.Request) (mover.Result, bool, error) {
				e.runs = []engine.RunInfo{{ID: "survivor", App: job.App, Action: "backup", Status: "running"}}
				return mover.Result{}, kept, context.Canceled
			}
			_, err := o.RunTarget(context.Background(), job, target)
			if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "recovery deferred") {
				t.Fatalf("missing deferred recovery error: %v", err)
			}
			other := target
			other.Name = "two"
			if _, err := o.RunTarget(context.Background(), job, other); err == nil {
				t.Fatal("surviving reader did not block new backup")
			}
			if !reflect.DeepEqual(calls, []string{"pre"}) {
				t.Fatalf("surviving mover's artifact mutated: %v", calls)
			}
		})
	}
}

func TestUnverifiableMoverExitDefersRecovery(t *testing.T) {
	o, job, target, e := lifecycleFixture(t)
	job.Spec.Stop = true
	inspectErr := errors.New("runtime unavailable")
	e.run = func(context.Context, mover.Request) (mover.Result, bool, error) {
		e.listErr = inspectErr
		return mover.Result{}, false, context.Canceled
	}
	o.quiesce = lifecycleQuiescer{stop: func(context.Context) error { return nil }, start: func(context.Context) error { t.Error("restarted with unknown reader state"); return nil }}
	o.hooks = lifecycleHooks{exec: func(_ context.Context, cmd string) error {
		if cmd != "pre" {
			t.Error("cleanup with unknown reader state")
		}
		return nil
	}}
	_, err := o.RunTarget(context.Background(), job, target)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, inspectErr) {
		t.Fatalf("lost primary/inspection error: %v", err)
	}
}

func TestLaterPreHookFailureStillCleansUp(t *testing.T) {
	o, job, target, e := lifecycleFixture(t)
	job.Spec.PreHooks = []api.Hook{{Command: []string{"pre"}}, {Command: []string{"partial"}}, {Command: []string{"never"}}}
	var calls []string
	o.hooks = lifecycleHooks{exec: func(_ context.Context, cmd string) error {
		calls = append(calls, cmd)
		if cmd == "partial" {
			return errors.New("changed state then failed")
		}
		return nil
	}}
	e.run = func(context.Context, mover.Request) (mover.Result, bool, error) {
		t.Error("backup after failed preparation")
		return mover.Result{}, false, nil
	}
	if _, err := o.RunTarget(context.Background(), job, target); err == nil {
		t.Fatal("prehook error lost")
	}
	if want := []string{"pre", "partial", "post", "post2"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls=%v want=%v", calls, want)
	}
}

func TestSuccessfulMoverRecoveryFailure(t *testing.T) {
	for _, phase := range []string{"post", "restart"} {
		t.Run(phase, func(t *testing.T) {
			o, job, target, _ := lifecycleFixture(t)
			failure := errors.New("synthetic recovery failure")
			if phase == "post" {
				o.hooks = lifecycleHooks{exec: func(_ context.Context, cmd string) error {
					if cmd == "post" {
						return failure
					}
					return nil
				}}
			} else {
				job.Spec.Stop = true
				o.quiesce = lifecycleQuiescer{stop: func(context.Context) error { return nil }, start: func(context.Context) error { return failure }}
			}
			res, err := o.RunTarget(context.Background(), job, target)
			if !errors.Is(err, failure) || res.SnapshotID != "synthetic-snapshot" {
				t.Fatalf("snapshot and recovery outcomes not preserved: result=%+v err=%v", res, err)
			}
		})
	}
}

func TestColdTargetsWaitForTheirOwnRecovery(t *testing.T) {
	o, job, target, _ := lifecycleFixture(t)
	job.Spec.Stop = true
	synctest.Test(t, func(t *testing.T) {
		var stops atomic.Int32
		release := make(chan struct{})
		done := make(chan error, 2)
		failure := errors.New("synthetic restart failure")
		o.quiesce = lifecycleQuiescer{stop: func(context.Context) error { stops.Add(1); return nil }, start: func(context.Context) error { <-release; return failure }}
		go func() { _, err := o.RunTarget(context.Background(), job, target); done <- err }()
		synctest.Wait()
		other := target
		other.Name = "two"
		go func() { _, err := o.RunTarget(context.Background(), job, other); done <- err }()
		synctest.Wait()
		if stops.Load() != 1 {
			t.Fatal("another cold target entered before recovery finished")
		}
		select {
		case err := <-done:
			t.Fatalf("target reported before recovery: %v", err)
		default:
		}
		close(release)
		for range 2 {
			if err := <-done; !errors.Is(err, failure) {
				t.Fatalf("target lost its restart failure: %v", err)
			}
		}
	})
}

func TestCleanupTimeoutDoesNotSkipLaterHooks(t *testing.T) {
	o, job, target, _ := lifecycleFixture(t)
	synctest.Test(t, func(t *testing.T) {
		later := false
		o.hooks = lifecycleHooks{exec: func(ctx context.Context, cmd string) error {
			switch cmd {
			case "post":
				<-ctx.Done()
				return ctx.Err()
			case "post2":
				requireCleanupContext(t, ctx)
				later = true
			}
			return nil
		}}
		start := time.Now()
		res, err := o.RunTarget(context.Background(), job, target)
		if !errors.Is(err, context.DeadlineExceeded) || !later || res.SnapshotID == "" {
			t.Fatalf("timed out cleanup lost outcome or skipped later hook: res=%+v err=%v later=%v", res, err, later)
		}
		if elapsed := time.Since(start); elapsed != 5*time.Minute {
			t.Fatalf("cleanup elapsed=%v, want five minute budget", elapsed)
		}
	})
}

func TestNamedSpecsShareContainerLifecycle(t *testing.T) {
	for _, restore := range []bool{false, true} {
		t.Run(fmt.Sprint("restore=", restore), func(t *testing.T) {
			o, job, target, e := lifecycleFixture(t)
			job.Spec.Stop = true
			synctest.Test(t, func(t *testing.T) {
				var stops, starts, reading atomic.Int32
				release := make(chan struct{})
				done := make(chan error, 2)
				o.quiesce = lifecycleQuiescer{stop: func(context.Context) error {
					if reading.Load() != 0 {
						t.Error("second spec stopped a container with an active reader")
					}
					stops.Add(1)
					return nil
				}, start: func(context.Context) error {
					if reading.Load() != 0 {
						t.Error("premature restart during another spec's read")
					}
					starts.Add(1)
					return nil
				}}
				e.run = func(_ context.Context, req mover.Request) (mover.Result, bool, error) {
					if req.Labels[mover.LabelSourceContainer] != job.Container {
						t.Error("missing shared source identity label")
					}
					if reading.Add(1) != 1 {
						t.Error("named specs overlap")
					}
					<-release
					reading.Add(-1)
					return mover.Result{}, false, nil
				}
				go func() { _, err := o.RunTarget(context.Background(), job, target); done <- err }()
				synctest.Wait()
				other := job
				other.App = "second-named-spec"
				otherTarget := target
				otherTarget.Name = "second-repo"
				go func() {
					if restore {
						done <- o.RunRestore(context.Background(), other.App, &other, otherTarget, RestoreOptions{Stop: true}, io.Discard)
						return
					}
					_, err := o.RunTarget(context.Background(), other, otherTarget)
					done <- err
				}()
				synctest.Wait()
				if stops.Load() != 1 || starts.Load() != 0 {
					t.Fatalf("source not excluded: stops=%d starts=%d", stops.Load(), starts.Load())
				}
				close(release)
				for range 2 {
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				}
				if stops.Load() != 2 || starts.Load() != 2 {
					t.Fatal("unbalanced recovery")
				}
			})
		})
	}
}

func TestSurvivingNamedSpecBlocksSharedSource(t *testing.T) {
	o, job, target, e := lifecycleFixture(t)
	e.runs = []engine.RunInfo{{App: "different-named-spec", SourceContainer: job.Container, ID: "survivor", Action: "backup", Status: "running"}}
	o.hooks = lifecycleHooks{exec: func(context.Context, string) error { t.Error("hook changed shared source"); return nil }}
	if _, err := o.RunTarget(context.Background(), job, target); err == nil {
		t.Fatal("surviving sibling spec was ignored")
	}
	if err := o.RunRestore(context.Background(), job.App, &job, target, RestoreOptions{Stop: true}, io.Discard); err == nil {
		t.Fatal("restore ignored sibling spec")
	}
}

type pendingLifecycleHooks struct {
	lifecycleHooks
	pending engine.HookTracker
}

func (h *pendingLifecycleHooks) CheckHookCompletion(ctx context.Context) error {
	return h.pending.Check(ctx)
}

func TestUncertainHookBlocksCleanupAndNewPreparation(t *testing.T) {
	for _, phase := range []string{"pre", "post"} {
		t.Run(phase, func(t *testing.T) {
			o, job, target, _ := lifecycleFixture(t)
			h := &pendingLifecycleHooks{}
			running, first := true, true
			var calls []string
			h.exec = func(_ context.Context, cmd string) error {
				calls = append(calls, cmd)
				if cmd == phase && first {
					first = false
					h.pending.Add("synthetic-exec", "writer", func(context.Context) error {
						if running {
							return errors.New("writer running")
						}
						return nil
					})
					return errors.Join(context.DeadlineExceeded, h.CheckHookCompletion(context.Background()))
				}
				return nil
			}
			o.hooks = h
			_, err := o.RunTarget(context.Background(), job, target)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost timeout: %v", err)
			}
			want := []string{"pre"}
			if phase == "post" {
				want = append(want, "post")
			}
			if !reflect.DeepEqual(calls, want) {
				t.Fatalf("cleanup ran alongside writer: %v", calls)
			}
			other := job
			other.App = "another-spec"
			if _, err := o.RunTarget(context.Background(), other, target); err == nil {
				t.Fatal("new preparation not fenced")
			}
			if !reflect.DeepEqual(calls, want) {
				t.Fatal("blocked run executed hooks")
			}
			running = false
			if _, err := o.RunTarget(context.Background(), other, target); err != nil {
				t.Fatalf("confirmed exit did not release fence: %v", err)
			}
		})
	}
}
