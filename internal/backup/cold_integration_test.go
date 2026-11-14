package backup

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/davidborzek/sabon/api"
	"github.com/davidborzek/sabon/internal/config"
	"github.com/davidborzek/sabon/internal/discovery"
	"github.com/davidborzek/sabon/internal/engine"
	"github.com/davidborzek/sabon/internal/engine/docker"
	"github.com/davidborzek/sabon/internal/mover"
	"github.com/davidborzek/sabon/internal/snapshot"
)

type leaseQuiescer struct {
	engine.Quiescer // legacy Stop/Start intentionally unavailable
	lease           *testColdLease
	acquireErr      error
}

func (q leaseQuiescer) AcquireCold(context.Context, string, []discovery.Source) (engine.ColdLease, error) {
	if q.lease == nil {
		return nil, q.acquireErr
	}
	return q.lease, q.acquireErr
}

type testColdLease struct {
	attached, released    bool
	attachErr, releaseErr error
	idleErr               error
	onIdle, onRelease     func(context.Context)
}

func (l *testColdLease) AttachMover(_ context.Context, r *mover.Request) error {
	l.attached = true
	r.Name = "durable-name"
	return l.attachErr
}
func (l *testColdLease) Release(ctx context.Context) error {
	if l.onRelease != nil {
		l.onRelease(ctx)
	}
	l.released = true
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return l.releaseErr
}

func (l *testColdLease) CheckIdle(ctx context.Context) error {
	if l.onIdle != nil {
		l.onIdle(ctx)
	}
	return l.idleErr
}

type coldTestEngine struct {
	engine.Engine
	lease  *testColdLease
	ran    bool
	runErr error
}

func (e *coldTestEngine) ListRuns(context.Context, string, string) ([]engine.RunInfo, error) {
	return nil, nil
}

func (e *coldTestEngine) RunMover(_ context.Context, r mover.Request) (mover.Result, bool, error) {
	if !e.lease.attached || r.Name != "durable-name" {
		panic("mover launched without attached durable intent")
	}
	e.ran = true
	return mover.Result{}, false, e.runErr
}

type lifecycleSnapshot struct {
	*fakeSnap
	cleanup func(context.Context)
}

func (s lifecycleSnapshot) Snapshot(ctx context.Context, app string, sources []snapshot.Source) ([]snapshot.Mount, func(context.Context), error) {
	mounts, _, err := s.fakeSnap.Snapshot(ctx, app, sources)
	return mounts, s.cleanup, err
}

// Exercise the merged lifecycle, rather than either feature in isolation:
// source cleanup is gated, release is unconditional, and restart precedes hooks.
func TestDurableLifecycleRecoveryOrdering(t *testing.T) {
	for _, mode := range []string{"running", "originally-stopped", "kept", "idle-error", "mover-error", "attach-error", "source-error"} {
		t.Run(mode, func(t *testing.T) {
			o, job, target, e := lifecycleFixture(t)
			job.Spec.Stop = true
			job.Spec.Snapshot = "zfs"
			job.Sources = binds("data")
			var calls []string
			primary, releaseErr, hookErr := errors.New("primary"), errors.New("release"), errors.New("hook")
			running := false // AcquireCold has stopped the application.
			l := &testColdLease{releaseErr: releaseErr}
			l.onIdle = func(ctx context.Context) {
				requireCleanupContext(t, ctx)
				calls = append(calls, "idle")
			}
			l.onRelease = func(ctx context.Context) {
				requireCleanupContext(t, ctx)
				calls = append(calls, "release")
				// The durable implementation owns original-state recovery; the
				// orchestrator must never override it through legacy Start.
				if mode != "originally-stopped" && mode != "kept" && mode != "idle-error" {
					running = true
				}
			}
			if mode == "idle-error" {
				l.idleErr = primary
			}
			if mode == "attach-error" {
				l.attachErr = primary
			}
			o.quiesce = leaseQuiescer{lease: l}
			o.snaps = []snapshot.Snapshotter{lifecycleSnapshot{fakeSnap: &fakeSnap{mode: "zfs", onFS: map[string]bool{"data": true}}, cleanup: func(ctx context.Context) {
				requireCleanupContext(t, ctx)
				calls = append(calls, "cleanup")
			}}}
			if mode == "source-error" {
				job.Spec.Snapshot = "unavailable"
			}
			o.hooks = lifecycleHooks{exec: func(_ context.Context, cmd string) error {
				calls = append(calls, cmd)
				if cmd == "pre" {
					return nil
				}
				if !l.released || running == (mode == "originally-stopped") {
					t.Errorf("post-hook before original state recovery: released=%v running=%v", l.released, running)
				}
				return hookErr
			}}
			e.run = func(_ context.Context, req mover.Request) (mover.Result, bool, error) {
				if !l.attached || req.Labels[mover.LabelSourceContainer] != job.Container {
					t.Fatal("missing durable attachment or source lifecycle label")
				}
				calls = append(calls, "mover")
				if mode == "mover-error" {
					return mover.Result{}, false, primary
				}
				return mover.Result{SnapshotID: "synthetic"}, mode == "kept", nil
			}
			res, err := o.RunTarget(context.Background(), job, target)
			if !l.released || !errors.Is(err, releaseErr) {
				t.Fatalf("release skipped or error lost: %v", err)
			}
			want := []string{"pre"}
			if mode != "attach-error" && mode != "source-error" {
				want = append(want, "mover")
			}
			want = append(want, "idle")
			if mode != "kept" && mode != "idle-error" {
				if mode != "source-error" {
					want = append(want, "cleanup")
				}
				want = append(want, "release", "post", "post2")
				if !errors.Is(err, hookErr) {
					t.Fatalf("hook error lost: %v", err)
				}
			} else {
				want = append(want, "release")
			}
			if mode == "mover-error" || mode == "attach-error" || mode == "idle-error" {
				if !errors.Is(err, primary) {
					t.Fatalf("primary error lost: %v", err)
				}
			}
			if mode == "running" && res.SnapshotID != "synthetic" {
				t.Fatal("snapshot result lost")
			}
			if !reflect.DeepEqual(calls, want) {
				t.Fatalf("calls=%v want=%v", calls, want)
			}
		})
	}
}

func TestDurableAcquireFailureDefersRecovery(t *testing.T) {
	for _, restore := range []bool{false, true} {
		o, job, target, e := lifecycleFixture(t)
		job.Spec.Stop = true
		failure := errors.New("ambiguous stop; durable recovery pending")
		o.quiesce = leaseQuiescer{acquireErr: failure}
		var calls []string
		o.hooks = lifecycleHooks{exec: func(_ context.Context, cmd string) error {
			calls = append(calls, cmd)
			return nil
		}}
		e.run = func(context.Context, mover.Request) (mover.Result, bool, error) {
			t.Fatal("mover launched after failed acquisition")
			return mover.Result{}, false, nil
		}
		var err error
		if restore {
			err = o.RunRestore(context.Background(), job.App, &job, target, RestoreOptions{Stop: true}, io.Discard)
		} else {
			_, err = o.RunTarget(context.Background(), job, target)
		}
		if !errors.Is(err, failure) {
			t.Fatalf("acquisition error lost: %v", err)
		}
		if (!restore && !reflect.DeepEqual(calls, []string{"pre"})) || (restore && len(calls) != 0) {
			t.Fatalf("unsafe cleanup after failed acquisition: %v", calls)
		}
	}
}

func TestColdLeaseBackupAndRestoreIntegration(t *testing.T) {
	for _, restore := range []bool{false, true} {
		for _, attachFailure := range []bool{false, true} {
			name := "backup"
			if restore {
				name = "restore"
			}
			if attachFailure {
				name += "/attach-error"
			}
			t.Run(name, func(t *testing.T) {
				t.Setenv("RESTIC_PASSWORD", "test-only")
				cleanupErr := errors.New("restart failed")
				primaryErr := errors.New("mover failed")
				l := &testColdLease{releaseErr: cleanupErr}
				if attachFailure {
					l.attachErr = primaryErr
				}
				e := &coldTestEngine{lease: l, runErr: primaryErr}
				o := New(&config.Config{Snapshot: "none"}, "test-image", e, nil, leaseQuiescer{lease: l}, fakeHost{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
				job := discovery.Job{App: "app", Container: "container", Spec: api.Spec{Stop: true}}
				target := api.Target{Name: "target", Path: "/test-repo"}
				var err error
				if restore {
					err = o.RunRestore(context.Background(), job.App, &job, target, RestoreOptions{Stop: true}, io.Discard)
				} else {
					_, err = o.RunTarget(context.Background(), job, target)
				}
				if !errors.Is(err, primaryErr) || !errors.Is(err, cleanupErr) {
					t.Fatalf("lost primary/cleanup error: %v", err)
				}
				if !l.released {
					t.Fatal("durable release skipped")
				}
				if e.ran == attachFailure {
					t.Fatalf("mover ran=%v attach failure=%v", e.ran, attachFailure)
				}
			})
		}
	}
}

type preflightQuiescer struct {
	leaseQuiescer
	failure                  error
	preflights, acquisitions int
}

func (q *preflightQuiescer) PreflightCold(context.Context) error {
	q.preflights++
	return q.failure
}

func (q *preflightQuiescer) AcquireCold(ctx context.Context, ref string, sources []discovery.Source) (engine.ColdLease, error) {
	q.acquisitions++
	return q.leaseQuiescer.AcquireCold(ctx, ref, sources)
}

type preflightHooks struct {
	engine.Hooks
	calls int
}

func (h *preflightHooks) RunOneShot(context.Context, engine.RunSpec) error {
	h.calls++
	return nil
}

func TestColdPreflightFailureHasNoLifecycleSideEffects(t *testing.T) {
	t.Setenv("RESTIC_PASSWORD", "test-only")
	failure := errors.New("cold storage unavailable")
	l := &testColdLease{}
	q := &preflightQuiescer{leaseQuiescer: leaseQuiescer{lease: l}, failure: failure}
	hooks := &preflightHooks{}
	e := &coldTestEngine{lease: l}
	o := New(&config.Config{Snapshot: "none"}, "test-image", e, hooks, q, fakeHost{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	job := discovery.Job{App: "app", Container: "container", Spec: api.Spec{
		Stop:      true,
		PreHooks:  []api.Hook{{Image: "fixture-hook", Command: []string{"pre"}}},
		PostHooks: []api.Hook{{Image: "fixture-hook", Command: []string{"post"}}},
	}}
	_, err := o.RunTarget(context.Background(), job, api.Target{Name: "target", Path: "/test-repo"})
	if !errors.Is(err, failure) {
		t.Fatalf("lost preflight error: %v", err)
	}
	if q.preflights != 1 || q.acquisitions != 0 || hooks.calls != 0 || e.ran || l.attached || l.released {
		t.Fatalf("preflight failure caused lifecycle work: preflights=%d acquisitions=%d hooks=%d mover=%v attached=%v released=%v", q.preflights, q.acquisitions, hooks.calls, e.ran, l.attached, l.released)
	}
}

func TestColdStorageConfigurationFailsBeforeHooks(t *testing.T) {
	for _, stateDir := range []string{"", "relative-state"} {
		t.Run("state-dir="+stateDir, func(t *testing.T) {
			o, job, target, e := lifecycleFixture(t)
			job.Spec.Stop = true
			// Invalid configuration must fail before any Docker API operation.
			o.quiesce = docker.NewDurableQuiescer(nil, stateDir)
			var hooks, movers int
			o.hooks = lifecycleHooks{exec: func(context.Context, string) error {
				hooks++
				return nil
			}}
			e.run = func(context.Context, mover.Request) (mover.Result, bool, error) {
				movers++
				return mover.Result{}, false, nil
			}
			_, err := o.RunTarget(context.Background(), job, target)
			if err == nil || !strings.Contains(err.Error(), "SABON_STATE_DIR") {
				t.Fatalf("expected cold storage configuration error, got %v", err)
			}
			if hooks != 0 || movers != 0 {
				t.Fatalf("configuration failure ran lifecycle work: hooks=%d movers=%d", hooks, movers)
			}
		})
	}
}
