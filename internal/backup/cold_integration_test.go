package backup

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/davidborzek/sabon/api"
	"github.com/davidborzek/sabon/internal/config"
	"github.com/davidborzek/sabon/internal/discovery"
	"github.com/davidborzek/sabon/internal/engine"
	"github.com/davidborzek/sabon/internal/mover"
)

type leaseQuiescer struct {
	engine.Quiescer // legacy Stop/Start intentionally unavailable
	lease           *testColdLease
	acquireErr      error
}

func (q leaseQuiescer) AcquireCold(context.Context, string, []discovery.Source) (engine.ColdLease, error) {
	return q.lease, q.acquireErr
}

type testColdLease struct {
	attached, released    bool
	attachErr, releaseErr error
}

func (l *testColdLease) AttachMover(_ context.Context, r *mover.Request) error {
	l.attached = true
	r.Name = "durable-name"
	return l.attachErr
}
func (l *testColdLease) Release(ctx context.Context) error {
	l.released = true
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return l.releaseErr
}

func (l *testColdLease) CheckIdle(context.Context) error { return nil }

type coldTestEngine struct {
	engine.Engine
	lease  *testColdLease
	ran    bool
	runErr error
}

func (e *coldTestEngine) RunMover(_ context.Context, r mover.Request) (mover.Result, bool, error) {
	if !e.lease.attached || r.Name != "durable-name" {
		panic("mover launched without attached durable intent")
	}
	e.ran = true
	return mover.Result{}, false, e.runErr
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
