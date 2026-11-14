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
