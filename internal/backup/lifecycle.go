package backup

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Each recovery action gets its own budget: a timed-out hook must not consume
// the restart's budget (or that of a later cleanup hook).
func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
}

// lifecycleLocks protect shared dump artifacts across target repositories.
// Unlike repository locks, these cover pre-hooks through application recovery.
// Waiting is cancellable and happens before taking a global concurrency slot.
type lifecycleLocks struct {
	mu sync.Mutex
	m  map[string]chan struct{}
}

func (l *lifecycleLocks) lock(ctx context.Context, app string) (func(), error) {
	l.mu.Lock()
	if l.m == nil {
		l.m = make(map[string]chan struct{})
	}
	ch := l.m[app]
	if ch == nil {
		ch = make(chan struct{}, 1)
		l.m[app] = ch
	}
	l.mu.Unlock()
	select {
	case ch <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-ch
			return nil, err
		}
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// This also checks movers surviving an earlier daemon. A failure to inspect is
// not evidence that it is safe to overwrite or delete a dump artifact.
func (o *Orchestrator) requireNoReaders(ctx context.Context, app string) error {
	runs, err := o.eng.ListRuns(ctx, app, "")
	if err != nil {
		return fmt.Errorf("verify app movers have exited: %w", err)
	}
	for _, run := range runs {
		if run.Status != "succeeded" && run.Status != "failed" && (run.Action == "backup" || run.Action == "restore") {
			return fmt.Errorf("app %q still has running %s mover %q; wait for it to exit before changing source data", app, run.Action, run.ID)
		}
	}
	return nil
}
