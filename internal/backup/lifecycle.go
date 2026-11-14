package backup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/davidborzek/sabon/internal/discovery"
)

func lifecycleKey(app, source string) string {
	if source != "" {
		return "source:" + source
	}
	return "app:" + app
}

func sourceContainer(job *discovery.Job) string {
	if job == nil {
		return ""
	}
	return job.Container
}

// Each recovery action gets its own budget: a timed-out hook must not consume
// the restart's budget (or that of a later cleanup hook).
func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
}

// lifecycleLocks protect shared dump artifacts across target repositories and
// named app specs sharing the same source container.
// Unlike repository locks, these cover pre-hooks through application recovery.
// Waiting is cancellable and happens before taking a global concurrency slot.
type lifecycleLocks struct {
	mu sync.Mutex
	m  map[string]chan struct{}
}

func (l *lifecycleLocks) lock(ctx context.Context, key string) (func(), error) {
	l.mu.Lock()
	if l.m == nil {
		l.m = make(map[string]chan struct{})
	}
	ch := l.m[key]
	if ch == nil {
		ch = make(chan struct{}, 1)
		l.m[key] = ch
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
func (o *Orchestrator) requireNoReaders(ctx context.Context, app, source string) error {
	// Query all apps: named specs can share one source container while using
	// different repositories. Legacy untagged movers can only match by app.
	runs, err := o.eng.ListRuns(ctx, "", "")
	if err != nil {
		return fmt.Errorf("verify app movers have exited: %w", err)
	}
	for _, run := range runs {
		if run.App != app && (source == "" || run.SourceContainer != source) {
			continue
		}
		if run.Status != "succeeded" && run.Status != "failed" && (run.Action == "backup" || run.Action == "restore") {
			return fmt.Errorf("app %q still has running %s mover %q; wait for it to exit before changing source data", app, run.Action, run.ID)
		}
	}
	return nil
}
