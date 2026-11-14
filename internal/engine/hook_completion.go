package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// HookCompletionChecker fences uncertain hook processes before another source
// mutation. It does not promise durable exec tracking across daemon restarts.
type HookCompletionChecker interface {
	CheckHookCompletion(context.Context) error
}

// HookUncertain preserves the runtime identity needed to inspect a command
// whose client connection ended without proof that the command exited.
type HookUncertain struct {
	Kind, ID string
	Err      error
}

func (e *HookUncertain) Error() string {
	return fmt.Sprintf("hook completion uncertain (%s %s): %v", e.Kind, e.ID, e.Err)
}
func (e *HookUncertain) Unwrap() error { return e.Err }

// HookTracker retains unresolved identities for this runtime instance. Checks
// are conservative across sources: one uncertain hook blocks all new hooks.
type HookTracker struct {
	mu      sync.Mutex
	pending map[string]hookCheck
}
type hookCheck struct {
	kind, id string
	check    func(context.Context) error
}

func (p *HookTracker) Add(kind, id string, check func(context.Context) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		p.pending = make(map[string]hookCheck)
	}
	p.pending[kind+":"+id] = hookCheck{kind, id, check}
}
func (p *HookTracker) Check(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []error
	for key, h := range p.pending {
		if err := h.check(ctx); err != nil {
			errs = append(errs, &HookUncertain{Kind: h.kind, ID: h.id, Err: err})
		} else {
			delete(p.pending, key)
		}
	}
	return errors.Join(errs...)
}
