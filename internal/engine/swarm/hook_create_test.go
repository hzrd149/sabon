package swarm

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/davidborzek/sabon/api"
	"github.com/davidborzek/sabon/internal/backup"
	"github.com/davidborzek/sabon/internal/config"
	"github.com/davidborzek/sabon/internal/discovery"
	"github.com/davidborzek/sabon/internal/engine"
	"github.com/davidborzek/sabon/internal/mover"
	"github.com/docker/docker/api/types/swarm"
)

type lostHookCreate struct {
	fakeSwarm
	creates     int
	taskService string
}

func (f *lostHookCreate) ServiceCreate(_ context.Context, spec swarm.ServiceSpec, _ swarm.ServiceCreateOptions) (swarm.ServiceCreateResponse, error) {
	f.creates++
	// The manager accepted this service, but the client lost the response.
	f.inspectSvc = swarm.Service{ID: "synthetic-accepted-hook", Spec: spec}
	return swarm.ServiceCreateResponse{}, context.DeadlineExceeded
}
func (f *lostHookCreate) TaskList(_ context.Context, opts swarm.TaskListOptions) ([]swarm.Task, error) {
	services := opts.Filters.Get("service")
	if len(services) > 0 {
		f.taskService = services[0]
	}
	return f.tasks, nil
}

type noHookMover struct {
	engine.Engine
	engine.Host
	runs int
}

func (e *noHookMover) EnsureCache(context.Context, string) error { return nil }
func (e *noHookMover) ListRuns(context.Context, string, string) ([]engine.RunInfo, error) {
	return nil, nil
}
func (e *noHookMover) RunMover(context.Context, mover.Request) (mover.Result, bool, error) {
	e.runs++
	return mover.Result{}, false, nil
}

func TestAcceptedHookCreateLostResponseBlocksLifecycle(t *testing.T) {
	t.Setenv("SABON_TEST_LOST_CREATE_PASSWORD", "synthetic")
	f := &lostHookCreate{fakeSwarm: fakeSwarm{tasks: []swarm.Task{{Status: swarm.TaskStatus{State: swarm.TaskStateRunning}}}}}
	h := NewHooks(f)
	e := &noHookMover{}
	o := backup.New(&config.Config{}, "synthetic", e, h, nil, e, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	job := discovery.Job{App: "synthetic-app", Container: "synthetic-source", Spec: api.Spec{
		PreHooks:  []api.Hook{{Image: "synthetic-writer", Command: []string{"writer"}}},
		PostHooks: []api.Hook{{Image: "synthetic-cleanup", Command: []string{"cleanup"}}},
	}}
	target := api.Target{Name: "one", Repo: "synthetic-repo", PasswordEnv: "SABON_TEST_LOST_CREATE_PASSWORD"}
	_, err := o.RunTarget(context.Background(), job, target)
	var uncertain *engine.HookUncertain
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &uncertain) || uncertain.Kind != "swarm-service-name" || uncertain.ID != f.inspectSvc.Spec.Name {
		t.Fatalf("creation uncertainty or operation name lost: %v", err)
	}
	if f.creates != 1 || e.runs != 0 || len(f.removed) != 0 {
		t.Fatalf("cleanup/backup ran despite lost response: creates=%d movers=%d removed=%v", f.creates, e.runs, f.removed)
	}
	job.App = "another-named-spec"
	if _, err := o.RunTarget(context.Background(), job, target); err == nil {
		t.Fatal("subsequent preparation ignored uncertain creation")
	}
	if f.creates != 1 {
		t.Fatal("subsequent preparation or cleanup launched a service")
	}
	if f.taskService != "synthetic-accepted-hook" {
		t.Fatalf("did not resolve operation to accepted service: %q", f.taskService)
	}
	f.tasks = nil
	if h.CheckHookCompletion(context.Background()) == nil {
		t.Fatal("missing task evidence mistaken for termination")
	}
	f.tasks = []swarm.Task{completeTask(0)}
	if err := h.CheckHookCompletion(context.Background()); err != nil {
		t.Fatalf("confirmed termination did not release fence: %v", err)
	}
	job.Spec.PreHooks, job.Spec.PostHooks = nil, nil
	if _, err := o.RunTarget(context.Background(), job, target); err != nil || e.runs != 1 {
		t.Fatalf("backup did not resume after confirmation: err=%v movers=%d", err, e.runs)
	}
}

func TestUnresolvedHookCreationRequiresEvidence(t *testing.T) {
	f := &lostHookCreate{}
	h := NewHooks(f)
	if err := h.RunOneShot(context.Background(), engine.RunSpec{Name: "synthetic-operation", Image: "synthetic"}); err == nil {
		t.Fatal("lost create response accepted")
	}
	accepted := f.inspectSvc
	f.inspectSvc = swarm.Service{} // no matching service visible yet
	if h.CheckHookCompletion(context.Background()) == nil {
		t.Fatal("absence mistaken for rejected creation")
	}
	f.inspectSvc = accepted
	f.inspectSvc.Spec.Labels = map[string]string{engine.HookLabelKey: "unrelated-operation"}
	f.tasks = []swarm.Task{completeTask(0)}
	if h.CheckHookCompletion(context.Background()) == nil {
		t.Fatal("unrelated service mistaken for operation evidence")
	}
	f.inspectSvc.Spec.Labels = map[string]string{engine.HookLabelKey: "synthetic-operation"}
	if err := h.CheckHookCompletion(context.Background()); err != nil {
		t.Fatal(err)
	}
}
