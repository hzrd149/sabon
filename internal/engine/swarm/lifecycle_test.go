package swarm

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/davidborzek/sabon/internal/engine"
	"github.com/davidborzek/sabon/internal/mover"
	"github.com/docker/docker/api/types/swarm"
)

func TestRunInfoPreservesSourceIdentity(t *testing.T) {
	s := New(&fakeSwarm{})
	svc := swarm.Service{Spec: swarm.ServiceSpec{Annotations: swarm.Annotations{Labels: map[string]string{mover.LabelSourceContainer: "synthetic-source"}}}}
	if got := s.runInfo(context.Background(), svc); got.SourceContainer != "synthetic-source" {
		t.Fatalf("source identity lost: %+v", got)
	}
}

func TestOrphanedWorkerIsNotTerminationEvidence(t *testing.T) {
	f := &fakeSwarm{tasks: []swarm.Task{{Status: swarm.TaskStatus{State: swarm.TaskStateOrphaned}}}}
	s := testSwarm(f)
	if !s.serviceRunning(context.Background(), "svc") {
		t.Fatal("orphan considered safe to reap")
	}
	if got := s.runInfo(context.Background(), histSvc("svc", "app", "target", "backup")); got.Status != "running" {
		t.Fatalf("orphan history claims termination: %+v", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, kept, err := s.RunMover(ctx, mover.Request{Name: "synthetic", Image: "synthetic", Spec: mover.Spec{App: "synthetic"}})
	if !kept || !errors.Is(err, context.DeadlineExceeded) || len(f.removed) != 0 {
		t.Fatalf("orphan not fenced: kept=%v err=%v removed=%v", kept, err, f.removed)
	}
}

func TestSwarmHookUncertaintyRetainsService(t *testing.T) {
	f := &fakeSwarm{tasks: []swarm.Task{{Status: swarm.TaskStatus{State: swarm.TaskStateOrphaned}}}}
	h := NewHooks(f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := h.RunOneShot(ctx, engine.RunSpec{Name: "synthetic", Image: "synthetic"})
	var uncertain *engine.HookUncertain
	if !errors.Is(err, context.Canceled) || !errors.As(err, &uncertain) || len(f.removed) != 0 {
		t.Fatalf("hook uncertainty lost: err=%v removed=%v", err, f.removed)
	}
	f.tasks = nil // service/task disappearance is not proof of worker exit
	if h.CheckHookCompletion(context.Background()) == nil {
		t.Fatal("missing task evidence accepted")
	}
	f.tasks = []swarm.Task{completeTask(0)}
	if err := h.CheckHookCompletion(context.Background()); err != nil {
		t.Fatalf("confirmed worker exit not accepted: %v", err)
	}
}

type failedHookRemoval struct {
	fakeSwarm
	err error
}

func (f *failedHookRemoval) ServiceRemove(context.Context, string) error { return f.err }

func TestSwarmHookRemovalErrorPreserved(t *testing.T) {
	failure := errors.New("synthetic hook service removal failure")
	f := &failedHookRemoval{fakeSwarm: fakeSwarm{tasks: []swarm.Task{completeTask(2)}}, err: failure}
	err := NewHooks(f).RunOneShot(context.Background(), engine.RunSpec{Name: "synthetic", Image: "synthetic"})
	if !errors.Is(err, failure) {
		t.Fatalf("removal failure lost: %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "exited with code 2") {
		t.Fatalf("hook failure lost: %v", err)
	}
}
