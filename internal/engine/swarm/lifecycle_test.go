package swarm

import (
	"context"
	"testing"

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
