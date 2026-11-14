package mover

import (
	"context"
	"errors"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type coldRunnerClient struct {
	client.APIClient
	list      []container.Summary
	removed   []string
	removeErr error
	bounded   bool
}

func (f *coldRunnerClient) ContainerList(context.Context, container.ListOptions) ([]container.Summary, error) {
	return f.list, nil
}
func (f *coldRunnerClient) ContainerRemove(ctx context.Context, id string, _ container.RemoveOptions) error {
	f.removed = append(f.removed, id)
	_, f.bounded = ctx.Deadline()
	return f.removeErr
}
func (f *coldRunnerClient) ImageInspect(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
	return image.InspectResponse{}, nil
}
func (f *coldRunnerClient) ContainerCreate(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error) {
	return container.CreateResponse{ID: "mover"}, nil
}
func (f *coldRunnerClient) ContainerStart(context.Context, string, container.StartOptions) error {
	return nil
}
func (f *coldRunnerClient) ContainerWait(context.Context, string, container.WaitCondition) (<-chan container.WaitResponse, <-chan error) {
	errs := make(chan error, 1)
	errs <- errors.New("lost wait response")
	return make(chan container.WaitResponse), errs
}

func TestColdReapSparesAllNonterminalMovers(t *testing.T) {
	f := &coldRunnerClient{}
	for _, state := range []string{"created", "running", "paused", "restarting", "removing", "unknown", "exited", "dead"} {
		f.list = append(f.list, container.Summary{ID: state, State: state, Labels: map[string]string{LabelColdOperation: "operation", LabelAction: "backup"}})
	}
	n, err := NewRunner(f).Reap(context.Background(), 0)
	if err != nil || n != 2 {
		t.Fatalf("reap=%d %v", n, err)
	}
	for _, id := range f.removed {
		if id != "exited" && id != "dead" {
			t.Fatalf("removed nonterminal cold mover %s", id)
		}
	}
}

func TestColdRunnerRemovalFailurePropagates(t *testing.T) {
	failure := errors.New("remove unavailable")
	f := &coldRunnerClient{removeErr: failure}
	_, kept, err := NewRunner(f).Run(context.Background(), Request{Image: "test-image", Spec: Spec{App: "app", Action: ActionBackup}, Labels: map[string]string{LabelColdOperation: "operation"}})
	if kept || !errors.Is(err, failure) {
		t.Fatalf("lost cold removal failure: kept=%v err=%v", kept, err)
	}
	if !f.bounded {
		t.Fatal("cold cleanup is unbounded")
	}
}
