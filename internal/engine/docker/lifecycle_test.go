package docker

import (
	"context"
	"errors"
	"testing"

	"github.com/davidborzek/sabon/internal/mover"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
)

type historyClient struct {
	client.APIClient
	err   error
	state *container.State
}

func (c historyClient) ContainerList(context.Context, container.ListOptions) ([]container.Summary, error) {
	return []container.Summary{{ID: "synthetic-mover"}}, nil
}
func (c historyClient) ContainerInspect(context.Context, string) (container.InspectResponse, error) {
	return container.InspectResponse{ContainerJSONBase: &container.ContainerJSONBase{ID: "synthetic-mover", State: c.state}, Config: &container.Config{Labels: map[string]string{mover.LabelSourceContainer: "synthetic-source"}}}, c.err
}

func TestListRunsCannotHideInspectionFailure(t *testing.T) {
	failure := errors.New("synthetic inspection failure")
	d := &Engine{cli: historyClient{err: failure}}
	if _, err := d.ListRuns(context.Background(), "app", ""); !errors.Is(err, failure) {
		t.Fatalf("inspection failure hidden: %v", err)
	}
	d.cli = historyClient{err: errdefs.NotFound(failure)}
	if runs, err := d.ListRuns(context.Background(), "app", ""); err != nil || len(runs) != 0 {
		t.Fatalf("removed mover should be skipped: runs=%v err=%v", runs, err)
	}
}

func TestRunHistoryRequiresTerminalState(t *testing.T) {
	for _, status := range []string{"", "created", "restarting", "running", "paused", "exited", "dead"} {
		d := &Engine{cli: historyClient{state: &container.State{Status: status}}}
		runs, err := d.ListRuns(context.Background(), "app", "")
		if err != nil {
			t.Fatal(err)
		}
		want := "running"
		if status == "exited" || status == "dead" {
			want = "succeeded"
		}
		if runs[0].Status != want {
			t.Errorf("state=%q outcome=%q want=%q", status, runs[0].Status, want)
		}
		if runs[0].SourceContainer != "synthetic-source" {
			t.Error("lost mover source identity")
		}
	}
}
