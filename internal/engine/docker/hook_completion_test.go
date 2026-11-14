package docker

import (
	"context"
	"errors"
	"testing"

	"github.com/davidborzek/sabon/internal/engine"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type uncertainHookClient struct {
	client.APIClient
	running       bool
	sourceRunning bool
	pid           int
	removeErr     error
}

func (c *uncertainHookClient) ContainerExecCreate(context.Context, string, container.ExecOptions) (container.ExecCreateResponse, error) {
	return container.ExecCreateResponse{ID: "synthetic-exec"}, nil
}
func (c *uncertainHookClient) ContainerExecAttach(context.Context, string, container.ExecAttachOptions) (types.HijackedResponse, error) {
	return types.HijackedResponse{}, context.DeadlineExceeded
}
func (c *uncertainHookClient) ContainerExecInspect(context.Context, string) (container.ExecInspect, error) {
	return container.ExecInspect{Running: c.running, Pid: c.pid}, nil
}
func (c *uncertainHookClient) ContainerInspect(_ context.Context, id string) (container.InspectResponse, error) {
	running := c.running
	if id == "synthetic-source" {
		running = c.sourceRunning
	}
	status := "exited"
	if running {
		status = "running"
	}
	return container.InspectResponse{ContainerJSONBase: &container.ContainerJSONBase{State: &container.State{Running: running, Status: status}}}, nil
}
func (c *uncertainHookClient) ImageInspect(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
	return image.InspectResponse{}, nil
}
func (c *uncertainHookClient) ContainerCreate(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error) {
	return container.CreateResponse{ID: "synthetic-hook-container"}, nil
}
func (c *uncertainHookClient) ContainerStart(context.Context, string, container.StartOptions) error {
	return context.DeadlineExceeded
}
func (c *uncertainHookClient) ContainerRemove(ctx context.Context, _ string, _ container.RemoveOptions) error {
	if _, ok := ctx.Deadline(); !ok || ctx.Err() != nil {
		panic("hook removal requires independent bounded context")
	}
	return c.removeErr
}

func TestExecTimeoutRetainsIdentityUntilExit(t *testing.T) {
	c := &uncertainHookClient{running: true, sourceRunning: true, pid: 123}
	h := NewHooks(c)
	err := h.Exec(context.Background(), "synthetic-source", []string{"writer"}, nil, "")
	var uncertain *engine.HookUncertain
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &uncertain) || uncertain.ID != "synthetic-exec" {
		t.Fatalf("lost timeout or exec identity: %v", err)
	}
	if h.CheckHookCompletion(context.Background()) == nil {
		t.Fatal("live exec not fenced")
	}
	c.running = false
	if err := h.CheckHookCompletion(context.Background()); err != nil {
		t.Fatalf("confirmed exit did not release fence: %v", err)
	}
}

func TestInterruptedAttachWithoutStartEvidenceStaysFenced(t *testing.T) {
	c := &uncertainHookClient{sourceRunning: true}
	h := NewHooks(c)
	err := h.Exec(context.Background(), "synthetic-source", []string{"writer"}, nil, "")
	var uncertain *engine.HookUncertain
	if !errors.As(err, &uncertain) {
		t.Fatalf("unstarted exec mistaken for exited exec: %v", err)
	}
	if h.CheckHookCompletion(context.Background()) == nil {
		t.Fatal("missing start/exit evidence accepted")
	}
	c.sourceRunning = false
	if err := h.CheckHookCompletion(context.Background()); err != nil {
		t.Fatalf("stopped source did not fence exec: %v", err)
	}
}

func TestHookRemovalFailureIsPreservedAndFenced(t *testing.T) {
	removeErr := errors.New("synthetic removal failure")
	c := &uncertainHookClient{running: true, removeErr: removeErr}
	h := NewHooks(c)
	err := h.RunOneShot(context.Background(), engine.RunSpec{Name: "synthetic-hook", Image: "synthetic"})
	var uncertain *engine.HookUncertain
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, removeErr) || !errors.As(err, &uncertain) || uncertain.ID != "synthetic-hook-container" {
		t.Fatalf("lost primary/removal/uncertainty: %v", err)
	}
	if h.CheckHookCompletion(context.Background()) == nil {
		t.Fatal("live container not fenced")
	}
	c.running = false
	if err := h.CheckHookCompletion(context.Background()); err != nil {
		t.Fatal(err)
	}
}
