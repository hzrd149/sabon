//go:build cold_e2e

package docker

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/davidborzek/sabon/internal/mover"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

// This contract-level Docker test never uses client.FromEnv/default sockets.
// Supply only a disposable daemon via SABON_COLD_E2E_DOCKER_HOST.
func TestColdDockerHardKill(t *testing.T) {
	host := os.Getenv("SABON_COLD_E2E_DOCKER_HOST")
	if host == "" {
		t.Skip("requires explicit disposable SABON_COLD_E2E_DOCKER_HOST")
	}
	cli, err := client.NewClientWithOpts(client.WithHost(host), client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	img := "busybox:1.37"
	if app := os.Getenv("SABON_COLD_E2E_CHILD_APP"); app != "" {
		q := NewDurableQuiescer(cli, os.Getenv("SABON_COLD_E2E_CHILD_STATE"))
		lease, err := q.AcquireCold(ctx, app, nil)
		if err != nil {
			t.Fatal(err)
		}
		req := mover.Request{}
		if err := lease.AttachMover(ctx, &req); err != nil {
			t.Fatal(err)
		}
		created, err := cli.ContainerCreate(ctx, &container.Config{Image: img, Cmd: []string{"sleep", "3600"}, Labels: req.Labels}, nil, nil, nil, req.Name)
		if err != nil {
			t.Fatal(err)
		}
		if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
			t.Fatal(err)
		}
		fmt.Println("READY", created.ID)
		select {}
	}
	pull, err := cli.ImagePull(ctx, img, image.PullOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(io.Discard, pull)
	pull.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, running := range []bool{true, false} {
		t.Run(fmt.Sprint(running), func(t *testing.T) {
			app, err := cli.ContainerCreate(ctx, &container.Config{Image: img, Cmd: []string{"sleep", "3600"}}, nil, nil, nil, "")
			if err != nil {
				t.Fatal(err)
			}
			defer cli.ContainerRemove(context.Background(), app.ID, container.RemoveOptions{Force: true})
			if running {
				if err := cli.ContainerStart(ctx, app.ID, container.StartOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestColdDockerHardKill$")
			cmd.Env = append(os.Environ(), "SABON_COLD_E2E_CHILD_APP="+app.ID, "SABON_COLD_E2E_CHILD_STATE="+dir)
			pipe, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			ready := make(chan string, 1)
			go func() {
				scanner := bufio.NewScanner(pipe)
				for scanner.Scan() {
					var tag, id string
					if _, err := fmt.Sscan(scanner.Text(), &tag, &id); err == nil && tag == "READY" {
						ready <- id
						return
					}
				}
				ready <- ""
			}()
			var moverID string
			select {
			case moverID = <-ready:
				if moverID == "" {
					t.Fatal("child failed before durable mover launch")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			defer cli.ContainerRemove(context.Background(), moverID, container.RemoveOptions{Force: true})
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			q := NewDurableQuiescer(cli, dir)
			if _, err := q.RecoverQuiesced(ctx); err == nil {
				t.Fatal("surviving mover was not a recovery barrier")
			}
			inspect, err := cli.ContainerInspect(ctx, app.ID)
			if err != nil {
				t.Fatal(err)
			}
			if inspect.State.Running {
				t.Fatal("writer resumed while mover survived")
			}
			zero := 0
			if err := cli.ContainerStop(ctx, moverID, container.StopOptions{Timeout: &zero}); err != nil {
				t.Fatal(err)
			}
			if n, err := q.RecoverQuiesced(ctx); n != 1 || err != nil {
				t.Fatalf("recovery: %d %v", n, err)
			}
			inspect, err = cli.ContainerInspect(ctx, app.ID)
			if err != nil {
				t.Fatal(err)
			}
			if inspect.State.Running != running {
				t.Fatalf("original running=%v recovered=%v", running, inspect.State.Running)
			}
			if n, err := q.RecoverQuiesced(ctx); n != 0 || err != nil {
				t.Fatalf("repeat recovery: %d %v", n, err)
			}
		})
	}
}
