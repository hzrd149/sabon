//go:build e2e

package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestColdBackupRestoreOriginalState runs the actual CLI and restic mover.
// Opt in only against an isolated daemon that sees the runner's temporary
// directory at the SAME absolute path (set TMPDIR to a shared bind directory):
//
//	SABON_E2E_ISOLATED=1 DOCKER_HOST=tcp://127.0.0.1:<isolated-port> \
//	  go test -tags e2e ./test/e2e -run '^TestColdBackupRestoreOriginalState$' -v -count=1
func TestColdBackupRestoreOriginalState(t *testing.T) {
	if os.Getenv("SABON_E2E_ISOLATED") != "1" {
		t.Skip("requires SABON_E2E_ISOLATED=1 and an explicit non-default DOCKER_HOST")
	}
	host := os.Getenv("DOCKER_HOST")
	u, err := url.Parse(host)
	if err != nil || host == "" || (u.Scheme != "tcp" && u.Scheme != "unix") {
		t.Fatal("DOCKER_HOST must explicitly select an isolated tcp:// or unix:// daemon")
	}
	if u.Scheme == "unix" {
		path := filepath.Clean(u.Path)
		defaults := []string{"/var/run/docker.sock", "/run/docker.sock", fmt.Sprintf("/run/user/%d/docker.sock", os.Getuid())}
		if runtimeDir := os.Getenv("XDG_RUNTIME_DIR"); runtimeDir != "" {
			defaults = append(defaults, filepath.Join(runtimeDir, "docker.sock"))
		}
		for _, socket := range defaults {
			if path == filepath.Clean(socket) {
				t.Fatal("refusing the default Docker socket; use a disposable isolated daemon")
			}
		}
	}

	// Do not inherit the developer's backup configuration, repository credentials,
	// or Docker context override. Keep transport/TLS settings for the explicit host.
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "SABON_") || strings.HasPrefix(key, "RESTIC_") || key == "DOCKER_CONTEXT" {
			continue
		}
		env = append(env, entry)
	}
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	command := func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		cmd.Dir = repoRoot
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	must := func(t *testing.T, args ...string) string {
		t.Helper()
		out, err := command(args...)
		if err != nil {
			t.Fatalf("%s failed: %v\n%s", strings.Join(args, " "), err, out)
		}
		return out
	}

	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	id := "sabon-cold-e2e-" + hex.EncodeToString(token[:])
	fixtureLabel := "test.sabon.cold-fixture=" + id
	image := id + ":test"
	cache := id + "-cache"
	// Root movers write repository files; remove them with a fixture-scoped
	// helper before deleting the runner-owned directory.
	work, err := os.MkdirTemp("", id+"-")
	if err != nil {
		t.Fatal(err)
	}
	var volumes []string
	sharedPathVerified := false
	t.Cleanup(func() {
		out, err := command("docker", "ps", "-aq", "--filter", "label="+fixtureLabel)
		if err != nil {
			t.Errorf("list fixture containers for cleanup: %v\n%s", err, out)
		} else if ids := strings.Fields(out); len(ids) > 0 {
			if out, err := command(append([]string{"docker", "rm", "-f"}, ids...)...); err != nil {
				t.Errorf("remove fixture containers: %v\n%s", err, out)
			}
		}
		for _, volume := range volumes {
			if out, err := command("docker", "volume", "rm", volume); err != nil {
				t.Errorf("remove fixture volume %s: %v\n%s", volume, err, out)
			}
		}
		_, _ = command("docker", "image", "rm", image)
		if sharedPathVerified {
			if out, err := command("docker", "run", "--rm", "--mount", "type=bind,src="+work+",dst=/fixture", "alpine:3", "sh", "-c", "rm -rf /fixture/repo"); err != nil {
				t.Errorf("remove fixture repository: %v\n%s", err, out)
			}
		}
		if err := os.RemoveAll(work); err != nil {
			t.Errorf("remove fixture directory: %v", err)
		}
	})

	must(t, "docker", "version")
	must(t, "docker", "pull", "alpine:3")
	// --mount fails rather than creating a missing daemon-side directory. The
	// nonce verifies the bind is really the same fixture before any repo writes.
	if err := os.WriteFile(filepath.Join(work, "probe"), []byte(id), 0600); err != nil {
		t.Fatal(err)
	}
	if got := must(t, "docker", "run", "--rm", "--mount", "type=bind,src="+work+",dst=/fixture,readonly", "alpine:3", "cat", "/fixture/probe"); got != id {
		t.Fatal("isolated daemon must share the runner's temporary fixture path")
	}
	sharedPathVerified = true
	must(t, "docker", "build", "-f", filepath.Join(repoRoot, "Dockerfile.mover"), "-t", image, "--build-arg", "VERSION=e2e", repoRoot)
	bin := filepath.Join(work, "sabon")
	must(t, "go", "build", "-o", bin, "./cmd/sabon")
	must(t, "docker", "volume", "create", "--label", fixtureLabel, cache)
	volumes = append(volumes, cache)

	state := filepath.Join(work, "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	targets := filepath.Join(work, "targets.yaml")
	yaml := fmt.Sprintf("targets:\n  - name: onsite\n    path: %s\n    schedule: \"0 0 * * * *\"\n    retention:\n      last: 3\n", strconv.Quote(filepath.Join(work, "repo")))
	if err := os.WriteFile(targets, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	env = append(env,
		"SABON_RUNTIME=standalone", "SABON_CONFIG="+targets,
		"SABON_STATE_DIR="+state, "SABON_INSTANCE="+id,
		"SABON_MOVER_IMAGE="+image, "SABON_MOVER_USER=0:0",
		"SABON_MOVER_LABELS="+fixtureLabel, "SABON_CACHE_VOLUME="+cache,
		"SABON_SNAPSHOT=none", "SABON_BACKUP_TIMEOUT=2m",
		"RESTIC_PASSWORD=cold-e2e-fixture-password",
	)
	for _, running := range []bool{true, false} {
		name := "stopped"
		if running {
			name = "running"
		}
		t.Run(name, func(t *testing.T) {
			app := id + "-" + name
			volume := app + "-data"
			must(t, "docker", "volume", "create", "--label", fixtureLabel, volume)
			volumes = append(volumes, volume)
			volumeCommand := func(script string, args ...string) string {
				t.Helper()
				cmd := []string{"docker", "run", "--rm", "--label", fixtureLabel, "--mount", "type=volume,src=" + volume + ",dst=/data", "alpine:3", "sh", "-eu", "-c", script, "fixture"}
				return must(t, append(cmd, args...)...)
			}
			payload := "original cold backup bytes: " + app + "\n"
			volumeCommand("printf '%s' \"$1\" > /data/deleted.txt; printf '%s' \"$1\" > /data/modified.txt", payload)
			spec := "repo: " + app + "\nauto: true\nstop: true\n"
			must(t, "docker", "run", "-d", "--name", app, "--label", fixtureLabel,
				"--label", "sabon.instance="+id, "--label", "sabon.enable=true", "--label", "sabon.backup="+spec,
				"--mount", "type=volume,src="+volume+",dst=/data", "alpine:3", "sleep", "3600")
			if !running {
				must(t, "docker", "stop", "--time", "1", app)
			}
			assertState := func(stage string) {
				t.Helper()
				got := strings.TrimSpace(must(t, "docker", "inspect", "--format", "{{.State.Running}}", app))
				if got != strconv.FormatBool(running) {
					t.Fatalf("%s: original running=%v, actual=%s", stage, running, got)
				}
			}
			assertState("before backup")
			must(t, bin, "backup", "--app", app, "--target", "onsite")
			assertState("after backup")
			volumeCommand("rm /data/deleted.txt; printf changed > /data/modified.txt")
			if got := volumeCommand("test ! -e /data/deleted.txt; cat /data/modified.txt"); got != "changed" {
				t.Fatalf("fixture mutation failed: %q", got)
			}
			must(t, bin, "restore", "--app", app, "--target", "onsite", "--in-place", "--stop")
			assertState("after restore")
			for _, file := range []string{"deleted.txt", "modified.txt"} {
				if got := volumeCommand("cat \"/data/$1\"", file); got != payload {
					t.Fatalf("restored %s: got %q, want %q", file, got, payload)
				}
			}
			// A fresh CLI process uses the same persistent state and has no stale
			// obligation capable of changing the original state after success.
			must(t, bin, "recover")
			assertState("after recovery retry")
		})
	}
}
