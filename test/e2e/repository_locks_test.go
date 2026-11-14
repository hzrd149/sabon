//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRepositoryLocksAfterDaemonRestart requires a disposable, exclusively
// reserved Docker daemon and a TMPDIR bind-visible at the same path inside it.
// The orchestrator, mover binary, and restic locks are real. A test-only restic
// wrapper substitutes a synthetic stdin backup and keeps its stream open until
// released, avoiding huge fixtures or assumptions about backup throughput.
func TestRepositoryLocksAfterDaemonRestart(t *testing.T) {
	if os.Getenv("SABON_E2E_ISOLATED") != "1" {
		t.Skip("requires SABON_E2E_ISOLATED=1 and an exclusively reserved disposable daemon")
	}
	host := os.Getenv("DOCKER_HOST")
	if !strings.HasPrefix(host, "unix:///") || host == "unix:///var/run/docker.sock" || host == "unix:///run/docker.sock" {
		t.Fatal("set DOCKER_HOST explicitly to the isolated daemon's non-default Unix socket")
	}
	if os.Getenv("TMPDIR") == "" {
		t.Fatal("set TMPDIR to an isolated daemon bind-visible directory")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	work, err := os.MkdirTemp("", "sabon-locks-")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(work)
	image := name + ":test"
	// Deliberately do not inherit SABON_*, RESTIC_*, Docker contexts, or backend
	// credentials. Only the explicit isolated endpoint and fixture config apply.
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + work, "TMPDIR=" + work,
		"DOCKER_HOST=" + host, "DOCKER_CONFIG=" + filepath.Join(work, "docker-config")}
	command := func(c context.Context, extra []string, exe string, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(c, exe, args...)
		cmd.Env = append(append([]string(nil), env...), extra...)
		return cmd
	}
	run := func(extra []string, exe string, args ...string) (string, error) {
		out, err := command(ctx, extra, exe, args...).CombinedOutput()
		return string(out), err
	}
	must := func(extra []string, exe string, args ...string) string {
		t.Helper()
		out, err := run(extra, exe, args...)
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", exe, args, err, out)
		}
		return strings.TrimSpace(out)
	}
	// Cleanup is scoped to this fixture, with its own bounded context even when
	// the test deadline expires. Never prune the daemon or select other apps.
	t.Cleanup(func() {
		c, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		clean := func(args ...string) string {
			out, err := command(c, nil, "docker", args...).CombinedOutput()
			if err != nil {
				t.Logf("cleanup docker %v: %v: %s", args, err, out)
			}
			return string(out)
		}
		ids := strings.Fields(clean("ps", "-aq", "--filter", "label=sabon.app="+name))
		for _, id := range ids {
			clean("rm", "-f", id)
		}
		for _, id := range strings.Fields(clean("ps", "-aq", "--filter", "label=sabon.e2e.fixture="+name)) {
			clean("rm", "-f", id)
		}
		clean("rm", "-f", name)
		clean("volume", "rm", name+"-cache")
		clean("run", "--rm", "--name", name+"-cleanup", "--entrypoint", "/bin/sh", "-v", work+":/fixture", image, "-c", "rm -rf /fixture/repo /fixture/restore")
		clean("image", "rm", image)
		if err := os.RemoveAll(work); err != nil {
			t.Errorf("remove fixture: %v", err)
		}
	})
	must(nil, "docker", "version")
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	// Reuse the host Go cache without passing its environment to runtime commands.
	buildEnv := []string{"HOME=" + os.Getenv("HOME"), "CGO_ENABLED=0"}
	bin := filepath.Join(work, "sabon")
	must(buildEnv, "go", "build", "-o", bin, filepath.Join(root, "cmd/sabon"))
	build := filepath.Join(work, "image")
	if err := os.Mkdir(build, 0o755); err != nil {
		t.Fatal(err)
	}
	must(buildEnv, "go", "build", "-o", filepath.Join(build, "sabon-mover"), filepath.Join(root, "cmd/sabon-mover"))
	write := func(path, value string, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, []byte(value), mode); err != nil {
			t.Fatal(err)
		}
	}
	const payload = "repository-lock restart fixture\n"
	write(filepath.Join(build, "restic"), `#!/bin/sh
set -eu
set -o pipefail
if [ "$1" = backup ]; then
  (
    printf 'repository-lock restart fixture\n'
    n=0
    while [ ! -e /tmp/release-backup ]; do
      n=$((n+1))
      [ "$n" -lt 180 ] || exit 1
      sleep 1
    done
  ) | /usr/bin/restic.real backup --json --stdin --stdin-filename fixture
else
  exec /usr/bin/restic.real "$@"
fi
`, 0o755)
	write(filepath.Join(build, "Dockerfile"), `FROM restic/restic:0.19.1
RUN mv /usr/bin/restic /usr/bin/restic.real
COPY restic /usr/bin/restic
COPY sabon-mover /usr/local/bin/sabon-mover
ENTRYPOINT ["/usr/local/bin/sabon-mover"]
`, 0o644)
	must(nil, "docker", "build", "-t", image, build)
	t.Log(must(nil, "docker", "run", "--rm", "--name", name+"-version", "--label", "sabon.e2e.fixture="+name, "--entrypoint", "/usr/bin/restic.real", image, "version"))
	src := filepath.Join(work, "src")
	if err := os.Mkdir(src, 0o755); err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(src, "fixture"), payload, 0o644)
	targets := filepath.Join(work, "targets.yaml")
	write(targets, "targets:\n  - name: isolated\n    path: "+filepath.Join(work, "repo")+"\n    schedule: \"0 0 0 1 1 *\"\n", 0o644)
	spec := "repo: " + name + "\nauto: false\nextraPaths: [" + src + "]\n"
	must(nil, "docker", "run", "-d", "--name", name, "--entrypoint", "/bin/sh",
		"--label", "sabon.enable=true", "--label", "sabon.instance="+name,
		"--label", "sabon.backup="+spec, "-v", src+":/data:ro", image, "-c", "sleep 300")
	fixtureEnv := []string{"SABON_CONFIG=" + targets, "SABON_MOVER_IMAGE=" + image,
		"SABON_INSTANCE=" + name, "SABON_CACHE_VOLUME=" + name + "-cache",
		"SABON_RUNTIME=standalone", "SABON_LOG_LEVEL=debug", "SABON_METRICS_ADDR=127.0.0.1:0",
		"RESTIC_PASSWORD=isolated-lock-fixture"}
	waitFor := func(label string, ready func() bool) {
		t.Helper()
		deadline := time.NewTimer(30 * time.Second)
		defer deadline.Stop()
		for !ready() {
			select {
			case <-ctx.Done():
				t.Fatalf("%s: %v", label, ctx.Err())
			case <-deadline.C:
				t.Fatalf("timed out waiting for %s", label)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	startDaemon := func(startup bool, suffix string) (*exec.Cmd, <-chan struct{}, string) {
		t.Helper()
		logPath := filepath.Join(work, suffix+".log")
		log, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		cmd := command(ctx, append(append([]string(nil), fixtureEnv...), fmt.Sprintf("SABON_RUN_ON_STARTUP=%t", startup)), bin)
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			_ = log.Close()
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); _ = log.Close(); close(done) }()
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Errorf("%s daemon did not exit", suffix)
			}
			if t.Failed() {
				if data, err := os.ReadFile(logPath); err == nil {
					t.Logf("%s daemon log:\n%s", suffix, data)
				}
			}
		})
		return cmd, done, logPath
	}
	first, firstDone, _ := startDaemon(true, "before")
	var mover string
	waitFor("backup mover", func() bool {
		out, err := run(nil, "docker", "ps", "-q", "--filter", "label=sabon.app="+name, "--filter", "label=sabon.action=backup")
		mover = strings.TrimSpace(out)
		return err == nil && mover != "" && !strings.Contains(mover, "\n")
	})
	lockIDs := func() string {
		t.Helper()
		return must(nil, "docker", "exec", mover, "/bin/sh", "-c", `/usr/bin/restic.real list locks`)
	}
	var locks string
	waitFor("real repository lock", func() bool {
		out, err := run(nil, "docker", "exec", mover, "/usr/bin/restic.real", "list", "locks")
		locks = strings.TrimSpace(out)
		return err == nil && len(locks) == 64
	})
	// SIGKILL prevents the original runner from performing any shutdown cleanup.
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, _, restartedLog := startDaemon(false, "after")
	waitFor("restarted daemon reconciliation", func() bool {
		out, _ := os.ReadFile(restartedLog)
		return strings.Contains(string(out), "scheduled job")
	})
	assertOwner := func() {
		t.Helper()
		if state := must(nil, "docker", "inspect", "--format", "{{.State.Running}}", mover); state != "true" {
			t.Fatalf("original mover did not survive: %s", state)
		}
		if got := lockIDs(); got != locks {
			t.Fatalf("original lock changed: got %q want %q", got, locks)
		}
	}
	assertOwner()
	for _, action := range []string{"prune", "check"} {
		out, err := run(fixtureEnv, bin, action, "--app", name, "--target", "isolated")
		if err == nil || !strings.Contains(out, "repository is already locked") {
			t.Fatalf("separate CLI %s must fail on active lock: %v\n%s", action, err, out)
		}
		assertOwner()
	}
	must(nil, "docker", "exec", mover, "touch", "/tmp/release-backup")
	if code := must(nil, "docker", "wait", mover); code != "0" {
		t.Fatalf("backup mover exit %s: %s", code, must(nil, "docker", "logs", mover))
	}
	// Use the same real restic version to check all stored bytes, then exercise
	// the application's restore CLI and compare the exact synthetic payload.
	repo := filepath.Join(work, "repo", name)
	must(nil, "docker", "run", "--rm", "--name", name+"-integrity", "--label", "sabon.e2e.fixture="+name, "--entrypoint", "/usr/bin/restic.real", "-v", repo+":/repo",
		"-e", "RESTIC_REPOSITORY=/repo", "-e", "RESTIC_PASSWORD=isolated-lock-fixture", image, "check", "--read-data")
	restore := filepath.Join(work, "restore")
	must(fixtureEnv, bin, "restore", "--app", name, "--target", "isolated", "--into", restore)
	got, err := run(nil, "docker", "run", "--rm", "--name", name+"-read", "--label", "sabon.e2e.fixture="+name, "--entrypoint", "/bin/cat", "-v", restore+":/restore:ro", image, "/restore/fixture")
	if err != nil {
		t.Fatalf("read restored payload: %v\n%s", err, got)
	}
	if got != payload {
		t.Fatalf("restored payload = %q, want %q", got, payload)
	}
}
