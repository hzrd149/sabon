//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDestinationGuard requires an explicitly selected isolated, local daemon:
//
//	SABON_E2E_ISOLATED=1 DOCKER_HOST=unix:///path/to/isolated/docker.sock \
//	  go test -tags e2e ./test/e2e -run '^TestDestinationGuard$' -count=1 -timeout=15m -v
//
// The writable fallback is synthetic: no real network mount is removed. Bind
// paths must be visible to the daemon on the same host as this test process.
func TestDestinationGuard(t *testing.T) {
	if os.Getenv("SABON_E2E_ISOLATED") != "1" {
		t.Skip("requires SABON_E2E_ISOLATED=1 and an isolated Docker daemon")
	}
	host := os.Getenv("DOCKER_HOST")
	if host == "" || host == "unix:///var/run/docker.sock" || host == "unix:///run/docker.sock" {
		t.Fatal("set DOCKER_HOST explicitly to the isolated daemon; refusing default Docker")
	}
	// DOCKER_CONTEXT otherwise takes precedence over DOCKER_HOST in the CLI.
	t.Setenv("DOCKER_CONTEXT", "")
	mustRun(t, nil, "docker", "version")
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("sabon-guard-%d-%d", os.Getpid(), time.Now().UnixNano())
	image := "sabon-mover:" + id
	volume, cache, workload := id+"-source", id+"-cache", id+"-app"
	// An explicit repo label makes this independent of implicit naming rules.
	appName := id + "-repo"
	mustRun(t, nil, "docker", "build", "-f", filepath.Join(root, "Dockerfile.mover"), "-t", image, "--build-arg", "VERSION=e2e-guard", root)
	t.Cleanup(func() { _, _ = run(t, "docker", "image", "rm", image) })
	bin := filepath.Join(t.TempDir(), "sabon")
	mustRun(t, nil, "go", "build", "-o", bin, filepath.Join(root, "cmd", "sabon"))

	// Root-owned repository files are cleaned through this test's own image.
	work, err := os.MkdirTemp("", id+"-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = run(t, "docker", "run", "--rm", "--mount", "type=bind,src="+work+",dst=/work", "--entrypoint", "sh", image, "-c", "rm -rf /work/repositories /work/restore")
		if err := os.RemoveAll(work); err != nil {
			t.Errorf("cleanup test directory: %v", err)
		}
	})
	base := filepath.Join(work, "repositories")
	repo := filepath.Join(base, appName)
	if err := os.MkdirAll(repo, 0777); err != nil {
		t.Fatal(err)
	}
	// Prove the underlying fallback directory is writable, rather than relying
	// on a missing directory or permission error to prevent initialization.
	probe := filepath.Join(repo, "writable-probe")
	if err := os.WriteFile(probe, []byte("synthetic"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(probe); err != nil {
		t.Fatal(err)
	}
	mustRun(t, nil, "docker", "volume", "create", volume)
	t.Cleanup(func() { _, _ = run(t, "docker", "volume", "rm", volume, cache) })
	const payload = "synthetic destination guard round-trip\n"
	mustRun(t, nil, "docker", "run", "--rm", "--mount", "type=volume,src="+volume+",dst=/source", "--entrypoint", "sh", image,
		"-c", "printf '%s' \"$1\" > /source/data.txt", "sh", payload)
	mustRun(t, nil, "docker", "run", "-d", "--name", workload,
		"--mount", "type=volume,src="+volume+",dst=/data",
		"--label", "sabon.enable=true", "--label", "sabon.backup=repo: "+appName+"\nauto: true\n",
		"--entrypoint", "sleep", image, "900")
	t.Cleanup(func() {
		_, _ = run(t, "docker", "rm", "-f", workload)
		out, err := run(t, "docker", "ps", "-aq", "--filter", "label=sabon.app="+appName)
		if err == nil {
			for _, containerID := range strings.Fields(out) {
				_, _ = run(t, "docker", "rm", "-f", containerID)
			}
		}
	})
	targets := filepath.Join(work, "targets.yaml")
	writeTargets := func(marker string, require bool) {
		t.Helper()
		config := fmt.Sprintf("targets:\n  - name: guarded\n    path: %q\n    destinationMarker: %q\n    requireExistingRepository: %t\n", base, marker, require)
		if err := os.WriteFile(targets, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
	}
	env := []string{
		"SABON_CONFIG=" + targets, "SABON_MOVER_IMAGE=" + image,
		"SABON_CACHE_VOLUME=" + cache, "RESTIC_PASSWORD=synthetic-guard-password",
		"SABON_RUNTIME=standalone", "SABON_MOVER_USER=0:0", "SABON_SNAPSHOT=none",
		"SABON_LABEL_PREFIX=sabon", "SABON_MOVER_HISTORY=3",
		"SABON_NOTIFY_URLS=", "SABON_MOVER_NETWORK=", "SABON_INSTANCE=",
		"SABON_BACKUP_TIMEOUT=2m",
	}
	backupArgs := []string{"backup", "--app", appName, "--target", "guarded"}
	expectFailure := func(reason string, wantEntries int) {
		t.Helper()
		out, err := runEnv(env, bin, backupArgs...)
		if err == nil || !strings.Contains(out, reason) {
			t.Fatalf("want failure containing %q; error=%v\n%s", reason, err, out)
		}
		if strings.Contains(out, "backup complete") {
			t.Fatalf("failed guard reported backup success:\n%s", out)
		}
		if _, err := os.Stat(filepath.Join(repo, "config")); !os.IsNotExist(err) {
			t.Fatalf("fallback repository config should not exist: %v", err)
		}
		entries, err := os.ReadDir(repo)
		if err != nil || len(entries) != wantEntries {
			t.Fatalf("guard modified destination: entries=%v error=%v", entries, err)
		}
	}
	expectSuccess := func() {
		t.Helper()
		out := mustRun(t, env, bin, backupArgs...)
		if !strings.Contains(out, "backup complete") {
			t.Fatalf("backup did not complete:\n%s", out)
		}
		if _, err := os.Stat(filepath.Join(repo, "config")); err != nil {
			t.Fatalf("repository config missing: %v", err)
		}
	}

	writeTargets("synthetic-destination-57", false)
	expectFailure("destination marker", 0)
	marker := filepath.Join(repo, ".sabon-destination")
	if err := os.WriteFile(marker, []byte("wrong-destination\n"), 0644); err != nil {
		t.Fatal(err)
	}
	expectFailure("destination marker identity mismatch", 1)
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	// Independent of the marker: require-existing alone must never initialize.
	writeTargets("", true)
	expectFailure("required existing repository", 0)

	// Only the operator/test provisions the marker; a final newline is accepted.
	if err := os.WriteFile(marker, []byte("synthetic-destination-57\n"), 0644); err != nil {
		t.Fatal(err)
	}
	writeTargets("synthetic-destination-57", false)
	expectSuccess()
	writeTargets("synthetic-destination-57", true)
	expectSuccess()
	restore := filepath.Join(work, "restore")
	mustRun(t, env, bin, "restore", "--app", appName, "--target", "guarded", "--into", restore)
	// Named volume sources are mounted by name at /data/<volume> in the mover.
	got, err := os.ReadFile(filepath.Join(restore, "data", volume, "data.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != payload {
		t.Fatalf("restored payload = %q, want %q", got, payload)
	}
	gotMarker, err := os.ReadFile(marker)
	if err != nil || string(gotMarker) != "synthetic-destination-57\n" {
		t.Fatalf("operator marker changed: %q, %v", gotMarker, err)
	}
}
