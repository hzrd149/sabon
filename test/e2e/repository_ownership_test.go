//go:build e2e

package e2e

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

// TestRepositoryOwnership requires an explicitly selected isolated Docker
// daemon. With DOCKER_HOST (and TLS settings, if needed) already configured:
//
//	SABON_E2E_ISOLATED=1 go test -tags e2e ./test/e2e -run '^TestRepositoryOwnership$' -count=1 -v
//
// All daemon-side data lives in uniquely named volumes, so the daemon may be
// remote/DinD: no bind path needs to be shared with the machine running go test.
// Scheduled coverage is exercised separately by the reconcile unit tests.
func TestRepositoryOwnership(t *testing.T) {
	if os.Getenv("SABON_E2E_ISOLATED") != "1" {
		t.Skip("requires SABON_E2E_ISOLATED=1 and an isolated Docker daemon")
	}
	if out, err := run(t, "docker", "version"); err != nil {
		t.Fatalf("isolated Docker daemon unavailable: %v\n%s", err, out)
	}
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	id := "sabon-ownership-" + hex.EncodeToString(nonce[:])
	const fixtureKey = "test.sabon.ownership"
	fixtureLabel := fixtureKey + "=" + id
	image := "sabon-mover:" + id
	var volumes []string
	imageBuilt := false
	// Remove only this test's labelled containers (including retained movers),
	// then its volumes and image tag. Never prune or remove shared base images.
	t.Cleanup(func() {
		cleanup := func(args ...string) {
			if out, err := run(t, "docker", args...); err != nil {
				t.Errorf("cleanup docker %v: %v\n%s", args, err, out)
			}
		}
		out, err := run(t, "docker", "ps", "-aq", "--filter", "label="+fixtureLabel)
		if err != nil {
			t.Errorf("list fixture containers for cleanup: %v\n%s", err, out)
		} else if ids := strings.Fields(out); len(ids) > 0 {
			cleanup(append([]string{"rm", "-f"}, ids...)...)
		}
		for _, volume := range volumes {
			cleanup("volume", "rm", volume)
		}
		if imageBuilt {
			cleanup("image", "rm", image)
		}
	})
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	mustRun(t, nil, "docker", "build", "-f", filepath.Join(root, "Dockerfile.mover"), "-t", image, "--build-arg", "VERSION=e2e-ownership", root)
	imageBuilt = true
	bin := filepath.Join(t.TempDir(), "sabon")
	mustRun(t, nil, "go", "build", "-o", bin, filepath.Join(root, "cmd/sabon"))
	newVolume := func(suffix string) string {
		t.Helper()
		name := id + "-" + suffix
		mustRun(t, nil, "docker", "volume", "create", "--label", fixtureLabel, name)
		volumes = append(volumes, name)
		return name
	}
	workspace := newVolume("workspace")
	cache := newVolume("cache")
	// The local-volume mountpoint is a daemon-host path, which the movers can
	// bind directly. The test reads it through a helper mounting the volume.
	workspacePath := strings.TrimSpace(mustRun(t, nil, "docker", "volume", "inspect", "--format", "{{.Mountpoint}}", workspace))
	if !path.IsAbs(workspacePath) || workspacePath == "/" {
		t.Fatalf("unexpected workspace mountpoint %q", workspacePath)
	}
	helper := func(volume string, command ...string) string {
		t.Helper()
		args := []string{"run", "--rm", "--label", fixtureLabel, "-v", volume + ":/fixture", "--entrypoint", "sh", image, "-c"}
		return mustRun(t, nil, "docker", append(args, command...)...)
	}
	helper(workspace, "mkdir -p /fixture/repos /fixture/restore")
	targets := filepath.Join(t.TempDir(), "targets.yaml")
	if err := os.WriteFile(targets, []byte(fmt.Sprintf("targets:\n  - name: onsite\n    path: %q\n", path.Join(workspacePath, "repos"))), 0o600); err != nil {
		t.Fatal(err)
	}
	envFor := func(instance string) []string {
		return []string{
			"SABON_CONFIG=" + targets,
			"SABON_RUNTIME=standalone",
			"SABON_LABEL_PREFIX=sabon",
			"SABON_INSTANCE=" + instance,
			"SABON_MOVER_IMAGE=" + image,
			"SABON_MOVER_LABELS=" + fixtureLabel,
			"SABON_CACHE_VOLUME=" + cache,
			"SABON_SNAPSHOT=none",
			"RESTIC_PASSWORD=synthetic-ownership-password",
		}
	}
	runCLI := func(instance string, args ...string) (string, error) {
		cmd := exec.Command(bin, args...)
		// Preserve Docker connection settings, but never inherit operational
		// sabon/restic configuration (notifications, hooks, credentials, etc.).
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "SABON_") && !strings.HasPrefix(entry, "RESTIC_") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		cmd.Env = append(cmd.Env, envFor(instance)...)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	mustCLI := func(args ...string) string {
		t.Helper()
		out, err := runCLI(id, args...)
		if err != nil {
			t.Fatalf("sabon %v failed: %v\n%s", args, err, out)
		}
		return out
	}
	createOwner := func(name, project, volume, instance, backupKey, spec string) {
		t.Helper()
		// Stopped containers are deliberately included in discovery; they need
		// no running process and their named-volume contents are stable.
		mustRun(t, nil, "docker", "create", "--name", name, "--label", fixtureLabel,
			"--label", "com.docker.compose.project="+project, "--label", "com.docker.compose.service=db",
			"--label", "sabon.enable=true", "--label", "sabon.instance="+instance,
			"--label", backupKey+"="+spec, "-v", volume+":/data", image)
	}
	projects := []string{id + "-alpha", id + "-beta"}
	sources := []string{newVolume("alpha-data"), newVolume("beta-data")}
	for i, project := range projects {
		payload := "sentinel for " + project
		helper(sources[i], `printf '%s' "$1" > /fixture/sentinel.txt`, "seed", payload)
		createOwner(project+"-db-1", project, sources[i], id, "sabon.backup", "auto: true\n")
	}
	out := mustCLI("backup")
	if strings.Count(out, "backup complete") != 2 {
		t.Fatalf("want two completed backups:\n%s", out)
	}
	for i, project := range projects {
		app := project + "-db"
		helper(workspace, `test -f "/fixture/repos/$1/config"`, "repo", app)
		out := mustCLI("snapshots", "--app", app, "--target", "onsite")
		if !strings.Contains(out, app) {
			t.Fatalf("snapshot missing for %s:\n%s", app, out)
		}
		mustCLI("restore", "--app", app, "--target", "onsite", "--into", path.Join(workspacePath, "restore", app))
		// Auto-discovered named volumes are mounted at /data/<volume> in the
		// mover; restic preserves that path beneath the staging directory.
		restored := path.Join("/fixture/restore", app, "data", sources[i], "sentinel.txt")
		got := helper(workspace, `cat "$1"`, "read", restored)
		if want := "sentinel for " + project; got != want {
			t.Fatalf("%s restored %q, want %q", app, got, want)
		}
		other := path.Join("/fixture/restore", app, "data", sources[1-i], "sentinel.txt")
		helper(workspace, `test ! -e "$1"`, "other-source", other)
	}
	// An explicit shared repo must fail before either owner gets a mover.
	// Cover plain/plain and plain/named, using separate discovery instances so
	// the successful default-name fixtures remain undisturbed.
	for _, kind := range []string{"plain", "named"} {
		instance := id + "-" + kind
		shared := instance + "-shared"
		for i, project := range projects {
			key := "sabon.backup"
			if kind == "named" && i == 1 {
				key += ".data"
			}
			createOwner(project+"-"+kind, project, sources[i], instance, key, "repo: "+shared+"\nauto: true\n")
		}
		moverIDs := func() string {
			return mustRun(t, nil, "docker", "ps", "-aq", "--filter", "label="+fixtureLabel, "--filter", "label=sabon.app")
		}
		before := moverIDs()
		out, err := runCLI(instance, "backup")
		if err == nil {
			t.Fatalf("shared repository backup unexpectedly succeeded:\n%s", out)
		}
		for _, text := range []string{"conflicting repository ownership", shared, projects[0] + "-" + kind, projects[1] + "-" + kind, "distinct repo"} {
			if !strings.Contains(out, text) {
				t.Errorf("conflict error missing %q:\n%s", text, out)
			}
		}
		if strings.Contains(out, "backup starting") || moverIDs() != before {
			t.Fatalf("conflict started a backup:\n%s", out)
		}
		helper(workspace, `test ! -e "/fixture/repos/$1"`, "shared-repo", shared)
	}
}
