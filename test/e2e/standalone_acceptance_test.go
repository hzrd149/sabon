//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestStandaloneAcceptance requires the dedicated nested daemon and a TMPDIR
// visible at the same path to that daemon. It never uses the default daemon:
//
//	SABON_E2E_ISOLATED=1 DOCKER_HOST=unix:///path/to/isolated/docker.sock \
//	TMPDIR=/path/shared-with-daemon go test -tags e2e ./test/e2e -run '^TestStandaloneAcceptance$' -v
//
// These are filesystem sentinel checks, not database consistency tests.
func TestStandaloneAcceptance(t *testing.T) {
	if os.Getenv("SABON_E2E_ISOLATED") != "1" {
		t.Skip("requires SABON_E2E_ISOLATED=1")
	}
	host := os.Getenv("DOCKER_HOST")
	if host == "" || host == "unix:///var/run/docker.sock" || host == "unix:///run/docker.sock" {
		t.Fatal("set DOCKER_HOST explicitly to an isolated daemon")
	}
	// Docker CLI contexts override DOCKER_HOST; pin both CLI and SDK to the
	// reserved socket even if the invoking shell has a selected context.
	t.Setenv("DOCKER_CONTEXT", "")
	mustRun(t, nil, "docker", "version")
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("sabon-accept-%d", time.Now().UnixNano())
	image := "sabon-mover:" + id
	mustRun(t, nil, "docker", "build", "-f", filepath.Join(root, "Dockerfile.mover"), "-t", image, "--build-arg", "VERSION=e2e", root)
	t.Cleanup(func() { mustRun(t, nil, "docker", "image", "rm", image) })
	bin := filepath.Join(t.TempDir(), "sabon")
	mustRun(t, nil, "go", "build", "-o", bin, filepath.Join(root, "cmd", "sabon"))

	t.Run("AutoNamedVolumesAndExclusion", func(t *testing.T) {
		f := newAcceptanceFixture(t, bin, image, id+"-auto")
		f.start(t)
		mustRun(t, f.env, bin, "backup")
		f.restore(t, "local", "latest", "roundtrip", "first", "second")
	})
	t.Run("NativeSFTPRestore", func(t *testing.T) {
		f := newAcceptanceFixture(t, bin, image, id+"-sftp")
		remote := f.sftp(t)
		// Only a remote target: no local repository can satisfy this restore.
		f.writeTargets(t, "targets:\n"+remote)
		f.start(t)
		mustRun(t, f.env, bin, "backup")
		f.restore(t, "remote", "latest", "remote-roundtrip", "first", "second")
	})
	t.Run("LocalSuccessRemoteCredentialFailure", func(t *testing.T) {
		f := newAcceptanceFixture(t, bin, image, id+"-failure")
		remote := f.sftp(t)
		// A valid but unauthorized key exercises native SSH authentication,
		// rather than failing config parsing before the local target runs.
		mustRun(t, nil, "docker", "run", "--rm", "-v", f.work+":/w", "--entrypoint", "ssh-keygen", image,
			"-q", "-t", "ed25519", "-N", "", "-f", "/w/keys/unauthorized")
		remote = strings.ReplaceAll(remote, "/keys/client,", "/keys/unauthorized,")
		f.writeTargets(t, f.localTargets()+remote)
		f.start(t)
		out, err := runEnv(f.env, bin, "backup")
		if err == nil {
			t.Errorf("backup succeeded despite unusable remote credentials:\n%s", out)
		}
		if !strings.Contains(strings.ToLower(out), "permission denied") {
			t.Errorf("expected SSH authentication failure, got:\n%s", out)
		}
		// Restore bytes, not just a success log, to prove partial success.
		f.restore(t, "local", "latest", "partial-success", "first", "second")
	})
	t.Run("HistoryAfterAppRemoval", func(t *testing.T) {
		f := newAcceptanceFixture(t, bin, image, id+"-history")
		f.start(t)
		mustRun(t, f.env, bin, "backup")
		ids := f.snapshots(t)
		if len(ids) != 1 {
			t.Fatalf("expected one initial snapshot, got %v", ids)
		}
		mustRun(t, nil, "docker", "rm", "-f", f.app)
		out := mustRun(t, f.env, bin, "snapshots", "--app", f.app, "--target", "local")
		if !strings.Contains(out, ids[0][:8]) {
			t.Errorf("historical snapshot %s missing after app removal:\n%s", ids[0], out)
		}
		f.restore(t, "local", ids[0], "orphaned-history", "first", "second")
	})
	t.Run("SameNameVolumeRecreation", func(t *testing.T) {
		f := newAcceptanceFixture(t, bin, image, id+"-recreate")
		f.start(t)
		mustRun(t, f.env, bin, "backup")
		old := f.snapshots(t)
		if len(old) != 1 {
			t.Fatalf("expected one initial snapshot, got %v", old)
		}
		mustRun(t, nil, "docker", "rm", "-f", f.app)
		// Completed movers are reaped at the next command's startup, not at
		// backup exit, and still hold Docker volume references in the meantime.
		// Remove only this fixture's exited movers to permit actual recreation.
		movers := strings.Fields(mustRun(t, nil, "docker", "ps", "-aq",
			"--filter", "label=acceptance.fixture="+f.app, "--filter", "status=exited"))
		if len(movers) > 0 {
			mustRun(t, nil, "docker", append([]string{"rm"}, movers...)...)
		}
		mustRun(t, nil, "docker", "volume", "rm", f.volumes[0])
		mustRun(t, nil, "docker", "volume", "create", f.volumes[0])
		f.seed(t, f.volumes[0], "replacement-generation-with-different-size")
		f.start(t)
		mustRun(t, f.env, bin, "backup")
		ids := f.snapshots(t)
		if len(ids) != 2 {
			t.Fatalf("expected original and replacement snapshots, got %v", ids)
		}
		var latest string
		for _, snapshot := range ids {
			if snapshot != old[0] {
				latest = snapshot
			}
		}
		if latest == "" {
			t.Fatal("replacement snapshot missing")
		}
		f.restore(t, "local", latest, "replacement", "replacement-generation-with-different-size", "second")
		f.restore(t, "local", old[0], "original", "first", "second")
	})
}

type acceptanceFixture struct {
	work, app, bin, image string
	volumes               []string
	env                   []string
}

func newAcceptanceFixture(t *testing.T, bin, image, app string) *acceptanceFixture {
	t.Helper()
	work, err := os.MkdirTemp("", app)
	if err != nil {
		t.Fatal(err)
	}
	f := &acceptanceFixture{work: work, app: app, bin: bin, image: image,
		volumes: []string{app + "-one", app + "-two"}}
	f.env = []string{
		"SABON_CONFIG=" + filepath.Join(work, "targets.yaml"), "SABON_MOVER_IMAGE=" + image,
		"SABON_RUNTIME=standalone", "SABON_INSTANCE=" + app, "SABON_CACHE_VOLUME=" + app + "-cache",
		"SABON_MOVER_HISTORY=0", "SABON_BACKUP_TIMEOUT=45s", "SABON_SNAPSHOT=none",
		"SABON_MOVER_LABELS=acceptance.fixture=" + app, "RESTIC_PASSWORD=" + password,
	}
	t.Cleanup(func() {
		// The history scenario deliberately removes the workload already.
		// Keep cleanup going on errors so one resource cannot strand others.
		cleanup := func(args ...string) {
			if out, err := run(t, "docker", args...); err != nil {
				t.Errorf("cleanup docker %v: %v\n%s", args, err, out)
			}
		}
		apps := strings.Fields(mustRun(t, nil, "docker", "ps", "-aq", "--filter", "name=^/"+app+"$"))
		if len(apps) > 0 {
			cleanup(append([]string{"rm", "-f"}, apps...)...)
		}
		ids := strings.Fields(mustRun(t, nil, "docker", "ps", "-aq", "--filter", "label=acceptance.fixture="+app))
		if len(ids) > 0 {
			cleanup(append([]string{"rm", "-f"}, ids...)...)
		}
		for _, v := range append(append([]string{}, f.volumes...), app+"-cache") {
			cleanup("volume", "rm", v)
		}
		cleanup("run", "--rm", "-v", work+":/w", "alpine:3", "sh", "-c", "rm -rf /w/*")
		if err := os.RemoveAll(work); err != nil {
			t.Error(err)
		}
	})
	for i, v := range f.volumes {
		mustRun(t, nil, "docker", "volume", "create", v)
		f.seed(t, v, []string{"first", "second"}[i])
	}
	if err := os.MkdirAll(filepath.Join(work, "repo"), 0o777); err != nil {
		t.Fatal(err)
	}
	f.writeTargets(t, f.localTargets())
	return f
}

func (f *acceptanceFixture) seed(t *testing.T, volume, payload string) {
	t.Helper()
	mustRun(t, nil, "docker", "run", "--rm", "-v", volume+":/data", "alpine:3", "sh", "-c",
		`printf '%s' "$1" > /data/required.txt; printf 'excluded' > /data/excluded.sentinel`, "seed", payload)
}

func (f *acceptanceFixture) start(t *testing.T) {
	t.Helper()
	spec := "repo: " + f.app + "\nauto: true\nexclude: ['**/excluded.sentinel']\n"
	mustRun(t, nil, "docker", "run", "-d", "--name", f.app,
		"--label", "sabon.enable=true", "--label", "sabon.instance="+f.app, "--label", "sabon.backup="+spec,
		"-v", f.volumes[0]+":/first", "-v", f.volumes[1]+":/second", "alpine:3", "sleep", "600")
}

func (f *acceptanceFixture) localTargets() string {
	return "targets:\n  - name: local\n    path: " + filepath.Join(f.work, "repo") + "\n"
}

func (f *acceptanceFixture) writeTargets(t *testing.T, data string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.work, "targets.yaml"), []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (f *acceptanceFixture) restore(t *testing.T, target, snapshot, directory string, payloads ...string) {
	t.Helper()
	into := filepath.Join(f.work, directory)
	mustRun(t, f.env, f.bin, "restore", "--app", f.app, "--target", target, "--snapshot", snapshot, "--into", into)
	for i, v := range f.volumes {
		base := filepath.Join(into, "data", v)
		got, err := os.ReadFile(filepath.Join(base, "required.txt"))
		if err != nil || string(got) != payloads[i] {
			t.Errorf("%s required content = %q, %v; want %q", v, got, err, payloads[i])
		}
		if _, err := os.Stat(filepath.Join(base, "excluded.sentinel")); !os.IsNotExist(err) {
			t.Errorf("%s excluded sentinel should be absent, stat error: %v", v, err)
		}
	}
}

// Read IDs independently of sabon's human-oriented listing (whose app heading
// alone does not prove that any snapshots exist).
func (f *acceptanceFixture) snapshots(t *testing.T) []string {
	t.Helper()
	out := mustRun(t, nil, "docker", "run", "--rm", "-v", filepath.Join(f.work, "repo")+":/repo:ro",
		"-e", "RESTIC_PASSWORD="+password, "--entrypoint", "restic", f.image,
		"-r", "/repo/"+f.app, "--no-lock", "snapshots", "--json")
	var snapshots []struct{ ID string }
	if err := json.Unmarshal([]byte(out), &snapshots); err != nil {
		t.Fatalf("decode snapshot IDs: %v\n%s", err, out)
	}
	ids := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if len(snapshot.ID) != 64 {
			t.Fatalf("invalid snapshot ID %q", snapshot.ID)
		}
		ids = append(ids, snapshot.ID)
	}
	return ids
}

func (f *acceptanceFixture) sftp(t *testing.T) string {
	t.Helper()
	keys := filepath.Join(f.work, "keys")
	if err := os.MkdirAll(keys, 0o777); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"client", "host"} {
		mustRun(t, nil, "docker", "run", "--rm", "-v", keys+":/k", "--entrypoint", "ssh-keygen", f.image,
			"-q", "-t", "ed25519", "-N", "", "-f", "/k/"+key)
	}
	pub, err := os.ReadFile(filepath.Join(keys, "host.pub"))
	if err != nil {
		t.Fatal(err)
	}
	known := filepath.Join(f.work, "known_hosts")
	if err := os.WriteFile(known, []byte("[sftp]:2022 "+keyField(pub)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	network, server := f.app+"-net", f.app+"-server"
	mustRun(t, nil, "docker", "network", "create", network)
	t.Cleanup(func() { mustRun(t, nil, "docker", "network", "rm", network) })
	mustRun(t, nil, "docker", "run", "-d", "--name", server, "--network", network, "--network-alias", "sftp",
		"-v", keys+":/k:ro", "rclone/rclone:1.75.2", "serve", "sftp", "/srv", "--addr", ":2022",
		"--key", "/k/host", "--authorized-keys", "/k/client.pub")
	t.Cleanup(func() { mustRun(t, nil, "docker", "rm", "-f", server) })
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := run(t, "docker", "run", "--rm", "--network", network, "--entrypoint", "nc", f.image,
			"-z", "-w", "2", "sftp", "2022"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SFTP server did not become ready")
		}
		time.Sleep(time.Second)
	}
	f.env = append(f.env, "SABON_MOVER_NETWORK="+network)
	return fmt.Sprintf(`  - name: remote
    repo: "sftp:e2e@sftp:/native/{app}"
    files:
      - {source: %s/keys/client, target: /secrets/key}
      - {source: %s, target: /secrets/known_hosts}
    resticArgs: ["-o", "sftp.args=-p 2022 -i /secrets/key -o UserKnownHostsFile=/secrets/known_hosts -o StrictHostKeyChecking=yes -o BatchMode=yes -o ConnectTimeout=5"]
`, f.work, known)
}
