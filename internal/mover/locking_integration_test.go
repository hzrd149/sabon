package mover

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This uses only temporary local repositories, no Docker or configured targets.
// Put restic on PATH to exercise its real lock protocol as well as our unit tests.
func TestExecuteWithRealResticLock(t *testing.T) {
	bin, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not on PATH")
	}
	for _, orphan := range []bool{false, true} {
		name := "active-independent-client"
		if orphan {
			name = "dead-owner-same-hostname"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			dir := t.TempDir()
			repo := filepath.Join(dir, "repo")
			env := []string{"RESTIC_REPOSITORY=" + repo, "RESTIC_PASSWORD=synthetic-test-password", "RESTIC_CACHE_DIR=" + filepath.Join(dir, "cache")}
			command := func(args ...string) *exec.Cmd {
				cmd := exec.CommandContext(ctx, bin, args...)
				cmd.Env = env
				return cmd
			}
			if out, err := command("init").CombinedOutput(); err != nil {
				t.Fatalf("init: %v\n%s", err, out)
			}
			// Keep a real backup running by holding its stdin open. Its lock is
			// owned by an independent process, not by any orchestrator mutex.
			owner := command("backup", "--stdin", "--stdin-filename", "fixture")
			var ownerOutput bytes.Buffer
			owner.Stdout, owner.Stderr = &ownerOutput, &ownerOutput
			stdin, err := owner.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := owner.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			defer func() {
				_ = stdin.Close()
				_ = owner.Process.Kill()
				if !waited {
					_ = owner.Wait()
				}
			}()
			if _, err := io.WriteString(stdin, "synthetic backup fixture\n"); err != nil {
				t.Fatal(err)
			}
			var lockPath string
			for lockPath == "" {
				entries, err := os.ReadDir(filepath.Join(repo, "locks"))
				// The local backend first writes a temporary file, then renames
				// it to the 64-character lock ID. Wait for the published lock.
				if err == nil && len(entries) == 1 && len(entries[0].Name()) == 64 {
					lockPath = filepath.Join(repo, "locks", entries[0].Name())
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("backup did not acquire a lock")
				case <-time.After(20 * time.Millisecond):
				}
			}
			lock, err := os.ReadFile(lockPath)
			if err != nil {
				t.Fatal(err)
			}
			if orphan {
				_ = owner.Process.Kill()
				_ = owner.Wait()
				waited = true
			}
			orig := execRestic
			t.Cleanup(func() { execRestic = orig; globalArgs = nil })
			execRestic = func(ctx context.Context, w io.Writer, args ...string) error {
				cmd := exec.CommandContext(ctx, bin, args...)
				cmd.Env = env
				cmd.Stdout, cmd.Stderr = w, w
				return cmd.Run()
			}
			for _, action := range []string{ActionCheck, ActionPrune, ActionSnapshots} {
				raw, err := MarshalSpec(Spec{App: "fixture", Action: action})
				if err != nil {
					t.Fatal(err)
				}
				t.Setenv(SpecEnv, raw)
				var output bytes.Buffer
				err = Execute(ctx, &output, &output)
				if action == ActionSnapshots {
					if err != nil {
						t.Fatalf("compatible snapshots: %v\n%s", err, output.String())
					}
				} else if err == nil || !strings.Contains(output.String(), "repository is already locked") {
					t.Fatalf("%s must reject conflicting lock: %v\n%s", action, err, output.String())
				}
				got, err := os.ReadFile(lockPath)
				if err != nil || !bytes.Equal(got, lock) {
					t.Fatalf("%s changed the independent client's lock: %v", action, err)
				}
			}
			if orphan {
				// Explicit recovery is safe in this isolated fixture: we killed
				// and waited for its only owner, in the same PID namespace.
				if out, err := command("unlock").CombinedOutput(); err != nil {
					t.Fatalf("manual stale-lock cleanup: %v\n%s", err, out)
				}
			} else {
				_ = stdin.Close()
				err := owner.Wait()
				waited = true
				if err != nil {
					t.Fatalf("independent backup: %v\n%s", err, ownerOutput.String())
				}
			}
			raw, err := MarshalSpec(Spec{App: "fixture", Action: ActionPrune})
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv(SpecEnv, raw)
			var output bytes.Buffer
			if err := Execute(ctx, &output, &output); err != nil {
				t.Fatalf("prune after lock release: %v\n%s", err, output.String())
			}
		})
	}
}
