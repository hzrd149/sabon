package mover

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func executeGuardSpec(t *testing.T, spec Spec) error {
	t.Helper()
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(SpecEnv, string(raw))
	return Execute(context.Background(), io.Discard, io.Discard)
}

func TestDestinationMarkerFailsBeforeAnyResticCommand(t *testing.T) {
	for _, action := range []string{"", ActionBackup, ActionRestore, ActionSnapshots, ActionCheck, ActionPrune} {
		for _, state := range []string{"absent", "wrong", "directory", "symlink", "trailing-data"} {
			t.Run(action+"/"+state, func(t *testing.T) {
				repo := t.TempDir() // writable underlying directory after a lost mount
				t.Setenv("RESTIC_REPOSITORY", repo)
				marker := filepath.Join(repo, ".sabon-destination")
				switch state {
				case "wrong", "trailing-data":
					data := "other-destination"
					if state == "trailing-data" {
						data = "synthetic-destination\nextra"
					}
					if err := os.WriteFile(marker, []byte(data), 0600); err != nil {
						t.Fatal(err)
					}
				case "directory":
					if err := os.Mkdir(marker, 0700); err != nil {
						t.Fatal(err)
					}
				case "symlink":
					if err := os.Symlink("missing", marker); err != nil {
						t.Fatal(err)
					}
				}
				original := execRestic
				t.Cleanup(func() { execRestic = original; globalArgs = nil })
				execRestic = func(context.Context, io.Writer, ...string) error {
					t.Fatal("restic invoked before destination validation")
					return nil
				}
				if err := executeGuardSpec(t, Spec{App: "synthetic", Sources: []string{"/data"}, Action: action, RestoreTarget: "/restore", DestinationMarker: "synthetic-destination"}); err == nil {
					t.Fatal("expected destination guard failure")
				}
				if _, err := os.Stat(filepath.Join(repo, "config")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("fallback repository created: %v", err)
				}
				if state == "absent" {
					entries, err := os.ReadDir(repo)
					if err != nil || len(entries) != 0 {
						t.Fatalf("destination modified: %v %v", entries, err)
					}
				}
			})
		}
	}
}

func TestDestinationMarkerAllowsInitialization(t *testing.T) {
	for _, ending := range []string{"", "\n"} {
		t.Run("ending="+ending, func(t *testing.T) {
			repo := t.TempDir()
			t.Setenv("RESTIC_REPOSITORY", repo)
			if err := os.WriteFile(filepath.Join(repo, ".sabon-destination"), []byte("synthetic-destination"+ending), 0600); err != nil {
				t.Fatal(err)
			}
			var calls []string
			original := execRestic
			t.Cleanup(func() { execRestic = original; globalArgs = nil })
			execRestic = func(_ context.Context, _ io.Writer, args ...string) error {
				calls = append(calls, args[0])
				if args[0] == "cat" {
					return os.ErrNotExist
				}
				return nil
			}
			if err := executeGuardSpec(t, Spec{App: "synthetic", Sources: []string{"/data"}, DestinationMarker: "synthetic-destination"}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(calls, []string{"cat", "init", "unlock", "backup"}) {
				t.Fatalf("calls = %v", calls)
			}
		})
	}
}

func TestRequireExistingRepository(t *testing.T) {
	for _, repo := range []string{"/repo", "sftp:backup@example.invalid:/synthetic"} {
		for _, action := range []string{ActionBackup, ActionRestore, ActionSnapshots, ActionCheck, ActionPrune} {
			t.Run(repo+"/"+action, func(t *testing.T) {
				t.Setenv("RESTIC_REPOSITORY", repo)
				probeErr := errors.New("synthetic repository unavailable")
				original := execRestic
				t.Cleanup(func() { execRestic = original; globalArgs = nil })
				execRestic = func(_ context.Context, _ io.Writer, args ...string) error {
					if !reflect.DeepEqual(args, []string{"cat", "config"}) {
						t.Fatalf("unexpected write: %v", args)
					}
					return probeErr
				}
				if err := executeGuardSpec(t, Spec{Action: action, Sources: []string{"/data"}, RequireExistingRepository: true}); !errors.Is(err, probeErr) {
					t.Fatalf("error not propagated: %v", err)
				}
			})
		}
	}
}

func TestRequireExistingRepositorySuccessAndDefaultCompatibility(t *testing.T) {
	for _, require := range []bool{false, true} {
		t.Run(map[bool]string{false: "default-init", true: "existing"}[require], func(t *testing.T) {
			var calls []string
			original := execRestic
			t.Cleanup(func() { execRestic = original; globalArgs = nil })
			execRestic = func(_ context.Context, _ io.Writer, args ...string) error {
				calls = append(calls, args[0])
				if args[0] == "cat" && !require {
					return os.ErrNotExist
				}
				return nil
			}
			if err := executeGuardSpec(t, Spec{Sources: []string{"/data"}, RequireExistingRepository: require}); err != nil {
				t.Fatal(err)
			}
			want := []string{"cat", "init", "unlock", "backup"}
			if require {
				want = []string{"cat", "cat", "unlock", "backup"}
			}
			if !reflect.DeepEqual(calls, want) {
				t.Fatalf("calls = %v, want %v", calls, want)
			}
		})
	}
}

func TestRequireExistingRepositoryProbeFailureAfterPreflight(t *testing.T) {
	probeErr := errors.New("synthetic destination disappeared")
	original := execRestic
	t.Cleanup(func() { execRestic = original; globalArgs = nil })
	probes := 0
	execRestic = func(_ context.Context, _ io.Writer, args ...string) error {
		if !reflect.DeepEqual(args, []string{"cat", "config"}) {
			t.Fatalf("unexpected write after failed probe: %v", args)
		}
		probes++
		if probes > 1 {
			return probeErr
		}
		return nil
	}
	if err := executeGuardSpec(t, Spec{Sources: []string{"/data"}, RequireExistingRepository: true}); !errors.Is(err, probeErr) {
		t.Fatalf("error not propagated: %v", err)
	}
}
