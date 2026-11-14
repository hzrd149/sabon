package api

import "testing"

func TestGuardedResticArgs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		valid bool
	}{
		{"short-repo", []string{"-r", "/other"}, false},
		{"short-repo-attached", []string{"-r/other"}, false},
		{"short-repo-equals", []string{"-r=/other"}, false},
		{"cluster-repo", []string{"-qr", "/other"}, false},
		{"cluster-repo-attached", []string{"-qr/other"}, false},
		{"cluster-repo-equals", []string{"-qr=/other"}, false},
		{"longer-cluster", []string{"-vqr/other"}, false},
		{"long-repo", []string{"--repo", "/other"}, false},
		{"long-repo-equals", []string{"--repo=/other"}, false},
		{"repository-file", []string{"--repository-file", "/other"}, false},
		{"repository-file-equals", []string{"--repository-file=/other"}, false},
		{"unknown-short", []string{"-x"}, false},
		{"benign-cluster-needs-splitting", []string{"-vv"}, false},
		{"attached-option-needs-splitting", []string{"-os3.connections=2"}, false},
		{"attached-password-needs-splitting", []string{"-p/password"}, false},
		{"short-quiet", []string{"-q"}, true},
		{"short-verbose-repeated", []string{"-v", "-v"}, true},
		{"short-help", []string{"-h"}, true},
		{"short-option", []string{"-o", "s3.connections=2"}, true},
		{"short-password", []string{"-p", "/password"}, true},
		{"long-options", []string{"--quiet", "--option=s3.connections=2", "--password-file=/password", "--retry-lock=1m"}, true},
		{"long-verbose", []string{"--verbose=2"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"marker", "existing", "unguarded"} {
				t.Run(mode, func(t *testing.T) {
					target := Target{Name: "synthetic", Path: "/backup", ResticArgs: tc.args}
					target.RequireExistingRepository = mode == "existing"
					if mode == "marker" {
						target.DestinationMarker = "synthetic"
					}
					f := File{Targets: []Target{target}}
					valid := tc.valid || mode == "unguarded"
					if err := f.Validate(); (err == nil) != valid {
						t.Fatalf("Validate(%q) = %v, valid = %v", tc.args, err, valid)
					}
				})
			}
		})
	}
}

func TestDestinationGuardValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target Target
		valid  bool
	}{
		{"default", Target{Path: "/backup"}, true},
		{"local-marker", Target{Path: "/backup", DestinationMarker: "synthetic"}, true},
		{"remote-marker", Target{Repo: "sftp:example.invalid:/backup", DestinationMarker: "synthetic"}, false},
		{"sftp-existing", Target{Repo: "sftp:example.invalid:/backup", RequireExistingRepository: true}, true},
		{"whitespace", Target{Path: "/backup", DestinationMarker: " "}, false},
		{"override-env", Target{Path: "/backup", DestinationMarker: "synthetic", Env: map[string]string{"RESTIC_REPOSITORY": "/other"}}, false},
		{"override-args", Target{Path: "/backup", DestinationMarker: "synthetic", ResticArgs: []string{"--repo=/other"}}, false},
		{"override-short", Target{Path: "/backup", RequireExistingRepository: true, ResticArgs: []string{"-r/other"}}, false},
		{"normal-args", Target{Path: "/backup", DestinationMarker: "synthetic", ResticArgs: []string{"--limit-upload=1024"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.target.Name = "synthetic"
			f := File{Targets: []Target{tc.target}}
			if err := f.Validate(); (err == nil) != tc.valid {
				t.Fatalf("Validate() = %v, valid = %v", err, tc.valid)
			}
		})
	}
}
