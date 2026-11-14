package api

import "testing"

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
