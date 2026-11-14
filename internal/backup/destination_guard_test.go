package backup

import (
	"testing"

	"github.com/davidborzek/sabon/api"
	"github.com/davidborzek/sabon/internal/config"
	"github.com/davidborzek/sabon/internal/discovery"
	"github.com/docker/docker/api/types/mount"
)

func TestGuardedLocalRepoUsesStrictBind(t *testing.T) {
	for _, target := range []api.Target{
		{Path: "/synthetic", DestinationMarker: "synthetic-destination"},
		{Path: "/synthetic", RequireExistingRepository: true},
	} {
		o := &Orchestrator{cfg: &config.Config{CacheVolume: "synthetic-cache"}}
		mounts, binds := o.cacheAndRepo(target, "demo")
		if len(binds) != 0 {
			t.Fatalf("guarded target uses directory-creating bind: %v", binds)
		}
		found := false
		for _, m := range mounts {
			if m.Target == repoMount {
				found = true
				if m.Type != mount.TypeBind || m.Source != "/synthetic/demo" || m.ReadOnly {
					t.Fatalf("wrong repo mount: %+v", m)
				}
			}
		}
		if !found {
			t.Fatal("repository mount missing")
		}
		spec := o.moverSpec(discovery.Job{App: "demo"}, target)
		if spec.DestinationMarker != target.DestinationMarker || spec.RequireExistingRepository != target.RequireExistingRepository {
			t.Fatalf("guard not propagated: %+v", spec)
		}
	}
}
