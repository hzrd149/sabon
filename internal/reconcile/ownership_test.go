package reconcile

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/davidborzek/sabon/api"
	"github.com/davidborzek/sabon/internal/backup"
	"github.com/davidborzek/sabon/internal/config"
	"github.com/davidborzek/sabon/internal/discovery"
	"github.com/davidborzek/sabon/internal/engine"
	dockerengine "github.com/davidborzek/sabon/internal/engine/docker"
	"github.com/davidborzek/sabon/internal/metrics"
	"github.com/davidborzek/sabon/internal/mover"
	"github.com/davidborzek/sabon/internal/scheduler"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type ownershipDiscoverer struct {
	engine.Discoverer
	jobs []discovery.Job
}

type ownershipDocker struct {
	client.APIClient
	containers []container.Summary
}

func (d *ownershipDocker) ContainerList(context.Context, container.ListOptions) ([]container.Summary, error) {
	return d.containers, nil
}

type ownershipMover struct {
	engine.Engine
	engine.Host
	runs chan mover.Request
}

func (e *ownershipMover) EnsureCache(context.Context, string) error { return nil }
func (e *ownershipMover) ListRuns(context.Context, string, string) ([]engine.RunInfo, error) {
	return nil, nil
}
func (e *ownershipMover) RunMover(_ context.Context, req mover.Request) (mover.Result, bool, error) {
	select {
	case e.runs <- req:
	default:
	}
	return mover.Result{}, false, nil
}

// Exercise discovery -> reconciliation -> real cron callback -> orchestrator.
// Only Docker and the mover are fakes; no host resources are created or touched.
func TestComposeProjectsScheduledCoverage(t *testing.T) {
	t.Setenv("SABON_TEST_OWNERSHIP_PASSWORD", "synthetic-password")
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fd := &ownershipDocker{}
	for _, project := range []string{"alpha", "beta"} {
		fd.containers = append(fd.containers, container.Summary{Names: []string{"/" + project + "-db-1"}, Labels: map[string]string{
			"sabon.enable": "true", "sabon.backup": "auto: false\nextraPaths: [/synthetic/" + project + "]",
			discovery.ComposeProjectLabel: project, discovery.ComposeServiceLabel: "db",
		}})
	}
	d := dockerengine.NewDiscoverer(fd, "sabon", false, "synthetic-cache", "", log)
	cfg := &config.Config{CacheVolume: "synthetic-cache", Targets: []api.Target{{Name: "local", Path: "/synthetic/repos", PasswordEnv: "SABON_TEST_OWNERSHIP_PASSWORD", Schedule: "* * * * * *"}}}
	e := &ownershipMover{runs: make(chan mover.Request, 16)}
	o := backup.New(cfg, "synthetic-mover", e, nil, nil, e, nil, log)
	s := scheduler.New(log)
	r := New(ctx, d, o, cfg, s, metrics.New("test"), nil, log)
	if err := r.Reconcile(); err != nil {
		t.Fatal(err)
	}
	s.Start()
	defer s.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case req := <-e.runs:
			project := strings.TrimSuffix(req.Spec.App, "-db")
			if project != "alpha" && project != "beta" {
				t.Fatalf("unexpected app %q", req.Spec.App)
			}
			found := false
			for _, m := range req.Mounts {
				if m.Source == "/synthetic/"+project {
					found = true
				}
			}
			if !found {
				t.Fatalf("%s lost its source: %+v", req.Spec.App, req.Mounts)
			}
			seen[project] = true
		case <-deadline.C:
			t.Fatalf("scheduled coverage missing: %v", seen)
		}
	}
	// A later conflict returned by the concrete discoverer must remove the
	// already registered callbacks, not leave their old source views active.
	for i := range fd.containers {
		fd.containers[i].Labels["sabon.backup"] += "\nrepo: shared"
	}
	if err := r.Reconcile(); err == nil || s.Len() != 0 {
		t.Fatalf("conflict from discovery: schedules=%d err=%v", s.Len(), err)
	}
	if err := r.RunAll(ctx, ""); err == nil {
		t.Fatal("one-shot accepted conflict from discovery")
	}
}

func (d *ownershipDiscoverer) List(context.Context) ([]discovery.Job, error) { return d.jobs, nil }

func TestOwnershipReconcileAndOneShot(t *testing.T) {
	for _, named := range []string{"", "data"} {
		t.Run("backup="+named, func(t *testing.T) {
			ctx := context.Background()
			log := slog.New(slog.NewTextHandler(io.Discard, nil))
			d := &ownershipDiscoverer{jobs: []discovery.Job{{App: "alpha-db", Container: "alpha-db-1"}, {App: "beta-db", Container: "beta-db-1", Backup: named}}}
			s := scheduler.New(log)
			cfg := &config.Config{Targets: []api.Target{{Name: "local", Schedule: "0 0 * * * *", Check: "0 0 1 * * *", Prune: "0 0 2 * * *"}}}
			r := New(ctx, d, nil, cfg, s, metrics.New("test"), nil, log)
			if err := r.Reconcile(); err != nil || s.Len() != 6 {
				t.Fatalf("distinct owners: schedules=%d err=%v", s.Len(), err)
			}
			d.jobs[1].App = "alpha-db"
			if err := r.Reconcile(); err == nil || !strings.Contains(err.Error(), "alpha-db") {
				t.Fatalf("want ownership error, got %v", err)
			}
			if s.Len() != 0 {
				t.Fatalf("stale schedules still active: %d", s.Len())
			}
			// A nil orchestrator ensures rejection happens before any execution.
			if err := r.RunAll(ctx, ""); err == nil {
				t.Fatal("RunAll accepted conflicting owners")
			}
			if err := r.RunMatching(ctx, "alpha-db", "local"); err == nil {
				t.Fatal("filtered run accepted conflicting owners")
			}
			if _, _, err := r.resolve(ctx, "alpha-db", "local"); err == nil {
				t.Fatal("API picked an arbitrary owner")
			}
			// Even disjoint target selections cannot make an app-only lookup
			// (including in-place restore) safe.
			d.jobs[0].Spec.Targets = []api.TargetSpec{{Name: "local"}}
			d.jobs[1].Spec.Targets = []api.TargetSpec{{Name: "offsite"}}
			if _, _, err := r.resolve(ctx, "alpha-db", ""); err == nil {
				t.Fatal("API accepted ambiguous app on disjoint targets")
			}
			d.jobs = d.jobs[:1]
			if err := r.Reconcile(); err != nil || s.Len() != 3 {
				t.Fatalf("recovery: schedules=%d err=%v", s.Len(), err)
			}
		})
	}
}
