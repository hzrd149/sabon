package reconcile

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/davidborzek/sabon/api"
	"github.com/davidborzek/sabon/internal/backup"
	"github.com/davidborzek/sabon/internal/config"
	"github.com/davidborzek/sabon/internal/discovery"
	"github.com/davidborzek/sabon/internal/engine"
	"github.com/davidborzek/sabon/internal/metrics"
	"github.com/davidborzek/sabon/internal/mover"
)

type recoveryRuntime struct {
	engine.Engine
	engine.Host
}

type recoveryHooks struct {
	engine.Hooks
	failure error
}

func (r recoveryRuntime) RunMover(context.Context, mover.Request) (mover.Result, bool, error) {
	return mover.Result{SnapshotID: "synthetic-snapshot"}, false, nil
}
func (r recoveryRuntime) ListRuns(context.Context, string, string) ([]engine.RunInfo, error) {
	return nil, nil
}
func (r recoveryHooks) Exec(context.Context, string, []string, []string, string) error {
	return r.failure
}
func (r recoveryRuntime) EnsureCache(context.Context, string) error { return nil }

func TestRunOnceReportsLifecycleFailureWithSnapshot(t *testing.T) {
	t.Setenv("SABON_TEST_LIFECYCLE_PASSWORD", "synthetic")
	runtime := recoveryRuntime{}
	hooks := recoveryHooks{failure: errors.New("synthetic cleanup failure")}
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	cfg := &config.Config{}
	orch := backup.New(cfg, "synthetic", runtime, hooks, nil, runtime, nil, log)
	r := New(context.Background(), nil, orch, cfg, nil, metrics.New("test"), nil, log)
	job := discovery.Job{App: "synthetic-app", Spec: api.Spec{PostHooks: []api.Hook{{Command: []string{"cleanup"}}}}}
	err := r.runOnce(context.Background(), job, api.Target{Name: "one", Path: "/synthetic", PasswordEnv: "SABON_TEST_LIFECYCLE_PASSWORD"})
	if !errors.Is(err, hooks.failure) {
		t.Fatalf("reconciler lost recovery failure: %v", err)
	}
	if got := logs.String(); !strings.Contains(got, `"msg":"backup lifecycle failed"`) || !strings.Contains(got, `"snapshot":"synthetic-snapshot"`) || strings.Contains(got, `"msg":"backup complete"`) {
		t.Fatalf("inaccurate final reporting: %s", got)
	}
}
