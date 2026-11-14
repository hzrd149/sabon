package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func metricValue(t *testing.T, metric prometheus.Metric) float64 {
	t.Helper()
	var value dto.Metric
	if err := metric.Write(&value); err != nil {
		t.Fatal(err)
	}
	if value.Gauge != nil {
		return value.Gauge.GetValue()
	}
	return value.Counter.GetValue()
}

func TestFailedLifecycleDoesNotAdvanceSuccess(t *testing.T) {
	m := New("test")
	m.ObserveBackup("synthetic", "one", true, time.Second, 1, 10)
	before := metricValue(t, m.lastSuccess.WithLabelValues("synthetic", "one"))
	// A snapshot's statistics may be returned alongside a recovery error.
	m.ObserveBackup("synthetic", "one", false, time.Second, 99, 999)
	if got := metricValue(t, m.backups.WithLabelValues("synthetic", "one", "error")); got != 1 {
		t.Fatalf("failure count=%v", got)
	}
	if got := metricValue(t, m.backups.WithLabelValues("synthetic", "one", "success")); got != 1 {
		t.Fatalf("success count=%v", got)
	}
	if got := metricValue(t, m.lastSuccess.WithLabelValues("synthetic", "one")); got != before {
		t.Fatal("failed recovery advanced success time")
	}
	if got := metricValue(t, m.filesNew.WithLabelValues("synthetic", "one")); got != 1 {
		t.Fatal("failed lifecycle replaced successful run statistics")
	}
}
