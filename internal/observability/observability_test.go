package observability

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestMetricsEndpoint_ExposesRegisteredCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsServiceWithRegistry(0, reg)

	svc.RecordCommandDropped()
	svc.RecordCommandLatency("PJLINK", "SUCCESS", 50*time.Millisecond)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	text := string(body)

	for _, want := range []string{
		"gateway_commands_dropped_total 1",
		"gateway_command_latency_seconds_count",
		"gateway_command_latency_seconds_bucket",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("expected /metrics to contain %q\nbody:\n%s", want, text)
		}
	}
}

func TestMetricsService_RecordMethodsUpdateCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	svc := NewMetricsServiceWithRegistry(0, reg)

	svc.RecordCommandDropped()
	svc.RecordCommandLatency("PJLINK", "ERROR", 100*time.Millisecond)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	text := string(body)

	if !strings.Contains(text, "gateway_commands_dropped_total 1") {
		t.Fatalf("expected dropped counter at 1 in exposition:\n%s", text)
	}
	if !strings.Contains(text, `gateway_command_latency_seconds_count{protocol="PJLINK",status="ERROR"}`) {
		t.Fatalf("expected latency histogram count in exposition:\n%s", text)
	}
}
