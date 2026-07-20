package observability

import (
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/team/edge-gateway/internal/core/domain"
	"github.com/team/edge-gateway/internal/core/ports"
)

var (
	commandLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_command_latency_seconds",
			Help:    "Latency of command processing in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"protocol", "status"},
	)

	commandsDropped = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gateway_commands_dropped_total",
			Help: "Total number of dropped commands due to queue saturation or failures",
		},
	)
)

// MetricsService implements ports.MetricsService for Prometheus.
type MetricsService struct {
	port int
}

func NewMetricsService(port int) *MetricsService {
	return &MetricsService{port: port}
}

func (m *MetricsService) RecordCommandLatency(protocol domain.Protocol, status string, latency time.Duration) {
	commandLatency.WithLabelValues(string(protocol), status).Observe(latency.Seconds())
	slog.Debug("Metric recorded: command latency", "protocol", protocol, "status", status, "latency", latency)
}

func (m *MetricsService) RecordCommandDropped() {
	commandsDropped.Inc()
	slog.Debug("Metric recorded: command dropped")
}

func (m *MetricsService) Start() {
	slog.Info("Starting Prometheus Metrics Server", "port", m.port)
	
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	
	if err := http.ListenAndServe(fmt.Sprintf(":%d", m.port), mux); err != nil {
		slog.Error("Metrics server crashed", "err", err)
	}
}

// HealthService exposes Kubernetes-compatible liveness and readiness endpoints.
type HealthService struct {
	port        int
	cloudClient ports.CloudClient
	registry    ports.DeviceRegistry
	mu          sync.RWMutex
	shuttingDown bool
}

func NewHealthService(port int, cloud ports.CloudClient, registry ports.DeviceRegistry) *HealthService {
	return &HealthService{
		port:        port,
		cloudClient: cloud,
		registry:    registry,
	}
}

func (h *HealthService) Start() {
	mux := http.NewServeMux()
	
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.shuttingDown {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Shutting Down"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		if !h.cloudClient.IsConnected() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Not connected to Azure IoT Hub"))
			return
		}
		// Also verify registry is loaded (e.g., len(devices) > 0)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ready"))
	})

	slog.Info("Starting Health Server", "port", h.port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", h.port), mux); err != nil {
		slog.Error("Health server crashed", "err", err)
	}
}

func (h *HealthService) Shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.shuttingDown = true
}
