package observability

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/team/edge-gateway/internal/buffer"
	"github.com/team/edge-gateway/internal/core/domain"
	"github.com/team/edge-gateway/internal/core/ports"
)

// MetricsService implements ports.MetricsService for Prometheus.
// Collectors are owned by the service instance and registered on the provided
// registry (or the Prometheus default registry when constructed via NewMetricsService).
type MetricsService struct {
	port            int
	gatherer        prometheus.Gatherer
	commandLatency  *prometheus.HistogramVec
	commandsDropped prometheus.Counter
}

// NewMetricsService creates a MetricsService that registers collectors on
// prometheus.DefaultRegisterer and serves them via prometheus.DefaultGatherer.
func NewMetricsService(port int) *MetricsService {
	return newMetricsService(port, prometheus.DefaultRegisterer, prometheus.DefaultGatherer)
}

// NewMetricsServiceWithRegistry creates a MetricsService bound to an isolated
// registry. Prefer this in tests so collectors do not leak across cases or -count runs.
func NewMetricsServiceWithRegistry(port int, reg *prometheus.Registry) *MetricsService {
	return newMetricsService(port, reg, reg)
}

func newMetricsService(port int, registerer prometheus.Registerer, gatherer prometheus.Gatherer) *MetricsService {
	commandLatency := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_command_latency_seconds",
			Help:    "Latency of command processing in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"protocol", "status"},
	)
	commandsDropped := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gateway_commands_dropped_total",
			Help: "Total number of dropped commands due to queue saturation or failures",
		},
	)

	registerer.MustRegister(commandLatency, commandsDropped)

	return &MetricsService{
		port:            port,
		gatherer:        gatherer,
		commandLatency:  commandLatency,
		commandsDropped: commandsDropped,
	}
}

func (m *MetricsService) RecordCommandLatency(protocol domain.Protocol, status string, latency time.Duration) {
	m.commandLatency.WithLabelValues(string(protocol), status).Observe(latency.Seconds())
	slog.Debug("Metric recorded: command latency", "protocol", protocol, "status", status, "latency", latency)
}

func (m *MetricsService) RecordCommandDropped() {
	m.commandsDropped.Inc()
	slog.Debug("Metric recorded: command dropped")
}

func (m *MetricsService) Start() {
	slog.Info("Starting Prometheus Metrics Server", "port", m.port)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.gatherer, promhttp.HandlerOpts{}))

	if err := http.ListenAndServe(fmt.Sprintf(":%d", m.port), mux); err != nil {
		slog.Error("Metrics server crashed", "err", err)
	}
}

// HealthService exposes Kubernetes-compatible liveness and readiness endpoints.
type HealthService struct {
	port          int
	cloudClient   ports.CloudClient
	registry      ports.DeviceRegistry
	buffer        buffer.Buffer
	fetchEndpoint string
	mu            sync.RWMutex
	shuttingDown  bool
}

func NewHealthService(port int, cloud ports.CloudClient, registry ports.DeviceRegistry, buf buffer.Buffer, fetchEndpoint string) *HealthService {
	return &HealthService{
		port:          port,
		cloudClient:   cloud,
		registry:      registry,
		buffer:        buf,
		fetchEndpoint: fetchEndpoint,
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

	if h.fetchEndpoint != "" {
		mux.HandleFunc(h.fetchEndpoint, h.handleFetch)
	}

	slog.Info("Starting Health Server", "port", h.port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", h.port), mux); err != nil {
		slog.Error("Health server crashed", "err", err)
	}
}

func (h *HealthService) handleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var items []buffer.BufferedItem
	var err error
	
	if h.buffer != nil {
		items, err = h.buffer.Fetch()
		if err != nil {
			slog.Error("Failed to fetch buffer", "err", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	} else {
		items = []buffer.BufferedItem{}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(items); err != nil {
		slog.Error("Failed to encode fetch response", "err", err)
	} else {
		slog.Info("Served fetch request", "records", len(items))
	}
}

func (h *HealthService) Shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.shuttingDown = true
}
