package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/team/edge-gateway/internal/core/domain"
)

type mockCloudClient struct {
	publishedData []publishedMessage
	publishErr    error
}

type publishedMessage struct {
	topic   string
	payload []byte
}

func (m *mockCloudClient) Connect() error                            { return nil }
func (m *mockCloudClient) Disconnect() error                         { return nil }
func (m *mockCloudClient) SubscribeToCommands(callback func(command domain.CloudCommand)) error {
	return nil
}
func (m *mockCloudClient) SendTelemetry(ctx context.Context, telemetry *domain.DeviceTelemetry) error {
	return nil
}
func (m *mockCloudClient) IsConnected() bool { return true }
func (m *mockCloudClient) Publish(ctx context.Context, topic string, payload []byte) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.publishedData = append(m.publishedData, publishedMessage{topic: topic, payload: payload})
	return nil
}

// metricsResponse builds a Prometheus text exposition response for testing.
const counterOnlyResponse = `# HELP gateway_commands_dropped_total Total number of dropped commands due to queue saturation or failures
# TYPE gateway_commands_dropped_total counter
gateway_commands_dropped_total 42
`

const histogramResponse = `# HELP gateway_command_latency_seconds Latency of command processing in seconds
# TYPE gateway_command_latency_seconds histogram
gateway_command_latency_seconds_bucket{protocol="PJLink",status="SUCCESS",le="0.005"} 0
gateway_command_latency_seconds_bucket{protocol="PJLink",status="SUCCESS",le="0.01"} 0
gateway_command_latency_seconds_bucket{protocol="PJLink",status="SUCCESS",le="0.025"} 1
gateway_command_latency_seconds_bucket{protocol="PJLink",status="SUCCESS",le="0.05"} 2
gateway_command_latency_seconds_bucket{protocol="PJLink",status="SUCCESS",le="0.1"} 3
gateway_command_latency_seconds_bucket{protocol="PJLink",status="SUCCESS",le="+Inf"} 5
gateway_command_latency_seconds_sum{protocol="PJLink",status="SUCCESS"} 0.35
gateway_command_latency_seconds_count{protocol="PJLink",status="SUCCESS"} 5
gateway_command_latency_seconds_bucket{protocol="PJLink",status="ERROR",le="0.005"} 0
gateway_command_latency_seconds_bucket{protocol="PJLink",status="ERROR",le="0.01"} 0
gateway_command_latency_seconds_bucket{protocol="PJLink",status="ERROR",le="0.025"} 0
gateway_command_latency_seconds_bucket{protocol="PJLink",status="ERROR",le="0.05"} 1
gateway_command_latency_seconds_bucket{protocol="PJLink",status="ERROR",le="0.1"} 1
gateway_command_latency_seconds_bucket{protocol="PJLink",status="ERROR",le="+Inf"} 2
gateway_command_latency_seconds_sum{protocol="PJLink",status="ERROR"} 0.12
gateway_command_latency_seconds_count{protocol="PJLink",status="ERROR"} 2
`

const combinedResponse = counterOnlyResponse + histogramResponse

func TestPublishMetrics_CounterSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			t.Errorf("expected path /metrics, got %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(counterOnlyResponse))
	}))
	defer server.Close()

	mockClient := &mockCloudClient{}
	publisher := NewPrometheusPublisher(server.URL, []string{"gateway_commands_dropped_total"}, 1*time.Second, "test/topic", mockClient)

	publisher.publishMetrics(context.Background())

	if len(mockClient.publishedData) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(mockClient.publishedData))
	}

	msg := mockClient.publishedData[0]
	if msg.topic != "test/topic" {
		t.Errorf("expected topic test/topic, got %s", msg.topic)
	}

	var pm PublishedMetric
	if err := json.Unmarshal(msg.payload, &pm); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}

	if pm.Metric != "gateway_commands_dropped_total" {
		t.Errorf("expected metric gateway_commands_dropped_total, got %s", pm.Metric)
	}
	if pm.Value != 42.0 {
		t.Errorf("expected value 42.0, got %v", pm.Value)
	}
	if len(pm.Labels) != 0 {
		t.Errorf("expected no labels for simple counter, got %v", pm.Labels)
	}
}

func TestPublishMetrics_HistogramCountExtraction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(histogramResponse))
	}))
	defer server.Close()

	mockClient := &mockCloudClient{}
	publisher := NewPrometheusPublisher(
		server.URL,
		[]string{"gateway_command_latency_seconds_count"},
		1*time.Second,
		"test/topic",
		mockClient,
	)

	publisher.publishMetrics(context.Background())

	// Should produce 2 published messages: one for PJLink/SUCCESS (count=5), one for PJLink/ERROR (count=2)
	if len(mockClient.publishedData) != 2 {
		t.Fatalf("expected 2 published messages (one per label set), got %d", len(mockClient.publishedData))
	}

	// Verify we can decode both and check values/labels
	type result struct {
		pm PublishedMetric
	}
	var results []result
	for _, msg := range mockClient.publishedData {
		var pm PublishedMetric
		if err := json.Unmarshal(msg.payload, &pm); err != nil {
			t.Fatalf("failed to unmarshal payload: %v", err)
		}
		results = append(results, result{pm: pm})
	}

	// Find the SUCCESS and ERROR entries
	var foundSuccess, foundError bool
	for _, r := range results {
		if r.pm.Metric != "gateway_command_latency_seconds_count" {
			t.Errorf("expected metric name gateway_command_latency_seconds_count, got %s", r.pm.Metric)
		}

		if r.pm.Labels["protocol"] != "PJLink" {
			t.Errorf("expected protocol label PJLink, got %s", r.pm.Labels["protocol"])
		}

		switch r.pm.Labels["status"] {
		case "SUCCESS":
			foundSuccess = true
			if r.pm.Value != 5.0 {
				t.Errorf("expected count 5 for SUCCESS, got %v", r.pm.Value)
			}
		case "ERROR":
			foundError = true
			if r.pm.Value != 2.0 {
				t.Errorf("expected count 2 for ERROR, got %v", r.pm.Value)
			}
		default:
			t.Errorf("unexpected status label: %s", r.pm.Labels["status"])
		}
	}

	if !foundSuccess {
		t.Error("did not find SUCCESS label series")
	}
	if !foundError {
		t.Error("did not find ERROR label series")
	}
}

func TestPublishMetrics_MetricNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(counterOnlyResponse))
	}))
	defer server.Close()

	mockClient := &mockCloudClient{}
	publisher := NewPrometheusPublisher(
		server.URL,
		[]string{"nonexistent_metric_total"},
		1*time.Second,
		"test/topic",
		mockClient,
	)

	publisher.publishMetrics(context.Background())

	// Should not publish anything, and should not error (just debug log)
	if len(mockClient.publishedData) != 0 {
		t.Errorf("expected 0 published messages for missing metric, got %d", len(mockClient.publishedData))
	}
}

func TestPublishMetrics_UnreachableEndpoint(t *testing.T) {
	mockClient := &mockCloudClient{}
	publisher := NewPrometheusPublisher(
		"http://localhost:1", // unreachable port
		[]string{"gateway_commands_dropped_total"},
		1*time.Second,
		"test/topic",
		mockClient,
	)

	// Should not panic, just log the error internally
	publisher.publishMetrics(context.Background())

	if len(mockClient.publishedData) != 0 {
		t.Errorf("expected 0 published messages when endpoint unreachable, got %d", len(mockClient.publishedData))
	}
}

func TestPublishMetrics_HttpServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
	}))
	defer server.Close()

	mockClient := &mockCloudClient{}
	publisher := NewPrometheusPublisher(
		server.URL,
		[]string{"gateway_commands_dropped_total"},
		1*time.Second,
		"test/topic",
		mockClient,
	)

	publisher.publishMetrics(context.Background())

	if len(mockClient.publishedData) != 0 {
		t.Errorf("expected 0 published messages on server error, got %d", len(mockClient.publishedData))
	}
}

func TestPublishMetrics_CombinedCounterAndHistogram(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(combinedResponse))
	}))
	defer server.Close()

	mockClient := &mockCloudClient{}
	publisher := NewPrometheusPublisher(
		server.URL,
		[]string{"gateway_commands_dropped_total", "gateway_command_latency_seconds_count"},
		1*time.Second,
		"test/topic",
		mockClient,
	)

	publisher.publishMetrics(context.Background())

	// 1 counter + 2 histogram series (SUCCESS + ERROR) = 3 total
	if len(mockClient.publishedData) != 3 {
		t.Fatalf("expected 3 published messages, got %d", len(mockClient.publishedData))
	}
}

func TestPublishMetrics_StartStop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(counterOnlyResponse))
	}))
	defer server.Close()

	mockClient := &mockCloudClient{}
	publisher := NewPrometheusPublisher(
		server.URL,
		[]string{"gateway_commands_dropped_total"},
		50*time.Millisecond,
		"test/topic",
		mockClient,
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		publisher.Start(ctx)
		close(done)
	}()

	// Wait a bit to verify ticker works
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// success — publisher exited on context cancellation
	case <-time.After(1 * time.Second):
		t.Fatal("publisher Start did not terminate on context cancellation")
	}
}

func TestPublishMetrics_MalformedExpositionBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("this is not valid prometheus exposition format {{{"))
	}))
	defer server.Close()

	mockClient := &mockCloudClient{}
	publisher := NewPrometheusPublisher(
		server.URL,
		[]string{"gateway_commands_dropped_total"},
		1*time.Second,
		"test/topic",
		mockClient,
	)

	publisher.publishMetrics(context.Background())

	if len(mockClient.publishedData) != 0 {
		t.Errorf("expected 0 published messages on malformed body, got %d", len(mockClient.publishedData))
	}
}
