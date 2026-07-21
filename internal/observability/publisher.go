package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/team/edge-gateway/internal/core/ports"
	"github.com/team/edge-gateway/internal/core/rules"
)

// PublishedMetric is the JSON payload published to the IoT Hub for each metric series.
type PublishedMetric struct {
	Metric    string            `json:"metric"`
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"timestamp"`
}

// metricEvaluator evaluates scraped metric samples (e.g. local alerting rules).
// Production uses *rules.Engine; tests may inject a no-op to avoid alert side effects.
type metricEvaluator interface {
	Evaluate(ctx context.Context, metricName string, value float64, labels map[string]string)
}

// nopMetricEvaluator is a no-op metricEvaluator for publisher-only tests.
type nopMetricEvaluator struct{}

func (nopMetricEvaluator) Evaluate(context.Context, string, float64, map[string]string) {}

// PrometheusPublisher scrapes the local Prometheus /metrics endpoint and
// publishes the configured metrics to the IoT Hub over MQTT.
type PrometheusPublisher struct {
	prometheusURL string
	metrics       []string
	interval      time.Duration
	topic         string
	cloudClient   ports.CloudClient
	httpClient    *http.Client
	rulesEngine   metricEvaluator
	logger        *slog.Logger
}

func NewPrometheusPublisher(
	prometheusURL string,
	metrics []string,
	interval time.Duration,
	topic string,
	cloudClient ports.CloudClient,
) *PrometheusPublisher {
	alertTopic := "devices/edge-gateway-sim/messages/events/alerts" // Can be pulled from config in the future
	return newPrometheusPublisher(
		prometheusURL,
		metrics,
		interval,
		topic,
		cloudClient,
		rules.NewEngine(cloudClient, alertTopic),
	)
}

func newPrometheusPublisher(
	prometheusURL string,
	metrics []string,
	interval time.Duration,
	topic string,
	cloudClient ports.CloudClient,
	evaluator metricEvaluator,
) *PrometheusPublisher {
	if evaluator == nil {
		evaluator = nopMetricEvaluator{}
	}
	return &PrometheusPublisher{
		prometheusURL: prometheusURL,
		metrics:       metrics,
		interval:      interval,
		topic:         topic,
		cloudClient:   cloudClient,
		httpClient:    &http.Client{Timeout: 10 * time.Second},
		rulesEngine:   evaluator,
		logger:        slog.Default().With("component", "prometheus_publisher"),
	}
}

func (p *PrometheusPublisher) Start(ctx context.Context) {
	p.logger.Info("Starting Prometheus Metrics Publisher",
		"url", p.prometheusURL,
		"metrics", p.metrics,
		"interval", p.interval,
		"topic", p.topic,
	)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	p.waitForCloud(ctx)
	p.publishMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Prometheus Metrics Publisher shutting down")
			return
		case <-ticker.C:
			p.logger.Info("publisher tick fired")
			p.publishMetrics(ctx)
		}
	}
}

// waitForCloud blocks until the cloud client reports connected or ctx is cancelled.
// The publisher goroutine starts before main calls Connect(), so the initial publish
// must wait rather than fail with "not connected".
func (p *PrometheusPublisher) waitForCloud(ctx context.Context) {
	if p.cloudClient.IsConnected() {
		return
	}

	p.logger.Debug("Waiting for cloud client connection before first metrics publish")

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.cloudClient.IsConnected() {
				return
			}
		}
	}
}

func (p *PrometheusPublisher) publishMetrics(ctx context.Context) {
	var found, notFound, failed, publishedOK int
	var familiesCount int
	defer func() {
		p.logger.Info("publishMetrics completed",
			"configured", len(p.metrics),
			"found", found,
			"not_found", notFound,
			"failed", failed,
			"published_ok", publishedOK,
			"families_scraped", familiesCount,
		)
	}()

	select {
	case <-ctx.Done():
		return
	default:
	}

	families, err := p.scrapeMetrics(ctx)
	if err != nil {
		p.logger.Error("Failed to scrape metrics endpoint", "err", err)
		return
	}
	familiesCount = len(families)

	if !p.cloudClient.IsConnected() {
		p.logger.Debug("Skipping cloud publish cycle: MQTT not connected")
		return
	}

	now := time.Now()

	for _, metricName := range p.metrics {
		select {
		case <-ctx.Done():
			return
		default:
		}

		published, err := p.extractAndPublish(ctx, families, metricName, now)
		if err != nil {
			failed++
			p.logger.Error("Failed to publish metric", "metric", metricName, "err", err)
			continue
		}
		if !published {
			notFound++
			p.logger.Debug("Metric not found in scrape output (may not be recorded yet)", "metric", metricName)
		} else {
			found++
			publishedOK++
		}
	}
}

// scrapeMetrics fetches and parses the /metrics endpoint into MetricFamily map.
func (p *PrometheusPublisher) scrapeMetrics(ctx context.Context) (map[string]*dto.MetricFamily, error) {
	metricsURL := fmt.Sprintf("%s/metrics", p.prometheusURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics endpoint returned status %d", resp.StatusCode)
	}

	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse prometheus exposition format: %w", err)
	}

	return families, nil
}

// extractAndPublish finds the requested metric in the parsed families and publishes
// each series to the cloud. Returns true if any series were found, false if the
// metric was not present in the scrape output.
func (p *PrometheusPublisher) extractAndPublish(
	ctx context.Context,
	families map[string]*dto.MetricFamily,
	metricName string,
	now time.Time,
) (bool, error) {
	// Case 1: Direct match — metric name is a registered family name (counter, gauge, etc.)
	if family, ok := families[metricName]; ok {
		return p.publishFamily(ctx, family, metricName, now)
	}

	// Case 2: Derived histogram/summary field — e.g. "foo_count" → family "foo" type HISTOGRAM
	if baseName, suffix, ok := splitDerivedSuffix(metricName); ok {
		if family, ok := families[baseName]; ok {
			return p.publishDerivedField(ctx, family, metricName, suffix, now)
		}
	}

	// Not found at all
	return false, nil
}

// splitDerivedSuffix checks whether metricName ends with a known histogram/summary
// derived suffix and returns the base family name and suffix.
func splitDerivedSuffix(metricName string) (baseName string, suffix string, ok bool) {
	// Order matters: check _bucket before _count to avoid false matches
	for _, s := range []string{"_bucket", "_count", "_sum", "_total"} {
		if strings.HasSuffix(metricName, s) {
			base := strings.TrimSuffix(metricName, s)
			if base != "" {
				return base, s, true
			}
		}
	}
	return "", "", false
}

// publishFamily publishes all series from a directly-matched metric family.
func (p *PrometheusPublisher) publishFamily(
	ctx context.Context,
	family *dto.MetricFamily,
	metricName string,
	now time.Time,
) (bool, error) {
	if len(family.GetMetric()) == 0 {
		return false, nil
	}

	for _, m := range family.GetMetric() {
		value, ok := extractDirectValue(family.GetType(), m)
		if !ok {
			continue
		}

		labels := labelPairsToMap(m.GetLabel())

		payload := PublishedMetric{
			Metric:    metricName,
			Value:     value,
			Labels:    labels,
			Timestamp: now,
		}

		// Evaluate metric against local rules engine
		p.rulesEngine.Evaluate(ctx, metricName, value, labels)

		if err := p.publishPayload(ctx, payload); err != nil {
			return true, err
		}

		p.logger.Debug("Published metric to cloud", "metric", metricName, "topic", p.topic)
	}

	return true, nil
}

// publishDerivedField publishes a derived field (_count, _sum, _bucket) from
// a histogram or summary family.
func (p *PrometheusPublisher) publishDerivedField(
	ctx context.Context,
	family *dto.MetricFamily,
	metricName string,
	suffix string,
	now time.Time,
) (bool, error) {
	fType := family.GetType()

	if len(family.GetMetric()) == 0 {
		return false, nil
	}

	for _, m := range family.GetMetric() {
		value, ok := extractDerivedValue(fType, suffix, m)
		if !ok {
			continue
		}

		labels := labelPairsToMap(m.GetLabel())

		payload := PublishedMetric{
			Metric:    metricName,
			Value:     value,
			Labels:    labels,
			Timestamp: now,
		}

		// Evaluate metric against local rules engine
		p.rulesEngine.Evaluate(ctx, metricName, value, labels)

		if err := p.publishPayload(ctx, payload); err != nil {
			return true, err
		}

		p.logger.Debug("Published metric to cloud", "metric", metricName, "topic", p.topic)
	}

	return true, nil
}

func (p *PrometheusPublisher) publishPayload(ctx context.Context, payload PublishedMetric) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal published metric JSON: %w", err)
	}

	if err := p.cloudClient.Publish(ctx, p.topic, payloadBytes); err != nil {
		return fmt.Errorf("failed to publish to cloud client: %w", err)
	}

	return nil
}

// extractDirectValue extracts the scalar value from a metric based on its family type.
func extractDirectValue(fType dto.MetricType, m *dto.Metric) (float64, bool) {
	switch fType {
	case dto.MetricType_COUNTER:
		if c := m.GetCounter(); c != nil {
			return c.GetValue(), true
		}
	case dto.MetricType_GAUGE:
		if g := m.GetGauge(); g != nil {
			return g.GetValue(), true
		}
	case dto.MetricType_UNTYPED:
		if u := m.GetUntyped(); u != nil {
			return u.GetValue(), true
		}
	// For histogram/summary accessed by their base name, return the sample count.
	case dto.MetricType_HISTOGRAM:
		if h := m.GetHistogram(); h != nil {
			return float64(h.GetSampleCount()), true
		}
	case dto.MetricType_SUMMARY:
		if s := m.GetSummary(); s != nil {
			return float64(s.GetSampleCount()), true
		}
	}
	return 0, false
}

// extractDerivedValue extracts a derived field (_count, _sum) from a histogram or summary.
func extractDerivedValue(fType dto.MetricType, suffix string, m *dto.Metric) (float64, bool) {
	switch suffix {
	case "_count":
		switch fType {
		case dto.MetricType_HISTOGRAM:
			if h := m.GetHistogram(); h != nil {
				return float64(h.GetSampleCount()), true
			}
		case dto.MetricType_SUMMARY:
			if s := m.GetSummary(); s != nil {
				return float64(s.GetSampleCount()), true
			}
		}
	case "_sum":
		switch fType {
		case dto.MetricType_HISTOGRAM:
			if h := m.GetHistogram(); h != nil {
				return h.GetSampleSum(), true
			}
		case dto.MetricType_SUMMARY:
			if s := m.GetSummary(); s != nil {
				return s.GetSampleSum(), true
			}
		}
	case "_total":
		// _total is a Counter naming convention; the family may be named with or without it
		if fType == dto.MetricType_COUNTER {
			if c := m.GetCounter(); c != nil {
				return c.GetValue(), true
			}
		}
	}
	return 0, false
}

// labelPairsToMap converts proto LabelPair slice to a plain map.
func labelPairsToMap(pairs []*dto.LabelPair) map[string]string {
	labels := make(map[string]string, len(pairs))
	for _, lp := range pairs {
		labels[lp.GetName()] = lp.GetValue()
	}
	return labels
}
