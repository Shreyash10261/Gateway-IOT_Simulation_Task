package rules

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/team/edge-gateway/internal/core/domain"
	"github.com/team/edge-gateway/internal/core/ports"
)

type Rule struct {
	MetricName string
	Operator   string // ">", "<", "=="
	Threshold  float64
	Severity   string // "CRITICAL", "WARNING"
}

type Engine struct {
	rules       []Rule
	cloudClient ports.CloudClient
	alertTopic  string
	logger      *slog.Logger
}

func NewEngine(cloudClient ports.CloudClient, alertTopic string) *Engine {
	// Define local rules. In a production system, these might be loaded from a config file.
	rules := []Rule{
		{
			MetricName: "gateway_commands_dropped_total",
			Operator:   ">",
			Threshold:  5.0,
			Severity:   "CRITICAL",
		},
		{
			MetricName: "gateway_command_latency_seconds_count",
			Operator:   ">",
			Threshold:  1000.0, // Arbitrary high volume alert
			Severity:   "WARNING",
		},
	}

	return &Engine{
		rules:       rules,
		cloudClient: cloudClient,
		alertTopic:  alertTopic,
		logger:      slog.Default().With("component", "rules_engine"),
	}
}

func (e *Engine) Evaluate(ctx context.Context, metricName string, value float64, labels map[string]string) {
	for _, rule := range e.rules {
		if rule.MetricName == metricName {
			if e.checkThreshold(value, rule.Threshold, rule.Operator) {
				e.logger.Warn("Rule threshold breached!", "metric", metricName, "value", value, "threshold", rule.Threshold)
				e.triggerAlert(ctx, rule, metricName, value, labels)
			}
		}
	}
}

func (e *Engine) checkThreshold(value, threshold float64, operator string) bool {
	switch operator {
	case ">":
		return value > threshold
	case "<":
		return value < threshold
	case "==":
		return value == threshold
	default:
		return false
	}
}

func (e *Engine) triggerAlert(ctx context.Context, rule Rule, metricName string, value float64, labels map[string]string) {
	if !e.cloudClient.IsConnected() {
		e.logger.Error("Cannot send alert: cloud client disconnected", "metric", metricName)
		return
	}

	alert := domain.AlertPayload{
		Severity:   rule.Severity,
		Message:    fmt.Sprintf("Metric %s breached threshold %v %s %v", metricName, value, rule.Operator, rule.Threshold),
		MetricName: metricName,
		Value:      value,
		Threshold:  rule.Threshold,
		Labels:     labels,
		Timestamp:  time.Now(),
	}

	payloadBytes, err := json.Marshal(alert)
	if err != nil {
		e.logger.Error("Failed to marshal alert payload", "err", err)
		return
	}

	if err := e.cloudClient.Publish(ctx, e.alertTopic, payloadBytes); err != nil {
		e.logger.Error("Failed to publish alert to cloud", "err", err)
		return
	}

	e.logger.Info("Successfully published CRITICAL alert to cloud", "topic", e.alertTopic)
}
