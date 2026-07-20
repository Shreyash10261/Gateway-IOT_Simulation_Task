package config

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	LogLevel               string        `mapstructure:"LOG_LEVEL"`
	MetricsPort            int           `mapstructure:"METRICS_PORT"`
	HealthPort             int           `mapstructure:"HEALTH_PORT"`
	RegistryPath           string        `mapstructure:"REGISTRY_PATH"`
	NetworkTimeout         int           `mapstructure:"NETWORK_TIMEOUT_MS"`
	CertPath               string        `mapstructure:"CERT_PATH"`
	KeyPath                string        `mapstructure:"KEY_PATH"`
	IotHubHostname         string        `mapstructure:"IOT_HUB_HOSTNAME"`
	WorkerPoolSize         int           `mapstructure:"WORKER_POOL_SIZE"`
	CommandQueueSize       int           `mapstructure:"COMMAND_QUEUE_SIZE"`
	RetryMaxAttempts       int           `mapstructure:"RETRY_MAX_ATTEMPTS"`
	RetryBaseBackoffMs     int           `mapstructure:"RETRY_BASE_BACKOFF_MS"`
	PrometheusURL          string        `mapstructure:"PROMETHEUS_URL"`
	PrometheusMetrics      []string      `mapstructure:"PROMETHEUS_METRICS"`
	PrometheusPollInterval time.Duration `mapstructure:"PROMETHEUS_POLL_INTERVAL"`
	PrometheusMqttTopic    string        `mapstructure:"PROMETHEUS_MQTT_TOPIC"`
}

func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("LOG_LEVEL", "info")
	v.SetDefault("METRICS_PORT", 9090)
	v.SetDefault("HEALTH_PORT", 8080)
	v.SetDefault("REGISTRY_PATH", "/etc/gateway/devices.json")
	v.SetDefault("NETWORK_TIMEOUT_MS", 5000)
	v.SetDefault("WORKER_POOL_SIZE", 100)
	v.SetDefault("COMMAND_QUEUE_SIZE", 5000)
	v.SetDefault("RETRY_MAX_ATTEMPTS", 3)
	v.SetDefault("RETRY_BASE_BACKOFF_MS", 200)
	v.SetDefault("PROMETHEUS_URL", "http://localhost:9090")
	v.SetDefault("PROMETHEUS_METRICS", []string{"gateway_commands_dropped_total", "gateway_command_latency_seconds_count"})
	v.SetDefault("PROMETHEUS_POLL_INTERVAL", "30s")
	v.SetDefault("PROMETHEUS_MQTT_TOPIC", "devices/edge-gateway-sim/messages/events/metrics")

	v.SetEnvPrefix("GATEWAY")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("/etc/gateway/")
	v.AddConfigPath(".")
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			slog.Warn("Error reading config file", "error", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	if cfg.IotHubHostname == "" {
		return nil, fmt.Errorf("IOT_HUB_HOSTNAME is required")
	}

	return &cfg, nil
}
