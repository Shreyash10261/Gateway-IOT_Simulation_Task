package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/team/edge-gateway/internal/adapters/cloud"
	"github.com/team/edge-gateway/internal/adapters/protocols"
	"github.com/team/edge-gateway/internal/adapters/registry"
	"github.com/team/edge-gateway/internal/adapters/southbound"
	"github.com/team/edge-gateway/internal/buffer"
	"github.com/team/edge-gateway/internal/config"
	"github.com/team/edge-gateway/internal/core/domain"
	"github.com/team/edge-gateway/internal/core/router"
	"github.com/team/edge-gateway/internal/observability"
)

// Injected via -ldflags during build
var (
	Version   = "unknown"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

func main() {
	// 1. Config & Logging
	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load configuration", "err", err)
		os.Exit(1)
	}

	var logLevel slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	slog.Info("Starting Edge Gateway", 
		"version", Version, 
		"commit", GitCommit, 
		"build_time", BuildTime,
	)

	// 2. Adapters (Southbound)
	devRegistry := registry.NewStaticJSONRegistry(cfg.RegistryPath)
	
	// Initialization timeout (fail-fast if registry blocks)
	initCtx, cancelInit := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelInit()
	if err := devRegistry.Load(initCtx); err != nil {
		slog.Error("Failed to load registry", "err", err)
		os.Exit(1)
	}
	
	tcpComm := southbound.NewTCPCommunicator(cfg.NetworkTimeout)
	adapterFactory := protocols.NewAdapterFactory()
	adapterFactory.Register(domain.ProtocolPJLink, protocols.NewPJLinkAdapter())

	// 3. Adapters (Northbound)
	mqttClient := cloud.NewAzureMQTTClient(cfg.IotHubHostname, nil)

	// 3.5 Buffer Initialization
	var sqliteBuffer buffer.Buffer
	if cfg.SQLiteDBPath != "" {
		buf, err := buffer.NewSQLiteBuffer(cfg.SQLiteDBPath)
		if err != nil {
			slog.Error("Failed to initialize SQLite buffer", "err", err)
			os.Exit(1)
		}
		
		retention := time.Duration(cfg.RetentionWindowMinutes) * time.Minute
		buf.StartCleanup(context.Background(), retention)
		sqliteBuffer = buf
	}

	// 4. Observability
	metricsSvc := observability.NewMetricsService(cfg.MetricsPort)
	healthSvc := observability.NewHealthService(cfg.HealthPort, mqttClient, devRegistry, sqliteBuffer, cfg.FetchEndpointPath)
	
	go metricsSvc.Start()
	go healthSvc.Start()

	promPubCtx, cancelPromPub := context.WithCancel(context.Background())
	promPublisher := observability.NewPrometheusPublisher(
		cfg.PrometheusURL,
		cfg.PrometheusMetrics,
		cfg.PrometheusPollInterval,
		cfg.PrometheusMqttTopic,
		mqttClient,
	)
	go promPublisher.Start(promPubCtx)

	// 5. Core Routing Engine
	engine := router.NewEngine(
		router.WithRegistry(devRegistry),
		router.WithCloud(mqttClient),
		router.WithTCP(tcpComm),
		router.WithAdapters(adapterFactory),
		router.WithMetrics(metricsSvc),
		router.WithBuffer(sqliteBuffer),
		router.WithTimeout(time.Duration(cfg.NetworkTimeout)*time.Millisecond),
		router.WithWorkerPoolSize(cfg.WorkerPoolSize),
		router.WithCommandQueueSize(cfg.CommandQueueSize),
		router.WithRetryPolicy(router.RetryPolicy{
			MaxAttempts: cfg.RetryMaxAttempts,
			BaseBackoff: time.Duration(cfg.RetryBaseBackoffMs) * time.Millisecond,
		}),
	)

	// 6. Start Engine FIRST (so workers are ready for incoming cloud messages)
	engineCtx, cancelEngine := context.WithCancel(context.Background())
	engine.Start(engineCtx)

	// Start Telemetry Pollers for PJLink devices
	telemetryChan := make(chan *domain.DeviceTelemetry, 100)
	
	// Background worker to buffer periodic telemetry
	go func() {
		for {
			select {
			case <-engineCtx.Done():
				return
			case t := <-telemetryChan:
				if sqliteBuffer != nil {
					if err := sqliteBuffer.AddData("telemetry", t); err != nil {
						slog.Error("Failed to buffer periodic telemetry", "err", err)
					}
				}
			}
		}
	}()

	// 6.5 Dynamic Telemetry Poller Spawner
	go func() {
		activePollers := make(map[string]bool)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-engineCtx.Done():
				return
			case <-ticker.C:
				devs, err := devRegistry.ListDevices(engineCtx)
				if err != nil {
					slog.Error("Failed to list devices from registry in poller loop", "err", err)
					continue
				}
				for _, dev := range devs {
					if dev.Protocol == domain.ProtocolPJLink {
						if !activePollers[dev.ID] {
							activePollers[dev.ID] = true
							slog.Info("Spawning new telemetry poller for device", "device", dev.ID, "protocol", dev.Protocol)
							go func(d *domain.Device) {
								_ = tcpComm.ListenTelemetry(engineCtx, d, telemetryChan)
							}(dev)
						}
					} else {
						// Log unhandled protocols so we can debug why it skipped them
						if !activePollers[dev.ID] {
							slog.Info("Discovered device in registry but protocol is not PJLink", "device", dev.ID, "protocol", dev.Protocol)
							activePollers[dev.ID] = true // Mark as seen so we don't spam the logs
						}
					}
				}
			}
		}
	}()

	// 7. Connect to Cloud and Bind Topics (Background reconnect loop handles failures)
	if err := mqttClient.Connect(); err != nil {
		slog.Warn("Failed to connect to Azure IoT on startup; background reconnect active", "err", err)
	}
	_ = mqttClient.SubscribeToCommands(engine.DispatchCommand)

	// 8. Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	slog.Info("Shutdown signal received", "signal", sig)

	healthSvc.Shutdown() // Mark Unready instantly
	cancelPromPub()      // Stop Prometheus Metrics Publisher
	cancelEngine()       // Signal workers to drain
	engine.Stop()        // Wait for workers to finish
	
	if sqliteBuffer != nil {
		sqliteBuffer.Close()
	}
	
	// Gracefully flush pending MQTT acks/telemetry before shutting down TCP socket
	_ = mqttClient.Disconnect()
	
	slog.Info("Edge Gateway shut down cleanly")
}
