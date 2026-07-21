package router

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/team/edge-gateway/internal/buffer"
	"github.com/team/edge-gateway/internal/core/domain"
	coreErrors "github.com/team/edge-gateway/internal/core/errors"
	"github.com/team/edge-gateway/internal/core/ports"
)

// OverflowPolicy dictates how the queue behaves when full.
type OverflowPolicy func(chan domain.CloudCommand, domain.CloudCommand) error

func DropNewestPolicy(ch chan domain.CloudCommand, cmd domain.CloudCommand) error {
	select {
	case ch <- cmd:
		return nil
	default:
		return coreErrors.ErrQueueFull
	}
}

// RetryPolicy defines backoff logic.
type RetryPolicy struct {
	MaxAttempts int
	BaseBackoff time.Duration
}

type Engine struct {
	registry       ports.DeviceRegistry
	cloud          ports.CloudClient
	tcpComm        ports.DeviceCommunicator
	adapters       ports.AdapterFactory
	metrics        ports.MetricsService
	buffer         buffer.Buffer
	
	cmdChan        chan domain.CloudCommand
	workerPool     int
	wg             sync.WaitGroup
	
	overflowPolicy OverflowPolicy
	retryPolicy    RetryPolicy
	networkTimeout time.Duration
}

type Option func(*Engine)

func WithRegistry(r ports.DeviceRegistry) Option { return func(e *Engine) { e.registry = r } }
func WithCloud(c ports.CloudClient) Option       { return func(e *Engine) { e.cloud = c } }
func WithTCP(t ports.DeviceCommunicator) Option  { return func(e *Engine) { e.tcpComm = t } }
func WithAdapters(a ports.AdapterFactory) Option { return func(e *Engine) { e.adapters = a } }
func WithMetrics(m ports.MetricsService) Option  { return func(e *Engine) { e.metrics = m } }
func WithBuffer(b buffer.Buffer) Option          { return func(e *Engine) { e.buffer = b } }
func WithTimeout(d time.Duration) Option         { return func(e *Engine) { e.networkTimeout = d } }

func WithWorkerPoolSize(s int) Option            { return func(e *Engine) { e.workerPool = s } }
func WithCommandQueueSize(s int) Option          { return func(e *Engine) { e.cmdChan = make(chan domain.CloudCommand, s) } }
func WithRetryPolicy(p RetryPolicy) Option       { return func(e *Engine) { e.retryPolicy = p } }

func NewEngine(opts ...Option) *Engine {
	e := &Engine{
		// Default sizes will be overridden if WithCommandQueueSize is passed
		cmdChan:        make(chan domain.CloudCommand, 5000),
		workerPool:     100,
		overflowPolicy: DropNewestPolicy,
		retryPolicy:    RetryPolicy{MaxAttempts: 3, BaseBackoff: 200 * time.Millisecond},
		networkTimeout: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func (e *Engine) Start(ctx context.Context) {
	slog.Info("Starting Routing Engine", "workers", e.workerPool)
	for i := 0; i < e.workerPool; i++ {
		e.wg.Add(1)
		go e.worker(ctx, i)
	}
}

func (e *Engine) DispatchCommand(cmd domain.CloudCommand) {
	if err := e.overflowPolicy(e.cmdChan, cmd); err != nil {
		slog.Error("Command queue full, dropping", "correlation_id", cmd.CorrelationID)
		if e.metrics != nil {
			e.metrics.RecordCommandDropped()
		}
	}
}

func (e *Engine) worker(ctx context.Context, id int) {
	defer e.wg.Done()
	log := slog.With("worker_id", id)

	for {
		select {
		case <-ctx.Done():
			log.Info("Worker shutting down")
			return
		case cmd, ok := <-e.cmdChan:
			if !ok {
				log.Info("Command channel closed, shutting down")
				return
			}
			e.processCommand(ctx, cmd, log)
		}
	}
}

func (e *Engine) processCommand(ctx context.Context, cmd domain.CloudCommand, baseLog *slog.Logger) {
	start := time.Now()
	log := baseLog.With("device_id", cmd.DeviceID, "correlation_id", cmd.CorrelationID)

	dev, err := e.registry.GetDevice(ctx, cmd.DeviceID)
	if err != nil {
		log.Error("Device not found")
		return
	}
	
	log = log.With("protocol", dev.Protocol)

	adapter, err := e.adapters.GetAdapter(dev.Protocol)
	if err != nil {
		log.Error("Unsupported protocol")
		return
	}

	rawPayload, err := adapter.TranslateCommand(cmd)
	if err != nil {
		log.Error("Translation failed", "err", err)
		return
	}

	rawTelemetry, chainErr := e.executeCommandChain(ctx, dev, adapter, cmd, rawPayload, log)

	// Record metrics and update state regardless of success/failure
	latency := time.Since(start)
	status := "SUCCESS"

	if chainErr != nil {
		status = "ERROR"
		log.Error("Southbound dispatch failed permanently", "err", chainErr)

		// Update registry state to Offline
		dev.Status = domain.StatusOffline
		dev.LastSeen = time.Now()
		_ = e.registry.Update(ctx, dev)
	} else {
		// Update registry state to Online
		dev.Status = domain.StatusOnline
		dev.LastSeen = time.Now()
		_ = e.registry.Update(ctx, dev)
	}

	if e.metrics != nil {
		e.metrics.RecordCommandLatency(dev.Protocol, status, latency)
	}

	if chainErr != nil {
		return // Do not send telemetry if network failed completely
	}

	telemetry := &domain.DeviceTelemetry{
		CorrelationID: cmd.CorrelationID,
		DeviceID:      dev.ID,
		Timestamp:     time.Now(),
		Data:          rawTelemetry,
	}
	
	if e.buffer != nil {
		if err := e.buffer.AddData("telemetry", telemetry); err != nil {
			log.Error("Failed to buffer telemetry", "err", err)
		}
	}
	
	e.publishTelemetryWithRetry(ctx, telemetry, log)
}

func (e *Engine) executeCommandChain(
	ctx context.Context,
	dev *domain.Device,
	adapter ports.ProtocolAdapter,
	cmd domain.CloudCommand,
	primaryPayload []byte,
	log *slog.Logger,
) (map[string]interface{}, error) {
	primaryResp, err := e.executeWithRetry(ctx, dev, primaryPayload, log)
	if err != nil {
		return nil, err
	}

	telemetry, err := adapter.ParseTelemetry(primaryResp)
	if err != nil {
		return nil, err
	}

	chainAdapter, ok := adapter.(ports.CommandChainAdapter)
	if !ok {
		return telemetry, nil
	}

	followUps, err := chainAdapter.FollowUpCommands(cmd)
	if err != nil {
		return nil, err
	}

	for _, payload := range followUps {
		followUpResp, err := e.executeWithRetry(ctx, dev, payload, log)
		if err != nil {
			return nil, err
		}

		extra, err := adapter.ParseTelemetry(followUpResp)
		if err != nil {
			return nil, err
		}
		mergeTelemetry(telemetry, extra)
	}

	return telemetry, nil
}

func mergeTelemetry(base, extra map[string]interface{}) {
	for key, value := range extra {
		base[key] = value
	}
}

func (e *Engine) executeWithRetry(ctx context.Context, dev *domain.Device, payload []byte, log *slog.Logger) ([]byte, error) {
	var lastErr error
	backoff := e.retryPolicy.BaseBackoff

	for attempt := 1; attempt <= e.retryPolicy.MaxAttempts; attempt++ {
		reqCtx, cancel := context.WithTimeout(ctx, e.networkTimeout)
		respPayload, err := e.tcpComm.SendCommand(reqCtx, dev, payload)
		cancel()

		if err == nil {
			return respPayload, nil // Success
		}

		lastErr = err
		if coreErrors.Classify(err) != coreErrors.ClassRetryable {
			log.Warn("Non-retryable network error", "attempt", attempt, "err", err)
			return nil, err // Fast fail
		}

		log.Warn("Network error, retrying", "attempt", attempt, "err", err)
		
		if attempt < e.retryPolicy.MaxAttempts {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				backoff *= 2 // Exponential backoff
			}
		}
	}
	return nil, lastErr
}

func (e *Engine) publishTelemetryWithRetry(ctx context.Context, t *domain.DeviceTelemetry, log *slog.Logger) {
	backoff := e.retryPolicy.BaseBackoff

	for attempt := 1; attempt <= e.retryPolicy.MaxAttempts; attempt++ {
		err := e.cloud.SendTelemetry(ctx, t)
		if err == nil {
			return // Success
		}

		log.Warn("Northbound telemetry publish failed, retrying", "attempt", attempt, "err", err)
		
		if attempt < e.retryPolicy.MaxAttempts {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff *= 2
			}
		}
	}
	log.Error("Failed to publish telemetry after max retries, dropping telemetry")
}

func (e *Engine) Stop() {
	close(e.cmdChan)
	e.wg.Wait()
	slog.Info("Routing Engine stopped")
}
