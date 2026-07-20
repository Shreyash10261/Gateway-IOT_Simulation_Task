package southbound

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/team/edge-gateway/internal/core/domain"
	coreErrors "github.com/team/edge-gateway/internal/core/errors"
)

type TCPCommunicator struct {
	defaultTimeout time.Duration
}

// NewTCPCommunicator initializes a TCP device communicator.
func NewTCPCommunicator(timeoutMs int) *TCPCommunicator {
	return &TCPCommunicator{
		defaultTimeout: time.Duration(timeoutMs) * time.Millisecond,
	}
}

// SendCommand dials the device over TCP, sends the payload, and waits for a response.
// Retries are explicitly NOT handled here; they belong in the Routing Engine.
func (c *TCPCommunicator) SendCommand(ctx context.Context, dev *domain.Device, payload []byte) ([]byte, error) {
	address := fmt.Sprintf("%s:%d", dev.IP, dev.Port)
	log := slog.With("device_id", dev.ID, "address", address)

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(c.defaultTimeout)
	}

	dialer := net.Dialer{Timeout: c.defaultTimeout}
	log.Debug("Dialing TCP connection")
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, categorizeNetworkError(err, "dial failed")
	}
	defer conn.Close()

	if err := conn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("failed to set socket deadline: %w", err)
	}

	log.Debug("Sending TCP payload", "bytes", len(payload))
	if _, err := conn.Write(payload); err != nil {
		return nil, categorizeNetworkError(err, "write failed")
	}

	// Use LimitReader to prevent unbounded reads from malicious/broken devices (1MB max limit)
	limitedReader := io.LimitReader(conn, 1024*1024)
	
	log.Debug("Awaiting TCP response")
	response, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, categorizeNetworkError(err, "read failed")
	}

	log.Debug("Received TCP response", "bytes", len(response))
	return response, nil
}

func (c *TCPCommunicator) ListenTelemetry(ctx context.Context, dev *domain.Device, telemetryChan chan<- *domain.DeviceTelemetry) error {
	return fmt.Errorf("streaming telemetry not supported over raw TCP in MVP")
}

// categorizeNetworkError translates low-level Go net errors into domain errors.
func categorizeNetworkError(err error, contextMsg string) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %s (context canceled)", coreErrors.ErrTimeout, contextMsg)
	}
	if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
		return fmt.Errorf("%w: %s", coreErrors.ErrTimeout, contextMsg)
	}
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: %s (unexpected EOF)", coreErrors.ErrNetwork, contextMsg)
	}
	return fmt.Errorf("%w: %s (%v)", coreErrors.ErrNetwork, contextMsg, err)
}
