package southbound

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/team/edge-gateway/internal/adapters/protocols"
	"github.com/team/edge-gateway/internal/core/domain"
	coreErrors "github.com/team/edge-gateway/internal/core/errors"
)

const maxPJLinkFrameBytes = 1024

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

	if dev.Protocol == domain.ProtocolPJLink {
		return c.sendPJLinkCommand(conn, payload, log)
	}

	log.Debug("Sending TCP payload", "bytes", len(payload))
	if _, err := conn.Write(payload); err != nil {
		return nil, categorizeNetworkError(err, "write failed")
	}

	limitedReader := io.LimitReader(conn, 1024*1024)

	log.Debug("Awaiting TCP response")
	response, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, categorizeNetworkError(err, "read failed")
	}

	log.Debug("Received TCP response", "bytes", len(response))
	return response, nil
}

func (c *TCPCommunicator) sendPJLinkCommand(conn net.Conn, payload []byte, log *slog.Logger) ([]byte, error) {
	greeting, err := readUntilCR(conn, maxPJLinkFrameBytes)
	if err != nil {
		return nil, categorizeNetworkError(err, "PJLink greeting read failed")
	}

	greetingText := strings.TrimSpace(string(greeting))
	if !strings.HasPrefix(greetingText, "PJLINK") {
		return nil, fmt.Errorf("%w: unexpected PJLink greeting %q", coreErrors.ErrNetwork, greetingText)
	}
	log.Debug("Received PJLink greeting", "greeting", greetingText)

	log.Debug("Sending PJLink payload", "bytes", len(payload))
	if _, err := conn.Write(payload); err != nil {
		return nil, categorizeNetworkError(err, "write failed")
	}

	response, err := readUntilCR(conn, maxPJLinkFrameBytes)
	if err != nil {
		return nil, categorizeNetworkError(err, "PJLink response read failed")
	}

	log.Debug("Received PJLink response", "response", strings.TrimSpace(string(response)))
	return response, nil
}

func readUntilCR(r io.Reader, maxBytes int) ([]byte, error) {
	buf := make([]byte, 0, 64)
	one := make([]byte, 1)

	for len(buf) < maxBytes {
		n, err := r.Read(one)
		if n > 0 {
			buf = append(buf, one[0])
			if one[0] == '\r' {
				return buf, nil
			}
		}
		if err != nil {
			if len(buf) > 0 {
				return buf, categorizeNetworkError(err, "incomplete frame read")
			}
			return nil, categorizeNetworkError(err, "read failed")
		}
	}

	return nil, fmt.Errorf("%w: PJLink frame exceeded %d bytes", coreErrors.ErrNetwork, maxBytes)
}

func (c *TCPCommunicator) ListenTelemetry(ctx context.Context, dev *domain.Device, telemetryChan chan<- *domain.DeviceTelemetry) error {
	if dev.Protocol == domain.ProtocolPJLink {
		return c.pollPJLinkTelemetry(ctx, dev)
	}
	return fmt.Errorf("streaming telemetry not supported over raw TCP in MVP")
}

func (c *TCPCommunicator) pollPJLinkTelemetry(ctx context.Context, dev *domain.Device) error {
	address := fmt.Sprintf("%s:%d", dev.IP, dev.Port)
	log := slog.With("device", dev.ID, "address", address)
	adapter := protocols.NewPJLinkAdapter()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		dialer := net.Dialer{Timeout: c.defaultTimeout}
		conn, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			log.Warn("Failed to dial PJLink device for telemetry, retrying in 5s", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
			continue
		}

		err = c.runPJLinkSession(ctx, conn, log, adapter, ticker)
		conn.Close()

		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("PJLink telemetry session ended with error, reconnecting", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (c *TCPCommunicator) runPJLinkSession(ctx context.Context, conn net.Conn, log *slog.Logger, adapter *protocols.PJLinkAdapter, ticker *time.Ticker) error {
	if err := conn.SetReadDeadline(time.Now().Add(c.defaultTimeout)); err != nil {
		return err
	}
	greeting, err := readUntilCR(conn, maxPJLinkFrameBytes)
	if err != nil {
		return fmt.Errorf("failed to read greeting: %w", err)
	}
	greetingText := strings.TrimSpace(string(greeting))
	if !strings.HasPrefix(greetingText, "PJLINK") {
		return fmt.Errorf("unexpected greeting: %q", greetingText)
	}
	log.Debug("Received PJLink greeting in poller", "greeting", greetingText)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := conn.SetDeadline(time.Now().Add(c.defaultTimeout)); err != nil {
				return err
			}
			if _, err := conn.Write([]byte("%1LAMP ?\r")); err != nil {
				return fmt.Errorf("failed to send LAMP query: %w", err)
			}
			lampResp, err := readUntilCR(conn, maxPJLinkFrameBytes)
			if err != nil {
				return fmt.Errorf("failed to read LAMP response: %w", err)
			}
			
			if err := conn.SetDeadline(time.Now().Add(c.defaultTimeout)); err != nil {
				return err
			}
			if _, err := conn.Write([]byte("%1POWR ?\r")); err != nil {
				return fmt.Errorf("failed to send POWR query: %w", err)
			}
			powrResp, err := readUntilCR(conn, maxPJLinkFrameBytes)
			if err != nil {
				return fmt.Errorf("failed to read POWR response: %w", err)
			}

			parsedLamp, errLamp := adapter.ParseTelemetry(lampResp)
			parsedPowr, errPowr := adapter.ParseTelemetry(powrResp)
			
			logArgs := []any{}
			hasErr := false
			
			if errLamp == nil {
				if errCode, ok := parsedLamp["error"]; ok {
					logArgs = append(logArgs, "lamp_error", errCode)
					hasErr = true
				} else {
					if v, ok := parsedLamp["lamp_hours"]; ok {
						logArgs = append(logArgs, "lamp_hours", v)
					}
					if v, ok := parsedLamp["lamp_status"]; ok {
						logArgs = append(logArgs, "lamp_status", v)
					}
				}
			}
			
			if errPowr == nil {
				if errCode, ok := parsedPowr["error"]; ok {
					logArgs = append(logArgs, "power_error", errCode)
					hasErr = true
				} else if state, ok := parsedPowr["powerState"]; ok {
					logArgs = append(logArgs, "power_status", state)
				}
			}
			
			if hasErr {
				log.Warn("Received device telemetry with errors", logArgs...)
			} else {
				log.Info("Received device telemetry", logArgs...)
			}
		}
	}
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
