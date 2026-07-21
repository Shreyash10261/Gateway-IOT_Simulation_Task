package ports

import (
	"context"
	"time"

	"github.com/team/edge-gateway/internal/core/domain"
)

type DeviceRegistry interface {
	GetDevice(ctx context.Context, id string) (*domain.Device, error)
	ListDevices(ctx context.Context) ([]*domain.Device, error)
	Register(ctx context.Context, dev *domain.Device) error
	Update(ctx context.Context, dev *domain.Device) error
	Remove(ctx context.Context, id string) error
}

type DeviceCommunicator interface {
	SendCommand(ctx context.Context, dev *domain.Device, payload []byte) ([]byte, error)
	ListenTelemetry(ctx context.Context, dev *domain.Device, telemetryChan chan<- *domain.DeviceTelemetry) error
}

type ProtocolAdapter interface {
	TranslateCommand(cmd domain.CloudCommand) ([]byte, error)
	ParseTelemetry(raw []byte) (map[string]interface{}, error)
}

// CommandChainAdapter optionally exposes follow-up southbound commands
// (e.g. GET_STATUS → POWR then LAMP on PJLink devices).
type CommandChainAdapter interface {
	ProtocolAdapter
	FollowUpCommands(cmd domain.CloudCommand) ([][]byte, error)
}

type AdapterFactory interface {
	GetAdapter(protocol domain.Protocol) (ProtocolAdapter, error)
}

type CloudClient interface {
	Connect() error
	Disconnect() error
	SubscribeToCommands(callback func(command domain.CloudCommand)) error
	SendTelemetry(ctx context.Context, telemetry *domain.DeviceTelemetry) error
	IsConnected() bool
	Publish(ctx context.Context, topic string, payload []byte) error
}

// MetricsService defines the observability contract for the Gateway.
type MetricsService interface {
	RecordCommandLatency(protocol domain.Protocol, status string, latency time.Duration)
	RecordCommandDropped()
}
