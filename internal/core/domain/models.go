package domain

import "time"

type Status string
const (
	StatusOnline      Status = "ONLINE"
	StatusOffline     Status = "OFFLINE"
	StatusDegraded    Status = "DEGRADED"
	StatusError       Status = "ERROR"
	StatusMaintenance Status = "MAINTENANCE"
	StatusUnknown     Status = "UNKNOWN"
)

type Protocol string
const (
	ProtocolPJLink Protocol = "PJLink"
	ProtocolONVIF  Protocol = "ONVIF"
	ProtocolISAPI  Protocol = "ISAPI"
	ProtocolShure  Protocol = "ShureAPI"
)

// CommandType provides strongly typed commands to prevent magic strings.
type CommandType string
const (
	CmdTurnOn    CommandType = "TURN_ON"
	CmdTurnOff   CommandType = "TURN_OFF"
	CmdGetStatus CommandType = "GET_STATUS"
	CmdReboot    CommandType = "REBOOT"
)

type Device struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Model        string            `json:"model"`
	Vendor       string            `json:"vendor"`
	Room         string            `json:"room"`
	MACAddress   string            `json:"mac_address"`
	IP           string            `json:"ip"`
	Port         int               `json:"port"`
	Protocol     Protocol          `json:"protocol"` 
	Capabilities []string          `json:"capabilities"` 
	Metadata     map[string]string `json:"metadata"`
	Status       Status            `json:"status"`
	LastSeen     time.Time         `json:"last_seen"`
}

type CloudCommand struct {
	CorrelationID string      `json:"correlation_id"`
	DeviceID      string      `json:"device_id"`
	CommandName   CommandType `json:"command_name"`
	Payload       []byte      `json:"payload,omitempty"`
	IssuedAt      time.Time   `json:"issued_at"`
}

type DeviceTelemetry struct {
	CorrelationID string                 `json:"correlation_id"`
	DeviceID      string                 `json:"device_id"`
	Timestamp     time.Time              `json:"timestamp"`
	Data          map[string]interface{} `json:"data"`
}
