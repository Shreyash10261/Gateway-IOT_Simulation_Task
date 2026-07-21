package protocols

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/team/edge-gateway/internal/core/domain"
	coreErrors "github.com/team/edge-gateway/internal/core/errors"
)

type PJLinkAdapter struct{}

func NewPJLinkAdapter() *PJLinkAdapter {
	return &PJLinkAdapter{}
}

// TranslateCommand converts an Azure JSON command into a raw PJLink string.
func (p *PJLinkAdapter) TranslateCommand(cmd domain.CloudCommand) ([]byte, error) {
	var pjCmd string
	switch cmd.CommandName {
	case domain.CmdTurnOn:
		pjCmd = "%1POWR 1\r"
	case domain.CmdTurnOff:
		pjCmd = "%1POWR 0\r"
	case domain.CmdGetStatus:
		pjCmd = "%1POWR ?\r"
	default:
		return nil, fmt.Errorf("%w: %s", coreErrors.ErrUnsupportedProtocol, cmd.CommandName)
	}

	return []byte(pjCmd), nil
}

// FollowUpCommands returns additional PJLink payloads to run after the primary command.
func (p *PJLinkAdapter) FollowUpCommands(cmd domain.CloudCommand) ([][]byte, error) {
	if cmd.CommandName == domain.CmdGetStatus {
		return [][]byte{[]byte("%1LAMP ?\r")}, nil
	}
	return nil, nil
}

// ParseTelemetry converts a raw PJLink response back into generic metadata.
// The Router is responsible for attaching CorrelationID, Timestamp, and DeviceID.
func (p *PJLinkAdapter) ParseTelemetry(raw []byte) (map[string]interface{}, error) {
	response := strings.TrimSpace(string(raw))
	data := make(map[string]interface{})

	if errCode, ok := parsePJLinkError(response); ok {
		data["error"] = errCode
		return data, nil
	}

	if strings.Contains(response, "POWR=") {
		parsePOWRResponse(response, data)
	}

	if strings.Contains(response, "LAMP=") {
		parseLAMPResponse(response, data)
	}

	if len(data) == 0 {
		data["rawResponse"] = response
	}

	return data, nil
}

func parsePJLinkError(response string) (string, bool) {
	switch {
	case strings.Contains(response, "ERR1"):
		return "ERR1_UNDEFINED_COMMAND", true
	case strings.Contains(response, "ERR2"):
		return "ERR2_OUT_OF_PARAMETER", true
	case strings.Contains(response, "ERR3"):
		return "ERR3_UNAVAILABLE_TIME", true
	case strings.Contains(response, "ERR4"):
		return "ERR4_PROJECTOR_FAILURE", true
	default:
		return "", false
	}
}

func parsePOWRResponse(response string, data map[string]interface{}) {
	idx := strings.Index(response, "POWR=")
	if idx < 0 {
		return
	}

	value := strings.TrimSpace(response[idx+5:])
	if value == "OK" {
		data["powerResult"] = "OK"
		return
	}

	switch value {
	case "0":
		data["powerState"] = "STANDBY"
	case "1":
		data["powerState"] = "ON"
	case "2":
		data["powerState"] = "COOLING"
	case "3":
		data["powerState"] = "WARMING"
	default:
		data["powerState"] = value
	}
}

func parseLAMPResponse(response string, data map[string]interface{}) {
	idx := strings.Index(response, "LAMP=")
	if idx < 0 {
		return
	}

	payload := strings.TrimSpace(response[idx+5:])
	parts := strings.Fields(payload)
	if len(parts) == 0 {
		return
	}

	if hours, err := strconv.Atoi(parts[0]); err == nil {
		data["lamp_hours"] = hours
	}

	if len(parts) > 1 {
		if parts[1] == "1" {
			data["lamp_status"] = "ON"
		} else {
			data["lamp_status"] = "OFF"
		}
	}
}
