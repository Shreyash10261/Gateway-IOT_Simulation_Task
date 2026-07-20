package protocols

import (
	"fmt"
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

// ParseTelemetry converts a raw PJLink response back into generic metadata.
// The Router is responsible for attaching CorrelationID, Timestamp, and DeviceID.
func (p *PJLinkAdapter) ParseTelemetry(raw []byte) (map[string]interface{}, error) {
	response := strings.TrimSpace(string(raw))
	data := make(map[string]interface{})

	if strings.Contains(response, "POWR=1") {
		data["powerState"] = "ON"
	} else if strings.Contains(response, "POWR=0") {
		data["powerState"] = "OFF"
	} else {
		data["rawResponse"] = response
	}

	return data, nil
}
