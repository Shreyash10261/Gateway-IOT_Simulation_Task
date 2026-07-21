package protocols

import (
	"testing"

	"github.com/team/edge-gateway/internal/core/domain"
)

func TestPJLinkAdapter_TranslateCommand(t *testing.T) {
	adapter := NewPJLinkAdapter()

	tests := []struct {
		name    domain.CommandType
		want    string
	}{
		{domain.CmdTurnOn, "%1POWR 1\r"},
		{domain.CmdTurnOff, "%1POWR 0\r"},
		{domain.CmdGetStatus, "%1POWR ?\r"},
	}

	for _, tt := range tests {
		t.Run(string(tt.name), func(t *testing.T) {
			got, err := adapter.TranslateCommand(domain.CloudCommand{CommandName: tt.name})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(got) != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPJLinkAdapter_FollowUpCommands(t *testing.T) {
	adapter := NewPJLinkAdapter()

	followUps, err := adapter.FollowUpCommands(domain.CloudCommand{CommandName: domain.CmdGetStatus})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(followUps) != 1 || string(followUps[0]) != "%1LAMP ?\r" {
		t.Fatalf("unexpected follow-up commands: %#v", followUps)
	}

	none, err := adapter.FollowUpCommands(domain.CloudCommand{CommandName: domain.CmdTurnOn})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("expected no follow-ups for TURN_ON, got %#v", none)
	}
}

func TestPJLinkAdapter_ParseTelemetry(t *testing.T) {
	adapter := NewPJLinkAdapter()

	tests := []struct {
		name     string
		raw      string
		wantKeys map[string]interface{}
	}{
		{
			name:     "power on",
			raw:      "%1POWR=1\r",
			wantKeys: map[string]interface{}{"powerState": "ON"},
		},
		{
			name:     "power standby",
			raw:      "%1POWR=0\r",
			wantKeys: map[string]interface{}{"powerState": "STANDBY"},
		},
		{
			name:     "power ok",
			raw:      "%1POWR=OK\r",
			wantKeys: map[string]interface{}{"powerResult": "OK"},
		},
		{
			name:     "lamp data",
			raw:      "%1LAMP=105 1\r",
			wantKeys: map[string]interface{}{"lamp_hours": 105, "lamp_status": "ON"},
		},
		{
			name:     "pjlink error",
			raw:      "%1POWR=ERR3\r",
			wantKeys: map[string]interface{}{"error": "ERR3_UNAVAILABLE_TIME"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := adapter.ParseTelemetry([]byte(tt.raw))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for key, want := range tt.wantKeys {
				if got[key] != want {
					t.Fatalf("key %q = %v, want %v", key, got[key], want)
				}
			}
		})
	}
}
