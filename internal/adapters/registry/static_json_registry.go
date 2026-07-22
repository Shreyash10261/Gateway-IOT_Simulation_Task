package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"log/slog"

	"github.com/team/edge-gateway/internal/core/domain"
	coreErrors "github.com/team/edge-gateway/internal/core/errors"
)

type staticJSONRegistry struct {
	mu      sync.RWMutex
	devices map[string]*domain.Device
	path    string
}

// NewStaticJSONRegistry creates a registry that loads from a JSON file or HTTP endpoint.
func NewStaticJSONRegistry(path string) *staticJSONRegistry {
	return &staticJSONRegistry{
		devices: make(map[string]*domain.Device),
		path:    path,
	}
}

// Load populates the registry from the configured path, with validation.
func (r *staticJSONRegistry) Load(ctx context.Context) error {
	if err := r.loadOnce(); err != nil {
		return err
	}
	if strings.HasPrefix(r.path, "http://") || strings.HasPrefix(r.path, "https://") {
		go r.poll(ctx)
	}
	return nil
}

func (r *staticJSONRegistry) poll(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.loadOnce(); err != nil {
				slog.Warn("Failed to poll dynamic registry", "err", err)
			}
		}
	}
}

func (r *staticJSONRegistry) loadOnce() error {
	var body io.ReadCloser
	if strings.HasPrefix(r.path, "http://") || strings.HasPrefix(r.path, "https://") {
		resp, err := http.Get(r.path)
		if err != nil {
			return fmt.Errorf("failed to fetch registry: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return fmt.Errorf("bad status fetching registry: %d", resp.StatusCode)
		}
		body = resp.Body
	} else {
		var err error
		body, err = os.Open(r.path)
		if err != nil {
			return fmt.Errorf("failed to open registry file: %w", err)
		}
	}
	defer body.Close()

	var deviceList []*domain.Device
	if err := json.NewDecoder(body).Decode(&deviceList); err != nil {
		return fmt.Errorf("failed to decode registry JSON: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	
	newDevices := make(map[string]*domain.Device)
	for _, dev := range deviceList {
		if err := validateDevice(dev); err != nil {
			slog.Warn("Skipping invalid device", "id", dev.ID, "err", err)
			continue
		}
		
		// Preserve runtime state if device already exists
		if existing, exists := r.devices[dev.ID]; exists {
			dev.Status = existing.Status
			dev.LastSeen = existing.LastSeen
			dev.Metadata = existing.Metadata
		} else {
			dev.Status = domain.StatusUnknown
		}
		newDevices[dev.ID] = dev
	}
	
	r.devices = newDevices
	return nil
}

// GetDevice returns a defensive copy of the device.
func (r *staticJSONRegistry) GetDevice(ctx context.Context, id string) (*domain.Device, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if dev, exists := r.devices[id]; exists {
		// Return defensive copy
		copyDev := *dev
		return &copyDev, nil
	}
	return nil, coreErrors.ErrDeviceNotFound
}

// ListDevices returns defensive copies of all devices.
func (r *staticJSONRegistry) ListDevices(ctx context.Context) ([]*domain.Device, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := make([]*domain.Device, 0, len(r.devices))
	for _, dev := range r.devices {
		copyDev := *dev
		list = append(list, &copyDev)
	}
	return list, nil
}

func (r *staticJSONRegistry) Register(ctx context.Context, dev *domain.Device) error {
	if err := validateDevice(dev); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.devices[dev.ID]; exists {
		return fmt.Errorf("cannot register: device %s already exists", dev.ID)
	}
	
	// Store defensive copy
	copyDev := *dev
	r.devices[copyDev.ID] = &copyDev
	return nil
}

// Update only modifies runtime state (Status, LastSeen, Metadata). Core config is immutable.
func (r *staticJSONRegistry) Update(ctx context.Context, updatedDev *domain.Device) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	existing, exists := r.devices[updatedDev.ID]
	if !exists {
		return coreErrors.ErrDeviceNotFound
	}

	// Only apply runtime state changes
	existing.Status = updatedDev.Status
	existing.LastSeen = updatedDev.LastSeen
	if updatedDev.Metadata != nil {
		existing.Metadata = updatedDev.Metadata
	}
	
	return nil
}

func (r *staticJSONRegistry) Remove(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.devices, id)
	return nil
}

// validateDevice performs deep validation of device configuration.
func validateDevice(dev *domain.Device) error {
	if dev.ID == "" {
		return fmt.Errorf("device ID cannot be empty")
	}
	if dev.IP == "" {
		return fmt.Errorf("ip address or hostname cannot be empty")
	}
	if dev.Port <= 0 || dev.Port > 65535 {
		return fmt.Errorf("invalid port: %d", dev.Port)
	}
	switch dev.Protocol {
	case domain.ProtocolPJLink, domain.ProtocolONVIF, domain.ProtocolISAPI, domain.ProtocolShure:
		// Valid
	default:
		return coreErrors.ErrUnsupportedProtocol
	}
	return nil
}
