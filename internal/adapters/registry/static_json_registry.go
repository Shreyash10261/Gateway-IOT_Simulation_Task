package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/team/edge-gateway/internal/core/domain"
	coreErrors "github.com/team/edge-gateway/internal/core/errors"
)

type staticJSONRegistry struct {
	mu      sync.RWMutex
	devices map[string]*domain.Device
	path    string
}

// NewStaticJSONRegistry creates a registry that loads from a JSON file.
func NewStaticJSONRegistry(path string) *staticJSONRegistry {
	return &staticJSONRegistry{
		devices: make(map[string]*domain.Device),
		path:    path,
	}
}

// Load populates the registry from the configured JSON file path, with validation.
func (r *staticJSONRegistry) Load(ctx context.Context) error {
	file, err := os.Open(r.path)
	if err != nil {
		return fmt.Errorf("failed to open registry file: %w", err)
	}
	defer file.Close()

	var deviceList []*domain.Device
	if err := json.NewDecoder(file).Decode(&deviceList); err != nil {
		return fmt.Errorf("failed to decode registry JSON: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, dev := range deviceList {
		if err := validateDevice(dev); err != nil {
			return fmt.Errorf("invalid device %s: %w", dev.ID, err)
		}
		if _, exists := r.devices[dev.ID]; exists {
			return fmt.Errorf("duplicate device ID found during load: %s", dev.ID)
		}

		// Initialize runtime state (LastSeen remains zero-value/unset until contact)
		dev.Status = domain.StatusUnknown
		r.devices[dev.ID] = dev
	}
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
