package protocols

import (
	"fmt"
	"sync"

	"github.com/team/edge-gateway/internal/core/domain"
	"github.com/team/edge-gateway/internal/core/ports"
)

type factory struct {
	mu       sync.RWMutex
	adapters map[domain.Protocol]ports.ProtocolAdapter
}

func NewAdapterFactory() *factory {
	return &factory{
		adapters: make(map[domain.Protocol]ports.ProtocolAdapter),
	}
}

func (f *factory) Register(protocol domain.Protocol, adapter ports.ProtocolAdapter) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adapters[protocol] = adapter
}

func (f *factory) GetAdapter(protocol domain.Protocol) (ports.ProtocolAdapter, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if adapter, exists := f.adapters[protocol]; exists {
		return adapter, nil
	}
	return nil, fmt.Errorf("no adapter registered for protocol: %s", protocol)
}
