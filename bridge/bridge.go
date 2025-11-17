package bridge

import (
	"fmt"
	"slices"
)

type Bridge interface {
	Start() error
	Close() error
	fmt.Stringer
}

func NewBridgeManager() *BridgeManager {
	return &BridgeManager{
		bridges: []Bridge{},
	}
}

type BridgeManager struct {
	bridges []Bridge
}

// Start starts new goroutines for all bridges an initializes them
func (b *BridgeManager) Start() error {
	started := []Bridge{}

	// Close all started bridges in the case of an error
	defer func() {
		if !slices.EqualFunc(b.bridges, started, func(b1, b2 Bridge) bool {
			return b1.String() == b2.String()
		}) {
			for _, bridge := range b.bridges {
				bridge.Close()
			}
		}
	}()

	for _, bridge := range b.bridges {
		if err := bridge.Start(); err != nil {
			return fmt.Errorf("couldn't start bridge: %v", err)
		}
		started = append(started, bridge)
	}

	return nil
}
