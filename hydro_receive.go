package hydro

import (
	"fmt"

	"github.com/Liphium/neogate"
	"github.com/bytedance/sonic"
)

// Handle receiving of a local event using Hydro. Will also let all adapters receive in Neo.
func (i *Instance[T]) Receive(adapters []HydroAdapter, event neogate.Event) error {
	// 1. Call some kind of filters to make sure the event is allowed to be received (drop all ones that don't have a specific policy or sth)
	// TODO: Implement this :D

	// 2. Send to all of the adapters through neogate

	// Encode event to make sure Neo can process it faster
	msg, err := sonic.Marshal(event)
	if err != nil {
		return fmt.Errorf("couldn't marshal event: %v", err)
	}

	// Send to all adapters locally through Neo
	for _, adapter := range adapters {
		adapter := AdapterFor(adapter.Category, adapter.Adapter)
		if err := i.gate.AdapterReceive(adapter, event, msg); err != nil {
			i.gate.Config.ErrorHandler(fmt.Errorf("[hydro] couldn't send to adapter %s: %v", adapter, err))
		}
	}

	return nil
}
