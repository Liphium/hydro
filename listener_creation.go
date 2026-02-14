package hydro

import (
	"log"
	"sync"
	"time"

	"github.com/Liphium/neogate"
	"github.com/dgraph-io/ristretto/v2"
)

type BatchOptions struct {
	BatchDuration time.Duration
	MaxAmount     int
}

type ListenerCreate[C Change[C]] struct {
	Identifier string                               // Identifier for the listener (REQUIRED)
	Get        func([]string) (map[string]C, error) // Get the base data from results of listeners or just with key (required)
	Convert    func(string, C) neogate.Event        // Should convert string and change info into an event that can be sent with Neo (required)
	Batching   *BatchOptions
}

// Helper function for initializing a new listener dictionary properly
func NewListenerDictionary[T any, C Change[C]](instance *Instance[T], create ListenerCreate[C]) *ListenerDictionary[T, C] {
	subDict, err := ristretto.NewCache(&ristretto.Config[string, *ListenerSubscriptions[T, C]]{
		MaxCost:     10_000,      // Maximum 10.000 stored items
		NumCounters: 10_000 * 10, // 10x what we want to store
		BufferItems: 64,          // Read description of field
	})
	if err != nil {
		log.Panicf("Couldn't create listener dictionary: %v", err)
	}

	return &ListenerDictionary[T, C]{
		Instance:   instance,
		Identifier: create.Identifier,

		subDict:     subDict,
		get:         create.Get,
		convert:     create.Convert,
		createMutex: &sync.Mutex{},
	}
}
