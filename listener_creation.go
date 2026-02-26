package hydro

import (
	"log"
	"sync"

	"github.com/Liphium/neogate"
	"github.com/dgraph-io/ristretto/v2"
)

type DatabaseListenerCreate[C Change[C]] struct {
	Identifier string                                           // Identifier for the listener (REQUIRED)
	Get        func([]string) (map[string]C, error)             // Get the base data from results of listeners or just with key (required)
	ToEvent    func(key string, change Change[C]) neogate.Event // Should convert string and change info into an event that can be sent with Neo (required)
	FromEvent  func(key string, encodedEvent []byte) Change[C]  // Should convert an event back to a change and its key

	PoolConfig PoolConfig // Config for the pooling of subscription workers
}

// Helper function for initializing a new listener dictionary properly
func NewListenerDictionary[T any, PS IPubSubBackend, DB any, C Change[C]](instance *Instance[T, PS], outbox *PubSubOutbox[DB, PS], create DatabaseListenerCreate[C]) *DatabaseListenerDictionary[T, PS, DB, C] {
	subDict, err := ristretto.NewCache(&ristretto.Config[string, *ListenerSubscriptions[T, PS, C]]{
		MaxCost:     10_000,      // Maximum 10.000 stored items
		NumCounters: 10_000 * 10, // 10x what we want to store
		BufferItems: 64,          // Read description of field
	})
	if err != nil {
		log.Panicf("Couldn't create listener dictionary: %v", err)
	}

	dictionary := &DatabaseListenerDictionary[T, PS, DB, C]{
		Instance:   instance,
		Identifier: create.Identifier,
		outbox:     outbox,

		subDict:     subDict,
		get:         create.Get,
		toEvent:     create.ToEvent,
		createMutex: &sync.Mutex{},
		pool:        NewPubSubPool(instance.pubSub, create.PoolConfig),
	}

	// Create the pool to forward messages to the dictionary
	dictionary.pool.OnMessage(func(channel, message string) {
		key := dictionary.channelToKey(channel)
		event := create.FromEvent(key, []byte(message))
		dictionary.onChange(dictionary.channelToKey(channel), event)
	})

	// Just print warning when an error happens in a channel for now
	// TODO(unbreathable): Let's figure out a proper way to handle errors here and maybe even automatically re-subscribe or sth?
	dictionary.pool.OnError(func(channel string, err error) {
		Log.Printf("WARNING: Error received for channel %s: %v", channel, err)
	})

	return dictionary
}
