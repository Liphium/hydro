package hydro

import (
	"log"
	"sync"

	"github.com/Liphium/neogate"
	"github.com/bytedance/sonic"
	"github.com/dgraph-io/ristretto/v2"
)

type ListenerCreate[C Change[C]] struct {
	Identifier string                                // Identifier for the listener (REQUIRED)
	Get        func([]string) (map[string]C, error)  // Get the base data from results of listeners or just with key (required)
	Convert    func(string, Change[C]) neogate.Event // Should convert string and change info into an event that can be sent with Neo (required)

	PoolConfig PoolConfig // Config for the pooling of subscription workers
}

// Helper function for initializing a new listener dictionary properly
func NewListenerDictionary[T any, PS IPubSubBackend, C Change[C]](instance *Instance[T, PS], create ListenerCreate[C]) *ListenerDictionary[T, PS, C] {
	subDict, err := ristretto.NewCache(&ristretto.Config[string, *ListenerSubscriptions[T, PS, C]]{
		MaxCost:     10_000,      // Maximum 10.000 stored items
		NumCounters: 10_000 * 10, // 10x what we want to store
		BufferItems: 64,          // Read description of field
	})
	if err != nil {
		log.Panicf("Couldn't create listener dictionary: %v", err)
	}

	dictionary := &ListenerDictionary[T, PS, C]{
		Instance:   instance,
		Identifier: create.Identifier,

		subDict:     subDict,
		get:         create.Get,
		convert:     create.Convert,
		createMutex: &sync.Mutex{},
		pool:        NewPubSubPool(instance.pubSub, create.PoolConfig),
	}

	// Create the pool and subscribe and stuff
	dictionary.pool.OnMessage(func(channel, message string) {
		var change C
		if err := sonic.UnmarshalString(message, &change); err != nil {
			Log.Printf("WARNING: Invalid change received in %s: %v (%v) \n", channel, message, err)
		}

		dictionary.onChange(dictionary.channelToKey(channel), change)
	})

	// Just print warning when an error happens in a channel for now
	// TODO(unbreathable): Let's figure out a proper way to handle errors here and maybe even automatically re-subscribe or sth?
	dictionary.pool.OnError(func(channel string, err error) {
		Log.Printf("WARNING: Error received for channel %s: %v", channel, err)
	})

	return dictionary
}
