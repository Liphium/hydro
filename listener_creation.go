package hydro

import (
	"context"
	"log"

	"github.com/bytedance/sonic"
	"github.com/dgraph-io/ristretto/v2"
)

type DatabaseListenerCreate[DB any, C Change[C]] struct {
	Identifier string                                   // Identifier for the listener (REQUIRED)
	Get        func(DB, []string) (map[string]C, error) // Get the base data from results of listeners or just with key (required)

	PoolConfig PoolConfig // Config for the pooling of subscription workers
}

// Helper function for initializing a new listener dictionary properly
func NewListenerDictionary[DB any, PS IPubSubBackend[DB], C Change[C]](instance *Instance[DB, PS], create DatabaseListenerCreate[DB, C]) *DatabaseListenerDictionary[DB, PS, C] {
	dictionary := &DatabaseListenerDictionary[DB, PS, C]{
		Instance:   instance,
		Identifier: create.Identifier,

		get:  create.Get,
		pool: NewPubSubPool(instance.pubSub, create.PoolConfig),
	}

	subDict, err := ristretto.NewCache(&ristretto.Config[string, *ListenerSubscriptions[DB, PS, C]]{
		MaxCost:     10_000,      // Maximum 10.000 stored items
		NumCounters: 10_000 * 10, // 10x what we want to store
		BufferItems: 64,          // Read description of field

		OnExit: func(val *ListenerSubscriptions[DB, PS, C]) {
			if err := dictionary.pool.Unsubscribe(context.Background(), val.channel); err != nil {
				Log.Printf("WARNING: Couldn't unsubscribe from channel %s: %v \n", val.channel, err)
			}
		},
	})
	if err != nil {
		log.Panicf("Couldn't create listener dictionary: %v", err)
	}
	dictionary.subDict = subDict

	// Create the pool to forward messages to the dictionary
	dictionary.pool.OnMessage(func(channel, message string) {
		var change C
		if err := sonic.UnmarshalString(message, &change); err != nil {
			Log.Println("ERROR: Couln't process event received through pubsub ("+create.Identifier+"):", err)
			return
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
