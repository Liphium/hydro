package hydro

import (
	"fmt"
	"sync"

	"github.com/Liphium/neogate"
	"github.com/dgraph-io/ristretto/v2"
)

type Identifiable interface {
	GetIdentifier() string
}

type ListenerDictionary[T any, C Change[C]] struct {
	Instance   *Instance[T] // Hydro instance related
	Identifier string       // Unique identifier for this listener dictionary

	// Dictionary for managing the subscriptions by key
	subDict *ristretto.Cache[string, *ListenerSubscriptions[T, C]]

	get         func([]string) (map[string]C, error)
	convert     func(string, C) neogate.Event
	createMutex *sync.Mutex
}

func (ld *ListenerDictionary[T, C]) GetIdentifier() string {
	return ld.Identifier
}

// DictionarySubscribe to a listener in the dictionary
func DictionarySubscribe[T any, C Change[C], S Subscription[C]](ld *ListenerDictionary[T, C], keys []string, identifier string, subscription S) error {

	// Find all listeners that are not already available
	nonCached := []string{}
	for _, key := range keys {

		// If there already is an existing listener, just use that one
		if subs, ok := ld.subDict.Get(key); ok {
			Want(subs, identifier, subscription)
		} else {
			nonCached = append(nonCached, key)
		}
	}
	if len(nonCached) == 0 {
		return nil
	}

	// Get the data for all listeners that haven't cached yet
	results, err := ld.Get(nonCached)
	if err != nil {
		return fmt.Errorf("couldn't get from listener: %v", err)
	}

	// Create new listeners and subscribe
	for key, change := range results {
		subs := CreateSubsWith(ld.Instance, func(change C) neogate.Event {
			return ld.convert(key, change)
		}, change)
		if !ld.subDict.SetWithTTL(key, subs, 1, SubscriptionDuration) {
			var ok bool
			subs, ok = ld.subDict.Get(key)
			if !ok {
				return fmt.Errorf("coudln't get listener for key: %s", key)
			}
		}
		Want(subs, identifier, subscription)
	}
	ld.subDict.Wait()

	return nil
}

// Get the value for keys from the listener dictionary (makes sure we can add batching in the future)
func (ld *ListenerDictionary[T, C]) Get(keys []string) (map[string]C, error) {
	return ld.get(keys)
}
