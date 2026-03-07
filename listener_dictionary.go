package hydro

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Liphium/neogate"
	"github.com/bytedance/sonic"
	"github.com/dgraph-io/ristretto/v2"
)

type Identifiable interface {
	GetIdentifier() string
}

type DatabaseListenerDictionary[T any, PS IPubSubBackend, DB any, C Change[C]] struct {
	Instance   *Instance[T, PS] // Hydro instance related
	Identifier string           // Unique identifier for this listener dictionary
	outbox     *PubSubOutbox[DB, PS]

	// Dictionary for managing the subscriptions by key
	subDict *ristretto.Cache[string, *ListenerSubscriptions[T, PS, C]]

	get         func(DB, []string) (map[string]C, error)
	toEvent     func(string, Change[C]) neogate.Event
	createMutex *sync.Mutex
	pool        *SubPool[PS]
}

func (ld *DatabaseListenerDictionary[T, PS, DB, C]) GetIdentifier() string {
	return ld.Identifier
}

// Generate the channel being used for a key of this listener dictionary in Hydro pub/sub
func (ld *DatabaseListenerDictionary[T, PS, DB, C]) keyToChannel(key string) string {
	return "ld:" + ld.Identifier + ":" + key
}

// Generate the key from a Hydro pub/sub channel
func (ld *DatabaseListenerDictionary[T, PS, DB, C]) channelToKey(channel string) string {
	values := strings.SplitN(channel, ":", 3)
	if len(values) != 3 {
		Log.Fatalln("Invalid channel:", channel)
	}
	return values[2]
}

func DictionarySubscribe[T any, PS IPubSubBackend, DB any, C Change[C], S Subscription[C]](ld *DatabaseListenerDictionary[T, PS, DB, C], db DB, keys []string, identifier string, subscription S) error {

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

	// Create subscriptions so that they can start receiving stuff already
	return createSubscriptions(ld, db, keys, identifier, subscription)
}

// Create subscriptions in the ListenerDictionary
// TODO(unbreathable): How do we fix broken subscriptions in case an error is returned below subscription creation?
func createSubscriptions[T any, PS IPubSubBackend, DB any, C Change[C], S Subscription[C]](ld *DatabaseListenerDictionary[T, PS, DB, C], db DB, keys []string, identifier string, subscription S) error {
	ctx := context.Background()

	toGet := []string{}
	toSubscribe := []string{}
	for _, key := range keys {
		subs := NewSubs(ld.Instance, func(change Change[C]) neogate.Event {
			return ld.toEvent(key, change)
		})
		if !ld.subDict.SetWithTTL(key, subs, 1, SubscriptionDuration) {
			var ok bool
			subs, ok = ld.subDict.Get(key)
			if !ok {
				return fmt.Errorf("couldn't get listener for key: %s", key)
			}
		}
		Want(subs, identifier, subscription)

		toGet = append(toGet, key)
		toSubscribe = append(toSubscribe, ld.keyToChannel(key))
	}
	ld.subDict.Wait()

	// Subscribe to pub/sub for all the keys to make sure we get all the changes
	if err := ld.pool.Subscribe(ctx, toSubscribe...); err != nil {
		return fmt.Errorf("couldn't create pub/sub subscription: %v", err)
	}

	// Get the base data for all listeners that were created
	results, err := ld.Get(db, toGet)
	if err != nil {
		return fmt.Errorf("couldn't get from base data: %v", err)
	}

	// Set the results for all the keys
	for _, key := range keys {
		result, ok := results[key]
		if !ok {
			return fmt.Errorf("didn't get data for key: %v", key)
		}

		if subs, ok := ld.subDict.Get(key); ok {
			subs.DisableQueuing(result)
		}
	}
	return nil
}

// Remove subscriptions for an identifier
func (ld *DatabaseListenerDictionary[T, PS, DB, C]) Unsubscribe(db DB, keys []string, identifier string) {
	for _, key := range keys {
		if subs, ok := ld.subDict.Get(key); ok {
			subs.Delete(identifier)
		}
	}
}

// Reset all values for the keys back to the original value by re-getting them and pushing that update
func (ld *DatabaseListenerDictionary[T, PS, DB, C]) Reset(db DB, keys []string) error {
	results, err := ld.get(db, keys)
	if err != nil {
		return err
	}

	// Build all of the messages for the outbox
	messages := make([]OutboxMessage, len(keys))
	for i, key := range keys {
		change, ok := results[key]
		if !ok {
			return fmt.Errorf("couldn't find result for key %s", key)
		}

		// Package the message for the outbox
		message, err := ld.packageForOutbox(key, change)
		if err != nil {
			return fmt.Errorf("couldn't package key %s for outbox: %v", key, err)
		}
		messages[i] = message
	}

	// Publish all the messages to the outbox
	return ld.outbox.save(db, messages)
}

// Get the value for keys from the listener dictionary (makes sure we can add batching in the future)
func (ld *DatabaseListenerDictionary[T, PS, DB, C]) Get(db DB, keys []string) (map[string]C, error) {
	return ld.get(db, keys)
}

// Handle a change for a specific key
func (ld *DatabaseListenerDictionary[T, PS, DB, C]) onChange(key string, change Change[C]) {
	if subs, ok := ld.subDict.Get(key); ok {
		subs.OnChange(change)
	}
}

// Package a key and change for the outbox
func (ld *DatabaseListenerDictionary[T, PS, DB, C]) packageForOutbox(key string, change Change[C]) (OutboxMessage, error) {
	var message OutboxMessage
	bytes, err := sonic.Marshal(change)
	if err != nil {
		return message, err
	}
	message = OutboxMessage{
		Identifier: ld.keyToChannel(key),
		Data:       bytes,
	}
	return message, nil
}

// Save a change to the outbox, makes sure all of this stays transactional
func (ld *DatabaseListenerDictionary[T, PS, DB, C]) Save(db DB, key string, change Change[C]) error {
	message, err := ld.packageForOutbox(key, change)
	if err != nil {
		return err
	}

	return ld.outbox.save(db, []OutboxMessage{
		message,
	})
}
