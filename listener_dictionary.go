package hydro

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/bytedance/sonic"
	"github.com/dgraph-io/ristretto/v2"
)

type Identifiable interface {
	GetIdentifier() string
}

type DatabaseListenerDictionary[DB any, PS IPubSubBackend[DB], C Change[C]] struct {
	Instance   *Instance[DB, PS] // Hydro instance related
	Identifier string            // Unique identifier for this listener dictionary

	// Dictionary for managing the subscriptions by key
	subDict *ristretto.Cache[string, *ListenerSubscriptions[DB, PS, C]]

	get         func(DB, []string) (map[string]C, error)
	createMutex sync.Mutex
	pool        *SubPool[DB, PS]
}

func (ld *DatabaseListenerDictionary[DB, PS, C]) GetIdentifier() string {
	return ld.Identifier
}

// Generate the channel being used for a key of this listener dictionary in Hydro pub/sub
func (ld *DatabaseListenerDictionary[DB, PS, C]) keyToChannel(key string) string {
	return "ld:" + ld.Identifier + ":" + key
}

// Generate the key from a Hydro pub/sub channel
func (ld *DatabaseListenerDictionary[DB, PS, C]) channelToKey(channel string) string {
	values := strings.SplitN(channel, ":", 3)
	if len(values) != 3 {
		Log.Fatalln("Invalid channel:", channel)
	}
	return values[2]
}

func (ld *DatabaseListenerDictionary[DB, PS, C]) Subscribe(db DB, keys []string, identifier string, subscription func(change Change[C])) error {

	// Find all listeners that are not already available
	nonCached := []string{}
	for _, key := range keys {

		// If there already is an existing listener, just use that one
		if subs, ok := ld.subDict.Get(key); ok {
			subs.Add(identifier, subscription)
		} else {
			nonCached = append(nonCached, key)
		}
	}
	if len(nonCached) == 0 {
		return nil
	}

	// Create subscriptions so that they can start receiving stuff already
	return ld.createSubscriptions(db, keys, identifier, subscription)
}

// Create subscriptions in the ListenerDictionary
// TODO(unbreathable): How do we fix broken subscriptions in case an error is returned below subscription creation?
func (ld *DatabaseListenerDictionary[DB, PS, C]) createSubscriptions(db DB, keys []string, identifier string, subscription func(change Change[C])) error {
	ctx := context.Background()

	toGet := []string{}
	toSubscribe := []string{}
	for _, key := range keys {
		subs := NewSubs[DB, PS, C](key, ld.Instance)
		if !ld.subDict.SetWithTTL(key, subs, 1, SubscriptionDuration) {
			var ok bool
			subs, ok = ld.subDict.Get(key)
			if !ok {
				return fmt.Errorf("couldn't get listener for key: %s", key)
			}
		}
		subs.Add(identifier, subscription)

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
func (ld *DatabaseListenerDictionary[DB, PS, C]) Unsubscribe(identifier string, keys []string) {
	for _, key := range keys {
		if subs, ok := ld.subDict.Get(key); ok {
			subs.Delete(identifier)
		}
	}
}

// Reset all values for the keys back to the original value by re-getting them and pushing that update
func (ld *DatabaseListenerDictionary[DB, PS, C]) Reset(ctx context.Context, db DB, keys []string) error {
	results, err := ld.get(db, keys)
	if err != nil {
		return err
	}

	// Build all of the messages for the outbox
	for _, key := range keys {
		change, ok := results[key]
		if !ok {
			return fmt.Errorf("couldn't find result for key %s", key)
		}

		// Encode the thing using json
		msg, err := ld.encode(key, change)
		if err != nil {
			return fmt.Errorf("encode: %w", err)
		}

		// Publish to pub/sub so everyone knows about the change
		if err := ld.Instance.pubSub.Publish(ctx, db, ld.keyToChannel(key), msg); err != nil {
			return fmt.Errorf("pub/sub publish: %w", err)
		}
	}

	return nil
}

// Get the value for keys from the listener dictionary (makes sure we can add batching in the future)
func (ld *DatabaseListenerDictionary[DB, PS, C]) Get(db DB, keys []string) (map[string]C, error) {
	return ld.get(db, keys)
}

// Handle a change for a specific key
func (ld *DatabaseListenerDictionary[DB, PS, C]) onChange(key string, change Change[C]) {
	if subs, ok := ld.subDict.Get(key); ok {
		subs.OnChange(change)
	}
}

// Package a key and change for the outbox
func (ld *DatabaseListenerDictionary[DB, PS, C]) encode(key string, change Change[C]) (string, error) {
	return sonic.MarshalString(change)
}

// Update a key with a change, will broadcast the event to all subscribers + cache it
func (ld *DatabaseListenerDictionary[DB, PS, C]) Update(ctx context.Context, db DB, key string, change Change[C]) error {

	// Encode the thing using json
	msg, err := ld.encode(key, change)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	return ld.Instance.pubSub.Publish(ctx, db, ld.keyToChannel(key), msg)
}
