package hydro

import (
	"sync"
	"time"

	"github.com/Liphium/neogate"
)

const RecommendedWantInterval = 20 * time.Second // The interval how often you should call "Want" on a subscription
const SubscriptionDuration = 60 * time.Second    // The duration after which a subscription will be deleted

// A subscription can either be done using a callback or a Hydro address
type Subscription[C Change[C]] interface {
	func(neogate.Event, C) | HydroAddress
}

type managedSubscription[C Change[C], S Subscription[C]] struct {
	mutex      *sync.RWMutex // Mutex for the want time
	lastWanted time.Time     // The last time the subscription was wanted by the subscriber
	sub        S             // The actual wrapped Subscription
	OnWant     func()        // Function called when the subscription is wanted (will be useful for later when we add back dependencies)
}

// A manager of subscriptions to a Listener that automatically evicts them statelessly when no longer wanted.
//
// Also aggressively caches the current return value from the listener. This is done by stacking the changes using the Stack method from the Change interface. OnSubscribe is usually pretty expensive and managing it like this makes sure we always only call it exactly once.
type ListenerSubscriptions[T any, C Change[C]] struct {
	instance *Instance[T]
	convert  func(C) neogate.Event

	subscriptions *sync.Map // List of all subscriptions

	// Caching
	cacheMutex   *sync.RWMutex // Mutex for cache access (managed by parent)
	cachedChange C             // Last cached change (stacked)
}

// Create a new manager of listener subscriptions
func CreateSubsWith[T any, C Change[C]](instance *Instance[T], convert func(C) neogate.Event, change C) *ListenerSubscriptions[T, C] {
	return &ListenerSubscriptions[T, C]{
		instance: instance,
		convert:  convert,

		subscriptions: &sync.Map{},

		cacheMutex:   &sync.RWMutex{},
		cachedChange: change,
	}
}

// Mark a listener subscription as wanted (identifier is a unique identifier of the subscription)
func Want[T any, C Change[C], S Subscription[C]](ls *ListenerSubscriptions[T, C], identifier string, subscription S) error {
	if obj, ok := ls.subscriptions.Load(identifier); ok {

		// Mark the current subscription as wanted
		sub := obj.(*managedSubscription[C, S])
		if sub.OnWant != nil {
			sub.OnWant()
		}
		sub.mutex.Lock()
		sub.lastWanted = time.Now()
		sub.mutex.Unlock()
	} else {

		// Create a new subscription if there isn't one
		sub := &managedSubscription[C, S]{
			mutex:      &sync.RWMutex{},
			lastWanted: time.Now(),
			sub:        subscription,
			// TODO: Add OnWant function or something so dependencies between listeners can work again
		}
		ls.subscriptions.Store(identifier, sub)

		ls.cacheMutex.RLock()
		change := ls.cachedChange
		ls.cacheMutex.RUnlock()
		sendToSubscription(ls, sub, change)
	}

	return nil
}

// Refresh a listener subscription and mark it as wanted (identifier is a unique identifier of the subscription)
func Refresh[T any, C Change[C], S Subscription[C]](ls *ListenerSubscriptions[T, C], identifier string) {
	if obj, ok := ls.subscriptions.Load(identifier); ok {
		sub := obj.(*managedSubscription[C, S])
		if sub.OnWant != nil {
			sub.OnWant()
		}
		sub.mutex.Lock()
		sub.lastWanted = time.Now()
		sub.mutex.Unlock()
	}
}

// Handles a change and sends it to all subscribers of the listener (THIS DOES NOT LOCK THE MUTEX)
func (ls *ListenerSubscriptions[T, C]) OnChange(change C) {
	event := ls.convert(change)

	// Update cache by stacking the change
	ls.cachedChange = ls.cachedChange.Stack(change).(C)

	minimumValidDate := time.Now().Add(-SubscriptionDuration)
	collectedAddresses := []HydroAddress{}

	// Go over all subscriptions and make sure they are still valid + call callback (if desired)
	ls.subscriptions.Range(func(key, value any) bool {
		switch sub := value.(type) {
		case *managedSubscription[C, func(neogate.Event, C)]:

			// Delete the subscription when it's no longer valid
			if sub.lastWanted.Before(minimumValidDate) {
				ls.subscriptions.Delete(key)
				return true
			}

			// Forward the event to the callback
			sub.sub(event, change)

		case *managedSubscription[C, HydroAddress]:

			// Delete the subscription when it's no longer valid (duplicate ik, but we can't do better here due to Go's type system)
			if sub.lastWanted.Before(minimumValidDate) {
				ls.subscriptions.Delete(key)
				return true
			}

			// Add the address to the collective Hydro send call
			collectedAddresses = append(collectedAddresses, sub.sub)
		}
		return true
	})

	// Do a Hydro send to all adapters called
	if len(collectedAddresses) > 0 {
		ls.instance.Send(collectedAddresses, event)
	}
}

// Send an event to a specific subscription
func sendToSubscription[T any, C Change[C], S Subscription[C]](ls *ListenerSubscriptions[T, C], sub *managedSubscription[C, S], change C) {
	event := ls.convert(change)

	switch s := any(sub).(type) {
	case *managedSubscription[C, func(neogate.Event, C)]:
		// Forward the event to the callback
		s.sub(event, change)

	case *managedSubscription[C, HydroAddress]:
		// Send via Hydro to the specific address
		ls.instance.Send([]HydroAddress{s.sub}, event)
	}
}
