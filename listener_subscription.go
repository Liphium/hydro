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
	func(neogate.Event, Change[C]) | HydroAddress
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
type ListenerSubscriptions[T any, PS IPubSubBackend, C Change[C]] struct {
	instance *Instance[T, PS]
	convert  func(Change[C]) neogate.Event
	mu       *sync.Mutex

	subscriptions *sync.Map // List of all subscriptions

	// When initializing the subscriptions it may happen that some changes need to be queued due to the actual base state not being available yet, this boolean and the queue handle queuing these changes and stacking them later
	queuing       bool
	queuedChanges []Change[C] // Ordered with the latest change being at the end of the slice
	cachedChange  Change[C]   // Last cached change (stacked with all previous changes to keep it consistent)
}

// Create a new manager of listener subscriptions
func NewSubs[T any, PS IPubSubBackend, C Change[C]](instance *Instance[T, PS], convert func(Change[C]) neogate.Event) *ListenerSubscriptions[T, PS, C] {
	return &ListenerSubscriptions[T, PS, C]{
		instance: instance,
		convert:  convert,
		mu:       &sync.Mutex{},

		queuing:       true, // Queuing is enabled in the beginning until Start is called
		queuedChanges: []Change[C]{},
		subscriptions: &sync.Map{},
	}
}

// Mark a listener subscription as wanted (identifier is a unique identifier of the subscription)
func Want[T any, PS IPubSubBackend, C Change[C], S Subscription[C]](ls *ListenerSubscriptions[T, PS, C], identifier string, subscription S) error {
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

		ls.mu.Lock()
		defer ls.mu.Unlock()

		// Only send when not queuing, after queuing is disabled you get the packet anyway
		if !ls.queuing {
			sendToSubscription(ls, sub, ls.cachedChange)
		}
	}

	return nil
}

// Refresh a listener subscription and mark it as wanted (identifier is a unique identifier of the subscription)
func Refresh[T any, PS IPubSubBackend, C Change[C], S Subscription[C]](ls *ListenerSubscriptions[T, PS, C], identifier string) {
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

// Delete a subscription from the subscriptions
func (ls *ListenerSubscriptions[T, PS, C]) Delete(identifier string) {
	ls.subscriptions.Delete(identifier)
}

// After DisableQueuing the subscriptions start to actually send changes when they are received, before all are queued
func (ls *ListenerSubscriptions[T, PS, C]) DisableQueuing(base Change[C]) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// If we're not queuing anymore, just ignore it
	if !ls.queuing {
		return
	}

	// Stack all queued changes on top of the base one
	for _, change := range ls.queuedChanges {
		base = base.Stack(change)
	}
	ls.queuing = false
	ls.onChangeNoMutex(base)
}

// Check if the subscriptions are currently still in queuing mode
func (ls *ListenerSubscriptions[T, PS, C]) IsQueuing() bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	return ls.queuing
}

// Handles a change and sends it to all subscribers of the listener
func (ls *ListenerSubscriptions[T, PS, C]) OnChange(change Change[C]) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.onChangeNoMutex(change)
}

// Handles a change and sends it to all subscribers of the listener (THIS DOES NOT LOCK THE MUTEX)
func (ls *ListenerSubscriptions[T, PS, C]) onChangeNoMutex(change Change[C]) {

	// If we're in queuing mode, queue the change
	if ls.queuing {
		ls.queuedChanges = append(ls.queuedChanges, change)
		return
	}

	event := ls.convert(change)

	// Update cache by stacking the change
	if ls.cachedChange != nil {
		ls.cachedChange = ls.cachedChange.Stack(change)
	} else {
		ls.cachedChange = change
	}

	minimumValidDate := time.Now().Add(-SubscriptionDuration)
	collectedAddresses := []HydroAddress{}

	// Go over all subscriptions and make sure they are still valid + call callback (if desired)
	ls.subscriptions.Range(func(key, value any) bool {
		switch sub := value.(type) {
		case *managedSubscription[C, func(neogate.Event, Change[C])]:

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
func sendToSubscription[T any, PS IPubSubBackend, C Change[C], S Subscription[C]](ls *ListenerSubscriptions[T, PS, C], sub *managedSubscription[C, S], change Change[C]) {
	event := ls.convert(change)

	switch s := any(sub).(type) {
	case *managedSubscription[C, func(neogate.Event, Change[C])]:
		// Forward the event to the callback
		s.sub(event, change)

	case *managedSubscription[C, HydroAddress]:
		// Send via Hydro to the specific address
		ls.instance.Send([]HydroAddress{s.sub}, event)
	}
}
