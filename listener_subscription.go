package hydro

import (
	"sync"
	"time"
)

const RecommendedWantInterval = 20 * time.Second // The interval how often you should call "Want" on a subscription
const SubscriptionDuration = 60 * time.Second    // The duration after which a subscription will be deleted

type Subscription[C Change[C]] = func(c Change[C])

// A manager of subscriptions to a Listener that automatically evicts them statelessly when no longer wanted.
//
// Also aggressively caches the current return value from the listener. This is done by stacking the changes using the Stack method from the Change interface. OnSubscribe is usually pretty expensive and managing it like this makes sure we always only call it exactly once.
type ListenerSubscriptions[DB any, PS IPubSubBackend[DB], C Change[C]] struct {
	instance *Instance[DB, PS]
	mu       *sync.Mutex // Mutex for the cache

	channel       string
	subscriptions *sync.Map // List of all subscriptions

	// When initializing the subscriptions it may happen that some changes need to be queued due to the actual base state not being available yet, this boolean and the queue handle queuing these changes and stacking them later
	queuing       bool
	queuedChanges []Change[C] // Ordered with the latest change being at the end of the slice
	cachedChange  Change[C]   // Last cached change (stacked with all previous changes to keep it consistent)
}

// Create a new manager of listener subscriptions
func NewSubs[DB any, PS IPubSubBackend[DB], C Change[C]](channel string, instance *Instance[DB, PS]) *ListenerSubscriptions[DB, PS, C] {
	return &ListenerSubscriptions[DB, PS, C]{
		instance: instance,
		mu:       &sync.Mutex{},
		channel:  channel,

		queuing:       true, // Queuing is enabled in the beginning until Start is called
		queuedChanges: []Change[C]{},
		subscriptions: &sync.Map{},
	}
}

// Mark a listener subscription as wanted (identifier is a unique identifier of the subscription)
func (ls *ListenerSubscriptions[DB, PS, C]) Add(identifier string, subscription func(c Change[C])) {

	// Create a new subscription if there isn't one (if already loaded, we don't need to send anything)
	if _, loaded := ls.subscriptions.LoadOrStore(identifier, subscription); loaded {
		return
	}

	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Only send when not queuing, after queuing is disabled you get the packet anyway
	if !ls.queuing {
		subscription(ls.cachedChange)
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

	// Update cache by stacking the change
	if ls.cachedChange != nil {
		ls.cachedChange = ls.cachedChange.Stack(change)
	} else {
		ls.cachedChange = change
	}

	// Go over all subscriptions and make sure they are still valid + call callback (if desired)
	ls.subscriptions.Range(func(key, value any) bool {
		value.(Subscription[C])(change)
		return true
	})
}
