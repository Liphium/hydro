package hydro

import (
	"sync"

	"github.com/Liphium/neogate"
)

type SingleListener[T any, C Change[C]] struct {
	instance *Instance[T] // Hydro instance related
	mutex    *sync.RWMutex
	subs     *ListenerSubscriptions[T, C]
	get      func() (C, error)
	convert  func(C) neogate.Event
}

func SingleSubscribe[T any, C Change[C], S Subscription[C]](instance *Instance[T], sl *SingleListener[T, C], identifier string, subscription S) error {
	sl.mutex.RLock()

	if sl.subs == nil {
		sl.mutex.RUnlock()

		// Make sure the thing isn't gotten twice
		if err := func() error {
			sl.mutex.Lock()
			defer sl.mutex.Unlock()

			// Only create when not yet created by another call (could potentially happen)
			if sl.subs == nil {

				// Get the change from the get function
				change, err := sl.get()
				if err != nil {
					return err
				}

				sl.subs = CreateSubsWith(instance, sl.convert, change)
			}
			return nil
		}(); err != nil {
			return err
		}

		sl.mutex.RLock()
	}
	defer sl.mutex.RUnlock()

	return Want(sl.subs, identifier, subscription)
}

func (sl *SingleListener[T, C]) OnChange(change C) {
	sl.subs.OnChange(change)
}
