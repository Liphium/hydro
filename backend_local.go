package hydro

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
)

// Make sure the local backend conforms to the IMutexBackend interface (so it can work in Hydro)
var _ IMutexBackend = &LocalBackend{}

type LocalBackend struct {
	mutexes *sync.Map // Channel id -> Mutex
}

type storedMutex struct {
	mutex   *sync.Mutex
	waiting *atomic.Int32
}

// Local implementation of TryLock using a simple sync.Map with mutexes.
//
// Worst case: O(n^2) since all keys have to be deleted again when the last lock fails.
func (lb *LocalBackend) TryLock(_ context.Context, channels []string) (bool, error) {

	// Sort all of the channels (to prevent deadlocks)
	slices.Sort(channels)

	for i, channel := range channels {
		obj, _ := lb.mutexes.LoadOrStore(channel, &storedMutex{
			mutex:   &sync.Mutex{},
			waiting: &atomic.Int32{},
		})
		stored := obj.(*storedMutex)

		if !stored.mutex.TryLock() {

			// Unlock all previously locked mutexes
			for _, lockedChannel := range slices.Backward(channels[:i]) {
				lockedObj, lockedOk := lb.mutexes.Load(lockedChannel)
				if !lockedOk {
					return false, errors.New("locked mutex no longer in map")
				}

				locked := lockedObj.(*storedMutex)
				locked.mutex.Unlock()
			}

			return false, nil
		}
	}
	return true, nil
}

// Local implementation of locking using a simple sync.Map with mutexes.
func (lb *LocalBackend) Lock(_ context.Context, channels []string) error {

	// Sort all of the channels (to prevent deadlocks)
	slices.Sort(channels)

	for _, channel := range channels {
		obj, _ := lb.mutexes.LoadOrStore(channel, &sync.Mutex{})
		stored := obj.(*storedMutex)

		stored.waiting.Add(1)
		stored.mutex.Lock()
		stored.waiting.Add(-1)
		lb.mutexes.Store(channel, stored) // We insert here again since we might have been deleted (our waiting might not have been seen yet)
	}
	return nil
}

// Local implementation of unlocking using a simple sync.Map with mutexes.
func (lb *LocalBackend) Unlock(_ context.Context, channels []string) error {

	// Sort all of the channels (to prevent deadlocks)
	slices.Sort(channels)

	for _, channel := range slices.Backward(channels) {
		obj, ok := lb.mutexes.Load(channel)
		if !ok {
			return errors.New("locked mutex not found")
		}
		stored := obj.(*storedMutex)

		stored.mutex.Unlock()

		// Delete the mutex from the map when there aren't any waiters
		if stored.waiting.Load() == 0 {
			lb.mutexes.Delete(channel)
		}
	}
	return nil
}
