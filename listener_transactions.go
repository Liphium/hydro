package hydro

import (
	"slices"
	"strings"
)

type TransactionCapable interface {
	lockFor(keys []string) ([]string, []any)
	unlockFor(locks []any)
	onChange(locks []any, results map[string]any)
	Identifiable
}

// Union type for storing subs and key in one struct
type lockedSub[T any, C Change[C]] struct {
	key  string
	subs *ListenerSubscriptions[T, C]
}

func (ld *ListenerDictionary[T, C]) lockFor(keys []string) ([]string, []any) {
	slices.Sort(keys)

	// Do locking in a predictable order (this makes sure no deadlocks can happen, even across multiple keys)
	lockedSubs := []any{}
	lockedKeys := []string{}
	for _, key := range keys {
		if subs, ok := ld.subDict.Get(key); ok {
			subs.cacheMutex.Lock()
			lockedSubs = append(lockedSubs, lockedSub[T, C]{
				key:  key,
				subs: subs,
			})
			lockedKeys = append(lockedKeys, key)
		}
	}

	return lockedKeys, lockedSubs
}

func (ld *ListenerDictionary[T, C]) unlockFor(locks []any) {
	for _, locked := range slices.Backward(locks) {
		lock := locked.(lockedSub[T, C])
		lock.subs.cacheMutex.Unlock()
	}
}

func (ld *ListenerDictionary[T, C]) onChange(locks []any, results map[string]any) {

	// Update all the cached changes
	for _, obj := range locks {
		locked := obj.(lockedSub[T, C])

		result, ok := results[locked.key]
		if !ok {
			continue
		}
		locked.subs.OnChange(result.(C))
	}
}

type TxObject struct {
	Object TransactionCapable
	Keys   []string

	// The locks gotten from the TransactionCapable interface (used for future calls to it)
	locks      []any
	lockedKeys []string
	changes    map[string]any
}

type Context struct {
	objects map[string]*TxObject
}

// Run a transaction on multiple objects (listener dictionaries / single listeners) capable of transactions
func Tx(objects []*TxObject, transaction func(*Context) error) error {

	// Sort the objects by identifier to prevent deadlocks
	slices.SortFunc(objects, func(a, b *TxObject) int {
		return strings.Compare(a.Object.GetIdentifier(), b.Object.GetIdentifier())
	})

	// Lock all of the objects in the proper order
	for _, object := range objects {
		object.lockedKeys, object.locks = object.Object.lockFor(object.Keys)
	}

	// Make sure all the locks are cleaned up properly
	defer func() {
		if obj := recover(); obj != nil {
			Log.Println("WARNING: error in hydro transaction:", obj)
		}

		for _, object := range slices.Backward(objects) {

			// Send changes in case there are any
			object.Object.onChange(object.locks, object.changes)

			// Finally, unlock the thingy and set it free
			object.Object.unlockFor(object.locks)
		}
	}()

	// Sort the objects into a map for better access in the context
	objectMap := map[string]*TxObject{}
	for _, object := range objects {
		objectMap[object.Object.GetIdentifier()] = object
	}
	ctx := Context{
		objects: objectMap,
	}

	return transaction(&ctx)
}
