package hydro

type Backend interface {
	// Tries to lock a mutex for some ids (returns true if locked)
	TryLock(id []string) (bool, error)

	// Should block until the mutex for all of the ids is locked
	Lock(id []string) error

	Unlock(id []string) error

	Publish(channel string, data any) error
	Subscribe(channel string) chan any
}
