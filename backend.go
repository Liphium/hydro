package hydro

import "context"

type IMutexBackend interface {
	// Tries to lock a mutex for some channels (returns true if locked)
	TryLock(ctx context.Context, channels []string) (bool, error)

	// Blocks until all the channels
	Lock(ctx context.Context, channels []string) error

	// Blocks until all the channels are unlocked (or returns an error)
	Unlock(ctx context.Context, channels []string) error
}
