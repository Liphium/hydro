package hydro

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// This is how long the TTL of a lock goes
	redisLockTTLSeconds = 1
)

var _ IMutexBackend = &RedisBackend{}

type RedisBackend struct {
	rdb *redis.Client
	id  string
}

// Create a new backend that uses Redis for mutexes.
//
// Just to answer the obvious question: Yes, this is slower then the local backend, but it still tries its best to only need one Redis operation when you want to lock something. It just isn't good with a lot of writes.
//
// Note: This backend **does not** support redis in Cluster mode. We may add a backend that can support Redis clusters in the future.
func NewRedisBackend(rdb *redis.Client) *RedisBackend {
	return &RedisBackend{
		rdb: rdb,

		// This id should hopefully never collide with another Hydro instance.
		// We could probably just negotiate this through Redis for absolute safety, but this should do...
		id: fmt.Sprintf("%s-%d", randomString(8), time.Now().Unix()),
	}
}

func lockKey(id string) string { return "hydro:redisv1:" + id }

// Redis implementation of TryLock using one batch operation that sets all the keys with expiration.
func (rb *RedisBackend) TryLock(ctx context.Context, channels []string) (bool, error) {

	// Sort all of the channels (to prevent deadlocks)
	slices.Sort(channels)

	// Try and lock everything
	return rb.tryLockAll(ctx, channels)
}

// Helper function that wraps the batch lock
func (rb *RedisBackend) tryLockAll(ctx context.Context, channels []string) (bool, error) {
	valueList := make([]interface{}, len(channels)*2)
	for i, channel := range channels {
		valueList[i*2] = channel
		valueList[i*2+1] = rb.id
	}

	cmd := rb.rdb.MSetEX(ctx, redis.MSetEXArgs{
		Condition: redis.NX,
		Expiration: &redis.ExpirationOption{
			Mode:  redis.EX,
			Value: redisLockTTLSeconds,
		},
	}, valueList...)
	return cmd.Val() == 1, nil
}

// Local implementation of locking using a simple sync.Map with mutexes.
func (lb *RedisBackend) Lock(_ context.Context, channels []string) error {
	// TODO: Implement
	return nil
}

// Local implementation of unlocking using a simple sync.Map with mutexes.
func (lb *RedisBackend) Unlock(_ context.Context, channels []string) error {
	// TODO: Implement
	return nil
}
