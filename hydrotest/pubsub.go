package hydrotest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Liphium/hydro"
	"github.com/stretchr/testify/assert"
)

func TestPubSubBackend[DB any, B hydro.IPubSubBackend[DB]](t *testing.T, newBackend func() B, publish func(backend B, c context.Context, channel, message string) error) {
	t.Helper()

	t.Run("worker", func(t *testing.T) {
		pubsub := newBackend()

		TestSubWorker(t, pubsub.CreateWorker, func(c context.Context, channel, message string) error {
			return publish(pubsub, c, channel, message)
		})
	})

	t.Run("many sub workers", func(t *testing.T) {
		pubsub := newBackend()
		ctx := context.Background()

		// Setup workers and subscriptions
		worker1 := pubsub.CreateWorker()
		worker2 := pubsub.CreateWorker()

		var mu1, mu2 sync.Mutex
		worker1Messages := make(map[string][]string)
		worker2Messages := make(map[string][]string)

		worker1.OnMessage(func(channel string, message string) {
			mu1.Lock()
			defer mu1.Unlock()
			worker1Messages[channel] = append(worker1Messages[channel], message)
		})

		worker2.OnMessage(func(channel string, message string) {
			mu2.Lock()
			defer mu2.Unlock()
			worker2Messages[channel] = append(worker2Messages[channel], message)
		})

		// Subscribe workers to different channels
		err := worker1.Subscribe(ctx, "worker1-channel1", "worker1-channel2")
		assert.NoError(t, err)

		err = worker2.Subscribe(ctx, "worker2-channel1", "worker2-channel2")
		assert.NoError(t, err)

		t.Run("publishing messages sends to the correct worker", func(t *testing.T) {
			// Publish to worker1's channels
			err := publish(pubsub, ctx, "worker1-channel1", "msg1")
			assert.NoError(t, err)

			err = publish(pubsub, ctx, "worker1-channel2", "msg2")
			assert.NoError(t, err)

			// Publish to worker2's channels
			err = publish(pubsub, ctx, "worker2-channel1", "msg3")
			assert.NoError(t, err)

			err = publish(pubsub, ctx, "worker2-channel2", "msg4")
			assert.NoError(t, err)

			// Give goroutines time to process
			time.Sleep(50 * time.Millisecond)

			// Verify worker1 received only its messages
			mu1.Lock()
			assert.Equal(t, []string{"msg1"}, worker1Messages["worker1-channel1"])
			assert.Equal(t, []string{"msg2"}, worker1Messages["worker1-channel2"])
			assert.Empty(t, worker1Messages["worker2-channel1"])
			assert.Empty(t, worker1Messages["worker2-channel2"])
			mu1.Unlock()

			// Verify worker2 received only its messages
			mu2.Lock()
			assert.Equal(t, []string{"msg3"}, worker2Messages["worker2-channel1"])
			assert.Equal(t, []string{"msg4"}, worker2Messages["worker2-channel2"])
			assert.Empty(t, worker2Messages["worker1-channel1"])
			assert.Empty(t, worker2Messages["worker1-channel2"])
			mu2.Unlock()
		})

		// Cleanup
		worker1.Close()
		worker2.Close()
	})
}
