package hydro_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Liphium/hydro"
	"github.com/stretchr/testify/assert"
)

// TODO: Test to make sure on subscription error all subscriptions are voided again

func TestLocalPubSub(t *testing.T) {
	t.Run("can't publish to non-existent channel", func(t *testing.T) {
		pubsub := hydro.NewLocalPubSub()
		ctx := context.Background()

		err := pubsub.Publish(ctx, "non-existent-channel", "test message")
		assert.ErrorIs(t, err, hydro.ErrChannelNotRegistered)
	})
}

func TestLocalSubWorker(t *testing.T) {
	var pubsub *hydro.LocalPubSub
	var worker hydro.ISubWorker
	ctx := context.Background()

	t.Run("can subscribe to channels", func(t *testing.T) {
		pubsub = hydro.NewLocalPubSub()
		worker = pubsub.CreateWorker()

		err := worker.Subscribe(ctx, "channel1")
		assert.NoError(t, err)

		err = worker.Subscribe(ctx, "channel2", "channel3")
		assert.NoError(t, err)
	})

	t.Run("can receive messages from subscribed channels", func(t *testing.T) {
		var mu sync.Mutex
		receivedMessages := make(map[string][]string)

		worker.OnMessage(func(channel string, message string) {
			mu.Lock()
			defer mu.Unlock()
			receivedMessages[channel] = append(receivedMessages[channel], message)
		})

		// Publish messages to different channels
		err := pubsub.Publish(ctx, "channel1", "message1")
		assert.NoError(t, err)

		err = pubsub.Publish(ctx, "channel2", "message2")
		assert.NoError(t, err)

		err = pubsub.Publish(ctx, "channel3", "message3")
		assert.NoError(t, err)

		// Give goroutine time to process messages
		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		assert.Equal(t, []string{"message1"}, receivedMessages["channel1"])
		assert.Equal(t, []string{"message2"}, receivedMessages["channel2"])
		assert.Equal(t, []string{"message3"}, receivedMessages["channel3"])
		mu.Unlock()
	})

	t.Run("unsubscribe stops message receiving", func(t *testing.T) {
		err := worker.Unsubscribe(ctx, "channel1", "channel2")
		assert.NoError(t, err)

		// Try to publish to unsubscribed channels - should error
		err = pubsub.Publish(ctx, "channel1", "should-not-receive")
		assert.ErrorIs(t, err, hydro.ErrChannelNotRegistered)

		err = pubsub.Publish(ctx, "channel2", "should-not-receive")
		assert.ErrorIs(t, err, hydro.ErrChannelNotRegistered)

		// Channel3 should still work
		err = pubsub.Publish(ctx, "channel3", "still-works")
		assert.NoError(t, err)
	})

	// Verify by trying to publish to the channels, should error
	t.Run("closure deletes all subscriptions", func(t *testing.T) {
		// Close the worker - this should automatically clean up all subscriptions
		worker.Close()

		// Give goroutine time to clean up
		time.Sleep(50 * time.Millisecond)

		// Try to publish to channel3 which was still subscribed before Close()
		// This should fail because Close() should have removed all subscriptions
		err := pubsub.Publish(ctx, "channel3", "after-close")
		assert.ErrorIs(t, err, hydro.ErrChannelNotRegistered,
			"Close() should remove all subscriptions from the channelMap")
	})
}

func TestLocalSubWorkerMany(t *testing.T) {
	pubsub := hydro.NewLocalPubSub()
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
		err := pubsub.Publish(ctx, "worker1-channel1", "msg1")
		assert.NoError(t, err)

		err = pubsub.Publish(ctx, "worker1-channel2", "msg2")
		assert.NoError(t, err)

		// Publish to worker2's channels
		err = pubsub.Publish(ctx, "worker2-channel1", "msg3")
		assert.NoError(t, err)

		err = pubsub.Publish(ctx, "worker2-channel2", "msg4")
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

	t.Run("subscribing to a channel on one worker denies subscribing on a different one", func(t *testing.T) {
		// Try to subscribe worker2 to a channel already owned by worker1
		err := worker2.Subscribe(ctx, "worker1-channel1")
		assert.ErrorIs(t, err, hydro.ErrChannelAlreadyRegistered)

		// Try to subscribe worker1 to a channel already owned by worker2
		err = worker1.Subscribe(ctx, "worker2-channel1")
		assert.ErrorIs(t, err, hydro.ErrChannelAlreadyRegistered)

		// Verify that a completely new worker also can't subscribe
		worker3 := pubsub.CreateWorker()
		err = worker3.Subscribe(ctx, "worker1-channel1")
		assert.ErrorIs(t, err, hydro.ErrChannelAlreadyRegistered)

		worker3.Close()
	})

	// Cleanup
	worker1.Close()
	worker2.Close()
}
