package hydro_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Liphium/hydro"
	"github.com/stretchr/testify/assert"
)

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

	t.Run("errors during subscription void all already done subscriptions", func(t *testing.T) {
		// This should not work because channel3 is already registered
		err := worker.Subscribe(ctx, "channel4", "channel5", "channel3")
		assert.ErrorIs(t, err, hydro.ErrChannelAlreadyRegistered)

		// channel4 and channel5 should not be registered
		err = pubsub.Publish(ctx, "channel4", "should-not-work")
		assert.ErrorIs(t, err, hydro.ErrChannelNotRegistered)
		err = pubsub.Publish(ctx, "channel5", "should-not-work")
		assert.ErrorIs(t, err, hydro.ErrChannelNotRegistered)
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

func TestLocalSubWorkerTransactionality(t *testing.T) {
	t.Run("messages are in proper order", func(t *testing.T) {
		pubsub := hydro.NewLocalPubSub()
		ctx := context.Background()
		worker := pubsub.CreateWorker()
		defer worker.Close()

		var mu sync.Mutex
		receivedMessages := []string{}

		worker.OnMessage(func(channel string, message string) {
			mu.Lock()
			defer mu.Unlock()
			receivedMessages = append(receivedMessages, message)
		})

		err := worker.Subscribe(ctx, "test-channel")
		assert.NoError(t, err)

		// Publish messages in order
		expectedMessages := []string{}
		for range 100 {
			msg := time.Now().Format("2006-01-02 15:04:05.000000000")
			expectedMessages = append(expectedMessages, msg)
			err := pubsub.Publish(ctx, "test-channel", msg)
			assert.NoError(t, err)
		}

		// Give goroutine time to process all messages
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		assert.Equal(t, expectedMessages, receivedMessages, "Messages should be received in the same order they were published")
		mu.Unlock()
	})

	t.Run("messages are in proper order across workers", func(t *testing.T) {
		pubsub := hydro.NewLocalPubSub()
		ctx := context.Background()

		// Create multiple workers
		worker1 := pubsub.CreateWorker()
		worker2 := pubsub.CreateWorker()
		worker3 := pubsub.CreateWorker()
		defer worker1.Close()
		defer worker2.Close()
		defer worker3.Close()

		var mu1, mu2, mu3 sync.Mutex
		worker1Messages := []string{}
		worker2Messages := []string{}
		worker3Messages := []string{}

		worker1.OnMessage(func(channel string, message string) {
			mu1.Lock()
			defer mu1.Unlock()
			worker1Messages = append(worker1Messages, message)
		})

		worker2.OnMessage(func(channel string, message string) {
			mu2.Lock()
			defer mu2.Unlock()
			worker2Messages = append(worker2Messages, message)
		})

		worker3.OnMessage(func(channel string, message string) {
			mu3.Lock()
			defer mu3.Unlock()
			worker3Messages = append(worker3Messages, message)
		})

		// Subscribe each worker to their own channel
		err := worker1.Subscribe(ctx, "channel1")
		assert.NoError(t, err)
		err = worker2.Subscribe(ctx, "channel2")
		assert.NoError(t, err)
		err = worker3.Subscribe(ctx, "channel3")
		assert.NoError(t, err)

		// Publish messages to all channels in an interleaved manner
		expected1 := []string{}
		expected2 := []string{}
		expected3 := []string{}

		for range 50 {
			msg1 := time.Now().Format("2006-01-02 15:04:05.000000000") + "-w1"
			expected1 = append(expected1, msg1)
			err := pubsub.Publish(ctx, "channel1", msg1)
			assert.NoError(t, err)

			msg2 := time.Now().Format("2006-01-02 15:04:05.000000000") + "-w2"
			expected2 = append(expected2, msg2)
			err = pubsub.Publish(ctx, "channel2", msg2)
			assert.NoError(t, err)

			msg3 := time.Now().Format("2006-01-02 15:04:05.000000000") + "-w3"
			expected3 = append(expected3, msg3)
			err = pubsub.Publish(ctx, "channel3", msg3)
			assert.NoError(t, err)
		}

		// Give goroutines time to process all messages
		time.Sleep(100 * time.Millisecond)

		// Verify each worker received their messages in the correct order
		mu1.Lock()
		assert.Equal(t, expected1, worker1Messages, "Worker1 should receive messages in order")
		mu1.Unlock()

		mu2.Lock()
		assert.Equal(t, expected2, worker2Messages, "Worker2 should receive messages in order")
		mu2.Unlock()

		mu3.Lock()
		assert.Equal(t, expected3, worker3Messages, "Worker3 should receive messages in order")
		mu3.Unlock()
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
