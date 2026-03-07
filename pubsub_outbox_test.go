package hydro_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Liphium/hydro"
	"github.com/Liphium/neogate"
	"github.com/stretchr/testify/assert"
)

// MockDB is an in-memory database that simulates outbox persistence.
// It stores messages in a slice protected by a mutex, mimicking a real
// database table with row-level locking via a processing flag.
type MockDB struct {
	mu         sync.Mutex
	messages   []hydro.OutboxMessage
	processing map[string]bool // identifiers currently being processed
}

func NewMockDB() *MockDB {
	return &MockDB{
		processing: make(map[string]bool),
	}
}

// save appends new messages to the in-memory store (simulates an INSERT).
func (db *MockDB) save(messages []hydro.OutboxMessage) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.messages = append(db.messages, messages...)
	return nil
}

// tx simulates a transactional pull: it locks rows that are not currently
// being processed, hands them to the handler, and deletes the completed ones.
func (db *MockDB) tx(handler func([]hydro.OutboxMessage) ([]string, error)) {
	db.mu.Lock()

	// Collect messages whose identifier is not already being processed.
	var available []hydro.OutboxMessage
	for _, msg := range db.messages {
		if !db.processing[msg.Identifier] {
			available = append(available, msg)
			db.processing[msg.Identifier] = true
		}
	}

	db.mu.Unlock()

	if len(available) == 0 {
		return
	}

	completed, _ := handler(available)

	// Build a set of completed identifiers for O(1) lookup.
	completedSet := make(map[string]bool, len(completed))
	for _, id := range completed {
		completedSet[id] = true
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Remove completed messages from the store.
	remaining := make([]hydro.OutboxMessage, 0, len(db.messages))
	for _, msg := range db.messages {
		if !completedSet[msg.Identifier] {
			remaining = append(remaining, msg)
		}
	}
	db.messages = remaining

	// Release processing locks.
	for _, msg := range available {
		delete(db.processing, msg.Identifier)
	}
}

// count returns the number of messages currently stored (used in assertions).
func (db *MockDB) count() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return len(db.messages)
}

var _ hydro.PackedMessage = simplePacked{}

type simplePacked struct {
	id   string
	data []byte
}

func (sp simplePacked) ConvertToOutbox() (hydro.OutboxMessage, error) {
	return hydro.OutboxMessage{
		Identifier: sp.id,
		Data:       sp.data,
	}, nil
}

// newOutboxInstance creates a hydro Instance and a MockDB-backed PubSubOutbox.
func newOutboxInstance(db *MockDB, waitDuration time.Duration) (*hydro.PubSubOutbox[*MockDB, *hydro.LocalPubSub], *hydro.LocalPubSub) {
	backend := hydro.NewLocalPubSub()
	instance := hydro.New(&hydro.Config[neogate.None, *hydro.LocalPubSub]{
		PubSubBackend: backend,
	})

	outbox := hydro.NewOutbox(instance, db, hydro.OutboxCreate[*MockDB]{
		WaitDuration: waitDuration,
		Save: func(database *MockDB, messages []hydro.OutboxMessage) error {
			return database.save(messages)
		},
		Tx: func(database *MockDB, handler func([]hydro.OutboxMessage) ([]string, error)) {
			database.tx(handler)
		},
	})

	return outbox, backend
}

func TestOutbox(t *testing.T) {
	t.Run("saved message is delivered to subscriber", func(t *testing.T) {
		db := NewMockDB()
		outbox, backend := newOutboxInstance(db, 20*time.Millisecond)
		defer outbox.Close()

		ctx := context.Background()
		worker := backend.CreateWorker()
		defer worker.Close()

		received := make(chan string, 10)
		worker.OnMessage(func(channel string, message string) {
			received <- message
		})

		err := worker.Subscribe(ctx, "test-channel")
		assert.NoError(t, err)

		// Save a message through the outbox (simulating a DB transaction).
		err = outbox.Save(db, "test-channel", []byte("hello"))
		assert.NoError(t, err)

		select {
		case msg := <-received:
			assert.Equal(t, "hello", msg)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timed out waiting for message to be delivered")
		}

		// The message should have been deleted from the store after delivery.
		assert.Eventually(t, func() bool {
			return db.count() == 0
		}, 500*time.Millisecond, 10*time.Millisecond, "message should be removed from store after delivery")
	})

	t.Run("multiple messages are all delivered", func(t *testing.T) {
		db := NewMockDB()
		outbox, backend := newOutboxInstance(db, 20*time.Millisecond)
		defer outbox.Close()

		ctx := context.Background()
		worker := backend.CreateWorker()
		defer worker.Close()

		const total = 5
		var mu sync.Mutex
		receivedMessages := make(map[string][]string)

		worker.OnMessage(func(channel string, message string) {
			mu.Lock()
			defer mu.Unlock()
			receivedMessages[channel] = append(receivedMessages[channel], message)
		})

		channels := []string{"ch-a", "ch-b", "ch-c", "ch-d", "ch-e"}
		err := worker.Subscribe(ctx, channels...)
		assert.NoError(t, err)

		for i, ch := range channels {
			err := outbox.Save(db, ch, []byte(channels[i]))
			assert.NoError(t, err)
		}

		assert.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			count := 0
			for _, msgs := range receivedMessages {
				count += len(msgs)
			}
			return count == total
		}, 500*time.Millisecond, 10*time.Millisecond, "all messages should be delivered")

		assert.Eventually(t, func() bool {
			return db.count() == 0
		}, 500*time.Millisecond, 10*time.Millisecond, "all messages should be removed from store")
	})

	t.Run("message for unregistered channel is still removed from store", func(t *testing.T) {
		db := NewMockDB()
		outbox, _ := newOutboxInstance(db, 20*time.Millisecond)
		defer outbox.Close()

		// No subscriber for this channel — Publish returns ErrChannelNotRegistered
		// which the outbox treats as a non-fatal error and still marks as completed.
		err := outbox.Save(db, "no-subscriber-channel", []byte("orphan"))
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			return db.count() == 0
		}, 500*time.Millisecond, 10*time.Millisecond, "orphan message should be removed from store even without a subscriber")
	})

	t.Run("SaveMultiple stores and delivers all messages atomically", func(t *testing.T) {
		db := NewMockDB()
		outbox, backend := newOutboxInstance(db, 20*time.Millisecond)
		defer outbox.Close()

		ctx := context.Background()
		worker := backend.CreateWorker()
		defer worker.Close()

		var mu sync.Mutex
		receivedMessages := make(map[string][]string)

		worker.OnMessage(func(channel string, message string) {
			mu.Lock()
			defer mu.Unlock()
			receivedMessages[channel] = append(receivedMessages[channel], message)
		})

		err := worker.Subscribe(ctx, "ch-a", "ch-b", "ch-c")
		assert.NoError(t, err)

		payloads := []hydro.PackedMessage{
			simplePacked{"ch-a", []byte("a")},
			simplePacked{"ch-b", []byte("b")},
			simplePacked{"ch-c", []byte("c")},
		}
		outbox.SaveMultiple(db, payloads)

		assert.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(receivedMessages["ch-a"]) == 1 &&
				len(receivedMessages["ch-b"]) == 1 &&
				len(receivedMessages["ch-c"]) == 1
		}, 500*time.Millisecond, 10*time.Millisecond, "all bulk messages should be delivered")

		assert.Eventually(t, func() bool {
			return db.count() == 0
		}, 500*time.Millisecond, 10*time.Millisecond, "store should be empty after bulk delivery")
	})

	t.Run("closing the outbox stops processing", func(t *testing.T) {
		db := NewMockDB()
		outbox, backend := newOutboxInstance(db, 50*time.Millisecond)

		ctx := context.Background()
		worker := backend.CreateWorker()
		defer worker.Close()

		received := make(chan string, 10)
		worker.OnMessage(func(_ string, message string) {
			received <- message
		})

		err := worker.Subscribe(ctx, "close-test-channel")
		assert.NoError(t, err)

		// Deliver one message, then close.
		err = outbox.Save(db, "close-test-channel", []byte("before-close"))
		assert.NoError(t, err)

		select {
		case msg := <-received:
			assert.Equal(t, "before-close", msg)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timed out waiting for pre-close message")
		}

		outbox.Close()

		// Save another message after closing — the outbox loop has exited so it
		// must never arrive on the channel.
		err = db.save([]hydro.OutboxMessage{{Identifier: "close-test-channel", Data: []byte("after-close")}})
		assert.NoError(t, err)

		select {
		case msg := <-received:
			t.Fatalf("unexpected message received after close: %s", msg)
		case <-time.After(200 * time.Millisecond):
			// Expected: no message delivered after the outbox was stopped.
		}
	})
}
