package hydro

import (
	"context"
	"time"
)

// The implementation for the outbox goes here.
//
// This will allow Hydro to scale horizontally as well as keep transactionality on one node by:
// 1. Writing the change update to the database when it happens
// 2. Then taking it out again when we're sending it to pub/sub
//
// Important here:
// - The next event with the same identifier can only be processed when the last one was deleted (successfully sent through pub/sub)
// - We need a goroutine that pulls from the database every 100 milliseconds (or sth) and pushes stuff to pub/sub

type PubSubOutbox[DB any, PS IPubSubBackend] struct {
	backend PS // The pub sub backend used for the outbox

	closeChan chan struct{} // For closing the outbox

	save func(database DB, message OutboxMessage) error
}

type OutboxMessage struct {
	Identifier string
	Data       []byte
}

type OutboxCreate[DB any] struct {

	// How long the outbox waits before pulling from the database again (default: 100 milliseconds).
	WaitDuration time.Duration

	// This function should save an event with an identifier to the database.
	Save func(database DB, message OutboxMessage) error

	// This is the main function handling the pulling of messages for the outbox.
	//
	// handler takes in a list of messages that you pulled from your database of choice in a transaction. It then returns which message identifiers were completed and an error for the first one that failed. Make sure your transaction skips any currently locked items in the table for the pull (the best pattern here is to just use one row / identifier to make sure no out-of-order stuff happens, if an insertion fails you can always just return a "already processing" error in most cases). Make sure to delete all the ones that have been completed.
	Tx func(database DB, handler func([]OutboxMessage) ([]string, error))
}

// Create a new Outbox for pub/sub. This is a data structure that can make sure all of your pub/sub stays transactional no matter which pub/sub implementation to use. This works with basically any database. All you need to do is create tables for the Outbox and also make sure you implement all the functions as required by the create.
func NewOutbox[T any, DB any, PS IPubSubBackend](instance *Instance[T, PS], connection DB, create OutboxCreate[DB]) *PubSubOutbox[DB, PS] {
	outbox := &PubSubOutbox[DB, PS]{
		backend:   instance.pubSub,
		save:      create.Save,
		closeChan: make(chan struct{}),
	}

	// Start a goroutione that pulls from the database and makes sure all the events are pushed into pub/sub
	go func() {
		waitDuration := create.WaitDuration
		if waitDuration == 0 {
			waitDuration = 100 * time.Millisecond
		}

		backoff := waitDuration
		maxBackoff := 30 * time.Second

		for {
			select {
			case <-outbox.closeChan:
				return
			case <-time.After(backoff):
				create.Tx(connection, func(messages []OutboxMessage) ([]string, error) {
					if len(messages) == 0 {
						// Reset backoff when no messages
						backoff = waitDuration
						return nil, nil
					}

					// Reset backoff on successful pull with messages
					backoff = waitDuration

					var completed []string
					for _, message := range messages {

						// Send the encoded event to pub/sub
						err := outbox.backend.Publish(context.Background(), message.Identifier, string(message.Data))
						if err != nil {
							// On publish error, use exponential backoff
							backoff *= 2
							if backoff > maxBackoff {
								backoff = maxBackoff
							}
							return completed, err
						}
						completed = append(completed, message.Identifier)
					}
					return completed, nil
				})
			}
		}
	}()

	return outbox
}

// Save an event to the outbox. Use this for transactional pub/sub using the database.
func (o *PubSubOutbox[DB, PS]) Save(db DB, identifier string, data []byte) error {
	return o.save(db, OutboxMessage{
		Identifier: identifier,
		Data:       data,
	})
}

// WATCH OUT: This does not publish to the outbox. Use Save for that. This is the same as calling the Publish function on the original pub/sub backend.
func (o *PubSubOutbox[DB, PS]) Publish(c context.Context, identifier string, message string) error {
	return o.backend.Publish(c, identifier, message)
}

// This method just wraps the function from the backend so you can still use the outbox as a pub/sub backend
func (o *PubSubOutbox[DB, PS]) CreateWorker() ISubWorker {
	return o.backend.CreateWorker()
}

// Stop the outbox from pulling from the database. After this you can not restart it.
func (o *PubSubOutbox[DB, PS]) Close() {
	o.closeChan <- struct{}{}
}
