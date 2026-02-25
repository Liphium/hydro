package hydro

import (
	"context"
	"time"
)

// This is just for the type check below
type m struct{}

func (m m) Identifier() string { return "t" }
func (m m) Message() string    { return "t" }

// Make sure the outbox complies with the pub/sub backend interface
var _ IPubSubBackend = &PubSubOutbox[any, *LocalPubSub, m]{}

// The implementation for the outbox goes here.
//
// This will allow Hydro to scale horizontally as well as keep transactionality on one node by:
// 1. Writing the change update to the database when it happens
// 2. Then taking it out again when we're sending it to pub/sub
//
// Important here:
// - The next event with the same identifier can only be processed when the last one was deleted (successfully sent through pub/sub)
// - We need a goroutine that pulls from the database every 100 milliseconds (or sth) and pushes stuff to pub/sub

type PubSubOutbox[DB any, PS IPubSubBackend, M OutboxMessage] struct {
	backend PS // The pub sub backend used for the outbox

	closeChan chan struct{} // For closing the outbox

	save func(database DB, message M) error
}

type OutboxMessage interface {
	Identifier() string
	Message() string
}

type OutboxCreate[DB any, PS IPubSubBackend, M OutboxMessage] struct {
	Backend PS

	// How long the outbox waits before pulling from the database again (default: 100 milliseconds).
	WaitDuration time.Duration

	// This function should save a message with an identifier to the database.
	Save func(database DB, message M) error

	// This is the main function handling the pulling of messages for the outbox.
	//
	// handler takes in a list of messages that you pulled from your database of choice in a transaction. It then returns which messages were completed and an error for the first one that failed. Make sure your transaction skips any currently locked items in the table for the pull. Make sure to delete all the ones that have been completed.
	Tx func(database DB, handler func([]M) ([]M, error))
}

// Create a new Outbox for pub/sub. This is a data structure that can make sure all of your pub/sub stays transactional no matter which pub/sub implementation to use. This works with basically any database. All you need to do is create tables for the Outbox and also make sure you implement all the functions as required by the create.
func NewOutbox[DB any, PS IPubSubBackend, M OutboxMessage](connection DB, create OutboxCreate[DB, PS, M]) *PubSubOutbox[DB, PS, M] {
	outbox := &PubSubOutbox[DB, PS, M]{
		backend:   create.Backend,
		save:      create.Save,
		closeChan: make(chan struct{}),
	}

	// Start a goroutione that pulls from the database and makes sure all the messages are pushed into pub/sub
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
				create.Tx(connection, func(messages []M) ([]M, error) {
					if len(messages) == 0 {
						// Reset backoff when no messages
						backoff = waitDuration
						return nil, nil
					}

					// Reset backoff on successful pull with messages
					backoff = waitDuration

					var completed []M
					for _, msg := range messages {
						err := outbox.backend.Publish(context.Background(), msg.Identifier(), msg.Message())
						if err != nil {
							// On publish error, use exponential backoff
							backoff *= 2
							if backoff > maxBackoff {
								backoff = maxBackoff
							}
							return completed, err
						}
						completed = append(completed, msg)
					}
					return completed, nil
				})
			}
		}
	}()

	return outbox
}

// Save a message to the outbox. Use this for transactional pub/sub using the database.
func (o *PubSubOutbox[DB, PS, M]) Save(db DB, message M) error {
	return o.save(db, message)
}

// WATCH OUT: This does not publish to the outbox. Use Save for that. This is the same as calling the Publish function on the original pub/sub backend.
func (o *PubSubOutbox[DB, PS, M]) Publish(c context.Context, identifier string, message string) error {
	return o.backend.Publish(c, identifier, message)
}

// This method just wraps the function from the backend so you can still use the outbox as a pub/sub backend
func (o *PubSubOutbox[DB, PS, M]) CreateWorker() ISubWorker {
	return o.backend.CreateWorker()
}

// Stop the outbox from pulling from the database. After this you can not restart it.
func (o *PubSubOutbox[DB, PS, M]) Close() {
	o.closeChan <- struct{}{}
}
