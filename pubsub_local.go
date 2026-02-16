package hydro

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrChannelAlreadyRegistered = errors.New("channel already registered by different worker")
)

var _ IPubSubBackend = &LocalPubSub{}
var _ IPubSubWorker = &LocalPubSubWorker{}

// TODO: Make this thing create workers that essentially just pass their stuff through the main LocalPubSub struct so they can all talk to each other

type LocalPubSub struct {
	channelMap *sync.Map
}

func NewLocalPubSub() *LocalPubSub {
	return &LocalPubSub{
		channelMap: &sync.Map{},
	}
}

func (lpb *LocalPubSub) CreateWorker() IPubSubWorker {
	return nil
}

type localPubSubMessage struct {
	channel string
	message string
}

type LocalPubSubWorker struct {
	backend        *LocalPubSub
	messageChan    chan localPubSubMessage
	closeChan      chan struct{}
	mu             *sync.Mutex
	messageHandler func(channel string, message string)
	errorHandler   func(channel string, err error)
}

func newLocalPubSubWorker(pubSub *LocalPubSub) *LocalPubSubWorker {
	worker := &LocalPubSubWorker{
		backend:     pubSub,
		messageChan: make(chan localPubSubMessage),
		closeChan:   make(chan struct{}, 1),
		mu:          &sync.Mutex{},
	}

	go func() {
		for {
			select {
			// Listen for messages and forward them to the handler
			case msg := <-worker.messageChan:
				worker.mu.Lock()
				worker.messageHandler(msg.channel, msg.message)
				worker.mu.Unlock()

			// Let the goroutine shut down when we receive a close signal
			case <-worker.closeChan:
				close(worker.messageChan)
				close(worker.closeChan)
				return
			}
		}
	}()
	return worker
}

func (w *LocalPubSubWorker) Publish(ctx context.Context, channel string, message string) error {
	w.messageChan <- localPubSubMessage{
		channel: channel,
		message: message,
	}
	return nil
}

func (w *LocalPubSubWorker) Subscribe(ctx context.Context, channels ...string) error {
	for _, channel := range channels {
		if _, ok := w.backend.channelMap.LoadOrStore(channel, w.messageChan); ok {
			return ErrChannelAlreadyRegistered
		}
	}
	return nil
}

func (w *LocalPubSubWorker) Unsubscribe(ctx context.Context, channels ...string) error {
	for _, channel := range channels {
		w.backend.channelMap.Delete(channel)
	}
	return nil
}

func (w *LocalPubSubWorker) OnMessage(handler func(channel string, message string)) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.messageHandler = handler
}

func (w *LocalPubSubWorker) OnError(handler func(channel string, err error)) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.errorHandler = handler
}

func (w *LocalPubSubWorker) Close() {
	w.closeChan <- struct{}{}
}
