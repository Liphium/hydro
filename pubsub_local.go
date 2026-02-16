package hydro

import (
	"context"
	"errors"
	"slices"
	"sync"
)

var (
	ErrChannelNotRegistered     = errors.New("channel is not registered")
	ErrChannelAlreadyRegistered = errors.New("channel already registered by different worker")
)

var _ IPubSubBackend = &LocalPubSub{}
var _ ISubWorker = &LocalSubWorker{}

type LocalPubSub struct {
	channelMap *sync.Map
}

func NewLocalPubSub() *LocalPubSub {
	return &LocalPubSub{
		channelMap: &sync.Map{},
	}
}

func (w *LocalPubSub) Publish(ctx context.Context, channel string, message string) error {
	ch, ok := w.channelMap.Load(channel)
	if !ok {
		return ErrChannelNotRegistered
	}
	ch.(chan localPubSubMessage) <- localPubSubMessage{
		channel: channel,
		message: message,
	}
	return nil
}

func (lpb *LocalPubSub) CreateWorker() ISubWorker {
	return newLocalSubWorker(lpb)
}

type localPubSubMessage struct {
	channel string
	message string
}

type LocalSubWorker struct {
	backend        *LocalPubSub
	messageChan    chan localPubSubMessage
	closeChan      chan struct{}
	mu             *sync.Mutex
	subscriptions  []string
	messageHandler func(channel string, message string)
	errorHandler   func(channel string, err error)
}

func newLocalSubWorker(pubSub *LocalPubSub) *LocalSubWorker {
	worker := &LocalSubWorker{
		backend:       pubSub,
		subscriptions: []string{},
		messageChan:   make(chan localPubSubMessage),
		closeChan:     make(chan struct{}, 1),
		mu:            &sync.Mutex{},
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

func (w *LocalSubWorker) Subscribe(ctx context.Context, channels ...string) error {
	for _, channel := range channels {
		if _, ok := w.backend.channelMap.LoadOrStore(channel, w.messageChan); ok {
			return ErrChannelAlreadyRegistered
		}
	}
	w.subscriptions = append(w.subscriptions, channels...)
	return nil
}

func (w *LocalSubWorker) Unsubscribe(ctx context.Context, channels ...string) error {
	for _, channel := range channels {
		w.backend.channelMap.Delete(channel)
	}
	w.subscriptions = slices.DeleteFunc(w.subscriptions, func(channel string) bool {
		return slices.Contains(channels, channel)
	})
	return nil
}

func (w *LocalSubWorker) OnMessage(handler func(channel string, message string)) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.messageHandler = handler
}

func (w *LocalSubWorker) OnError(handler func(channel string, err error)) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.errorHandler = handler
}

func (w *LocalSubWorker) Close() {
	w.closeChan <- struct{}{}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, sub := range w.subscriptions {
		w.backend.channelMap.Delete(sub)
	}
}
