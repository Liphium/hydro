package hydro

import (
	"context"
	"errors"
	"sync"
)

var ErrChannelNotSubscribed = errors.New("channel not subscribed")

// Make sure the pool implements the same
var _ IPubSubWorker = &PubSubPool[any]{}

type PoolConfig struct {
	// How many subscriptions should, at maximum, be done by one worker (default: 100)
	MaxAmountByWorker int
}

type workerInfo struct {
	worker            IPubSubWorker
	subscriptionCount int
}

type PubSubPool[T any] struct {
	config   PoolConfig
	instance *Instance[T]

	mu               sync.RWMutex
	workers          []*workerInfo
	channelToWorker  map[string]*workerInfo // Track which worker handles which channel
	onMessageHandler func(channel string, message string)
	onErrorHandler   func(channel string, err error)
}

func NewPubSubPool[T any](instance *Instance[T], config PoolConfig) *PubSubPool[T] {
	if config.MaxAmountByWorker == 0 {
		config.MaxAmountByWorker = 100
	}
	return &PubSubPool[T]{
		config:          config,
		instance:        instance,
		workers:         make([]*workerInfo, 0),
		channelToWorker: make(map[string]*workerInfo),
	}
}

// Publish publishes a message to a subscribed channel
func (p *PubSubPool[T]) Publish(ctx context.Context, channel string, message string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Find the worker that handles this channel
	worker, exists := p.channelToWorker[channel]
	if !exists {
		return ErrChannelNotSubscribed
	}

	return worker.worker.Publish(ctx, channel, message)
}

// Subscribe subscribes to channels, distributing them across workers
func (p *PubSubPool[T]) Subscribe(ctx context.Context, channels ...string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Group channels by whether they need a new subscription or already exist
	newChannels := make([]string, 0)
	for _, channel := range channels {
		if _, exists := p.channelToWorker[channel]; !exists {
			newChannels = append(newChannels, channel)
		}
	}

	if len(newChannels) == 0 {
		return nil // All channels already subscribed
	}

	// Find or create a worker with capacity
	worker, err := p.getOrCreateWorkerWithCapacity()
	if err != nil {
		return err
	}

	// Subscribe to the new channels
	if err := worker.worker.Subscribe(ctx, newChannels...); err != nil {
		return err
	}

	// Track the subscriptions
	for _, channel := range newChannels {
		p.channelToWorker[channel] = worker
		worker.subscriptionCount++
	}

	return nil
}

// Unsubscribe unsubscribes from channels
func (p *PubSubPool[T]) Unsubscribe(ctx context.Context, channels ...string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Group channels by worker
	workerChannels := make(map[*workerInfo][]string)
	for _, channel := range channels {
		if worker, exists := p.channelToWorker[channel]; exists {
			workerChannels[worker] = append(workerChannels[worker], channel)
		}
	}

	// Unsubscribe from each worker
	for worker, chans := range workerChannels {
		if err := worker.worker.Unsubscribe(ctx, chans...); err != nil {
			return err
		}

		// Update tracking
		for _, channel := range chans {
			delete(p.channelToWorker, channel)
		}

		worker.subscriptionCount -= len(chans)
	}

	return nil
}

// OnMessage sets the message handler for all workers
func (p *PubSubPool[T]) OnMessage(handler func(channel string, message string)) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onMessageHandler = handler

	// Apply to all existing workers
	for _, w := range p.workers {
		w.worker.OnMessage(handler)
	}
}

// OnError sets the error handler for all workers
func (p *PubSubPool[T]) OnError(handler func(channel string, err error)) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.onErrorHandler = handler

	// Apply to all existing workers
	for _, w := range p.workers {
		w.worker.OnError(handler)
	}
}

// getOrCreateWorkerWithCapacity finds a worker with capacity or creates a new one
// Must be called with p.mu locked
func (p *PubSubPool[T]) getOrCreateWorkerWithCapacity() (*workerInfo, error) {
	// Find a worker with available capacity
	for _, w := range p.workers {
		if w.subscriptionCount < p.config.MaxAmountByWorker {
			return w, nil
		}
	}

	// All workers are at capacity, create a new one
	return p.createWorker()
}

// createWorker creates a new worker and adds it to the pool
// Must be called with p.mu locked
func (p *PubSubPool[T]) createWorker() (*workerInfo, error) {
	worker := p.instance.pubSub.CreateWorker()

	// Set handlers if they've been configured
	if p.onMessageHandler != nil {
		worker.OnMessage(p.onMessageHandler)
	}
	if p.onErrorHandler != nil {
		worker.OnError(p.onErrorHandler)
	}

	info := &workerInfo{
		worker:            worker,
		subscriptionCount: 0,
	}

	p.workers = append(p.workers, info)
	return info, nil
}

func (p *PubSubPool[T]) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, worker := range p.workers {
		worker.worker.Close()
		worker.subscriptionCount = 0
	}
	clear(p.workers)
	clear(p.channelToWorker)
}
