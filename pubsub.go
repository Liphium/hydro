package hydro

import (
	"context"
	"errors"
)

// Standardized errors for pub/sub
var (
	ErrChannelNotRegistered     = errors.New("channel is not registered")
	ErrChannelAlreadyRegistered = errors.New("channel already registered by different worker")
)

type ISubWorker interface {
	Subscribe(ctx context.Context, channels ...string) error
	Unsubscribe(ctx context.Context, channels ...string) error
	OnMessage(func(channel string, message string))
	OnError(func(channel string, err error))
	Close()
}

type IPubSubBackend interface {
	CreateWorker() ISubWorker
}
