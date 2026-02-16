package hydro

import "context"

type ISubWorker interface {
	Subscribe(ctx context.Context, channels ...string) error
	Unsubscribe(ctx context.Context, channels ...string) error
	OnMessage(func(channel string, message string))
	OnError(func(channel string, err error))
	Close()
}

type IPubSubBackend interface {
	Publish(ctx context.Context, channel string, message string) error
	CreateWorker() ISubWorker
}
