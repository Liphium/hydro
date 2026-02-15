package hydro

import "context"

type IPubSubWorker interface {
	Publish(ctx context.Context, channel string, message string) error
	Subscribe(ctx context.Context, channels ...string) error
	Unsubscribe(ctx context.Context, channels ...string) error
	OnMessage(func(channel string, message string))
	OnError(func(channel string))
}

type IPubSubBackend interface {
	CreateWorker() IPubSubWorker
}
