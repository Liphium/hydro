package hydro

type IPubSubSubscription interface {
	Receive() (string, error)
	Close()
}

type IPubSubBackend interface {
	Publish(channel string, message string) error
	Subscribe(channel string) IPubSubSubscription
}
