package hydro

var _ IPubSubBackend = &LocalPubSub{}
var _ IPubSubSubscription = &LocalPubSubSubscription{}

type LocalPubSubSubscription struct{}

func (sub *LocalPubSubSubscription) Receive() (string, error)

func (sub *LocalPubSubSubscription) Close()

type LocalPubSub struct{}

func NewLocalPubSub() *LocalPubSub {
	return &LocalPubSub{}
}

func (lpb *LocalPubSub) Publish(channel string, message string) error {
	return nil
}

func (lpb *LocalPubSub) Subscribe(channel string) IPubSubSubscription {
	return nil
}
