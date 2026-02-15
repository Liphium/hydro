package hydro

var _ IPubSubBackend = &LocalPubSub{}

// TODO: Make this thing create workers that essentially just pass their stuff through the main LocalPubSub struct so they can all talk to each other

type LocalPubSub struct{}

func NewLocalPubSub() *LocalPubSub {
	return &LocalPubSub{}
}

func (lpb *LocalPubSub) CreateWorker() IPubSubWorker {
	return nil
}
