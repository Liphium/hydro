package hydro

// Ignore, needed for type check below
type s struct{}

func (d s) Stack(s Change[s]) Change[s] {
	return d
}

// For making sure the thing actually implements the interface
var _ PackedMessage = dbldOutboxMessage[any, IPubSubBackend, any, s]{}

type dbldOutboxMessage[T any, PS IPubSubBackend, DB any, C Change[C]] struct {
	dict   *DatabaseListenerDictionary[T, PS, DB, C]
	key    string
	change C
}

func (m dbldOutboxMessage[T, PS, DB, C]) convertToOutbox() (OutboxMessage, error) {
	return m.dict.packageForOutbox(m.key, m.change)
}

func (ld *DatabaseListenerDictionary[T, PS, DB, C]) ForOutbox(key string, change C) dbldOutboxMessage[T, PS, DB, C] {
	return dbldOutboxMessage[T, PS, DB, C]{
		dict:   ld,
		key:    key,
		change: change,
	}
}
