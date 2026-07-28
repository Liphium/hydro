package hydro

import (
	"log"
	"os"
)

var Log = log.New(os.Stdout, "hydro ", log.Flags())

type Instance[DB any, PS IPubSubBackend[DB]] struct {
	pubSub PS // The pub/sub backend currently in use
}

type Config[DB any, PS IPubSubBackend[DB]] struct {

	// The backend for Hydro's pub/sub model (if not set we'll use a local backend that acts as a replacement for a dedicated pub/sub service)
	PubSubBackend PS
}

// Create a new Hydro instance
func New[DB any, PS IPubSubBackend[DB]](config *Config[DB, PS]) *Instance[DB, PS] {
	return &Instance[DB, PS]{
		pubSub: config.PubSubBackend,
	}
}
