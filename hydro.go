// Hydro does the following things:
// 1. Manages the lifecycle of adapters for remote neogate clients.
// 2. Provide an endpoint implementation for handling requests.
// 3. Provide a way to send to the adapters using the endpoint.
package hydro

import (
	"fmt"
	"log"
	"os"

	"github.com/Liphium/neogate"
)

var Log = log.New(os.Stdout, "hydro ", log.Flags())

// The Hydro server for just sending the packet directly to local clients
const ServerLocal = "_local"

// Get the name of an adapter Hydro will send to for an id and a category
func AdapterFor(category AdapterCategory, id string) string {
	return fmt.Sprintf("hydro:%s:%s", category, id)
}

type AdapterCategory string

// Address for an adapter on another server in Hydro (might be an account or something else)
type HydroAddress struct {
	Server  string       // Server where the thing exists
	Adapter HydroAdapter // Adapter to call on the target server
}

type HydroAdapter struct {
	Category AdapterCategory // The category the adapter is in
	Adapter  string          // The id of the adapter
}

type Instance[T any] struct {
	gate        *neogate.Instance[T] // The neogate instance Hydro manages
	gatewayPath string               // The gateway path for the Hydro gate on all servers
	pubSub      IPubSubBackend       // The pub/sub backend currently in use
}

type Config[T any] struct {
	// The neogate instance. All events will be sent through here.
	Gate *neogate.Instance[T]

	// Path for the external Hydro gateway (just leave empty if you don't plan on mounting it anyway)
	GatewayPath string

	// The backend for Hydro's pub/sub model (if not set we'll use a local backend that acts as a replacement for a dedicated pub/sub service)
	PubSubBackend IPubSubBackend
}

// Create a new Hydro instance
func New[T any](config *Config[T]) *Instance[T] {
	if config.PubSubBackend == nil {
		config.PubSubBackend = NewLocalPubSub()
	}

	return &Instance[T]{
		gate:        config.Gate,
		gatewayPath: config.GatewayPath,
		pubSub:      config.PubSubBackend,
	}
}
