package hydro_test

import (
	"context"
	"testing"

	"github.com/Liphium/hydro"
	"github.com/Liphium/hydro/hydrotest"
)

func TestLocalPubSub(t *testing.T) {
	hydrotest.TestPubSubBackend(t, hydro.NewLocalPubSub, func(backend *hydro.LocalPubSub, c context.Context, channel, message string) error {
		return backend.Publish(c, "", channel, message) // The database is not needed for local pub/sub, but for other production implementations
	})
}
