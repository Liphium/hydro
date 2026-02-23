package hydro_test

import (
	"testing"

	"github.com/Liphium/hydro"
	"github.com/Liphium/hydro/hydrotest"
)

func TestPubSubPool(t *testing.T) {
	backend := hydro.NewLocalPubSub()

	hydrotest.TestSubWorker(t, func() hydro.ISubWorker {
		return hydro.NewPubSubPool(backend, hydro.PoolConfig{
			MaxAmountByWorker: 1, // To actually test it properly
		})
	}, backend.Publish)
}
