//go:build !release

package main

import (
	"examples/postgresql/starter"

	"github.com/Liphium/magic/v3"
)

func main() {
	magic.Start(starter.BuildMagicConfig())
}
