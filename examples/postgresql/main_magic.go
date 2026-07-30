//go:build !release

package main

import (
	"examples/simple/starter"

	"github.com/Liphium/magic/v3"
)

func main() {
	magic.Start(starter.BuildMagicConfig())
}
