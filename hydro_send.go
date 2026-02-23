package hydro

import "github.com/Liphium/neogate"

// Send an event to Hydro addresses on any server.
func (i *Instance[T, PS]) Send(targets []HydroAddress, event neogate.Event) {

	// 1. Sort the targets by server
	targetsPerServer := map[string][]HydroAdapter{}
	for _, target := range targets {
		targetsPerServer[target.Server] = append(targetsPerServer[target.Server], target.Adapter)
	}

	// 2. Send to the different targets in different goroutines with bundled packets
	for server, targets := range targetsPerServer {
		go func() {

			// If it's localhost, just receive using the current Hydro instance, otherwise call the endpoint on the server
			if server == ServerLocal {
				i.Receive(targets, event)
			} else {
				i.sendToRemote(server, targets, event)
			}
		}()
	}
}

func (i *Instance[T, PS]) sendToRemote(server string, ids []HydroAdapter, event neogate.Event) {
	// TODO: Implement
}
