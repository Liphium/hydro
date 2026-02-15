package hydro

import (
	"fmt"
	"maps"
	"slices"
	"time"
)

type BatchOptions struct {
	BatchDuration time.Duration
	MaxAmount     int
}

type BatchOutput[I comparable, O any] struct {
	Err     error
	Outputs map[I]O
}

type BatchRequest[I comparable, O any] struct {
	Inputs    []I
	Collector chan BatchOutput[I, O]
}

type Batcher[I comparable, O any] struct {
	Options   BatchOptions
	Collector chan BatchRequest[I, O]
	BatchFunc func([]I) (map[I]O, error)
}

func (b *Batcher[I, O]) Init() {
	if b.Options.MaxAmount == 0 {
		Log.Fatal("ERROR: batcher: Batching options max amount can't be zero")
	}
	if b.Options.BatchDuration == 0 {
		Log.Fatal("ERROR: batcher: Batching duration can't be zero")
	}

	go func() {
		currentLength := 0
		currentRequests := []BatchRequest[I, O]{}
		var currentTimer *time.Timer = nil

		// Returns true when the final function call should be done
		handleRequest := func(rq BatchRequest[I, O]) bool {
			if currentLength+len(rq.Inputs) > b.Options.MaxAmount {
				left := b.Options.MaxAmount - currentLength
				currentLength += left

				// Add part of the current request to the current batch
				partCollector := make(chan BatchOutput[I, O], 1)
				currentRequests = append(currentRequests, BatchRequest[I, O]{
					Inputs:    rq.Inputs[:left],
					Collector: partCollector,
				})

				// Create a new request that processes the last of the inputs
				newRq := BatchRequest[I, O]{
					Inputs:    rq.Inputs[left:],
					Collector: make(chan BatchOutput[I, O], 1),
				}
				b.Collector <- newRq

				// Make a new goroutine that collects the final results
				go func() {
					part1 := <-partCollector
					part2 := <-newRq.Collector

					if part1.Err != nil || part2.Err != nil {
						rq.Collector <- BatchOutput[I, O]{
							Err: fmt.Errorf("an error occured in parts: part1=%v, part2=%v", part1.Err, part2.Err),
						}
					} else {

						// Combine the outputs
						maps.Copy(part1.Outputs, part2.Outputs)
						rq.Collector <- BatchOutput[I, O]{
							Outputs: part1.Outputs,
						}
					}
				}()
			} else {
				currentRequests = append(currentRequests, rq)
				currentLength += len(rq.Inputs)
			}

			// Create a new timer and return when the current batch isn't enough to statisfy the bounds
			if currentLength < b.Options.MaxAmount {
				currentTimer = time.NewTimer(b.Options.BatchDuration)
				return false
			}

			return true
		}

		doFinalRequest := func() {
			// Copy current inputs and empty
			copied := slices.Clone(currentRequests)
			currentRequests = []BatchRequest[I, O]{}
			currentLength = 0
			currentTimer = nil

			go func() {
				// Collect inputs
				inputs := []I{}
				for _, rq := range copied {
					inputs = append(inputs, rq.Inputs...)
				}

				// Call the batch function with all of the inputs
				output, err := b.BatchFunc(inputs)
				if err != nil {

					// Send an error to all of the output channels
					for _, rq := range copied {
						rq.Collector <- BatchOutput[I, O]{
							Err: fmt.Errorf("error during batch request: %v", err),
						}
					}
					return
				}

				// Send the outputs to everyone (it's a map pointer that's meant to be read-only so no problem in the future PLEASE)
				res := BatchOutput[I, O]{
					Outputs: output,
				}
				for _, rq := range copied {
					rq.Collector <- res
				}
			}()
		}

		// The actual loop for the batcher
		for {

			// If there is a timer, wait for the timer to run out or new requests
			if currentTimer != nil {
				select {
				case rq := <-b.Collector:
					if handleRequest(rq) {
						doFinalRequest()
					}
					continue
				case <-currentTimer.C:
					doFinalRequest()
					continue
				}
			}

			// If there is no timer, just wait for a new request and process it when there
			rq := <-b.Collector
			if handleRequest(rq) {
				doFinalRequest()
			}
		}
	}()
}

// Submit a task to the batcher
func (b *Batcher[I, O]) Do(inputs []I) (map[I]O, error) {
	collector := make(chan BatchOutput[I, O], 1)
	b.Collector <- BatchRequest[I, O]{
		Inputs:    inputs,
		Collector: collector,
	}
	result := <-collector
	return result.Outputs, result.Err
}
