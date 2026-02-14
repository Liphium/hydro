package hydro_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Liphium/hydro"
	"github.com/stretchr/testify/assert"
)

func TestStringLengthBatcher(t *testing.T) {
	mutex := &sync.Mutex{}
	chunksGotten := []int{}

	collector := make(chan hydro.BatchRequest[string, int])
	batcher := hydro.Batcher[string, int]{
		Options: hydro.BatchOptions{
			BatchDuration: time.Millisecond * 100,
			MaxAmount:     10,
		},
		Collector: collector,
		BatchFunc: func(strings []string) (map[string]int, error) {
			mutex.Lock()
			chunksGotten = append(chunksGotten, len(strings))
			mutex.Unlock()

			final := map[string]int{}
			for _, s := range strings {
				final[s] = len(s)
			}
			return final, nil
		},
	}
	batcher.Init()

	t.Run("normal batching works", func(t *testing.T) {
		wg := &sync.WaitGroup{}
		for range 2 {
			wg.Go(func() {
				output, err := batcher.Do([]string{"test"})
				assert.Nil(t, err)
				assert.Equal(t, 4, output["test"])
			})
		}

		wg.Wait()
		assert.Equal(t, 1, len(chunksGotten))
		assert.Equal(t, 2, chunksGotten[0])
	})

	t.Run("batching with max amount exceeded", func(t *testing.T) {
		mutex.Lock()
		chunksGotten = []int{}
		mutex.Unlock()

		wg := &sync.WaitGroup{}
		// Send 25 requests, which should create 3 batches (10, 10, 5)
		for range 25 {
			wg.Go(func() {
				output, err := batcher.Do([]string{"hello"})
				assert.Nil(t, err)
				assert.Equal(t, 5, output["hello"])
			})
		}

		wg.Wait()

		assert.Equal(t, 3, len(chunksGotten))
		slices.Sort(chunksGotten)
		assert.Equal(t, []int{5, 10, 10}, chunksGotten)
	})

	t.Run("batching with max duration exceeded", func(t *testing.T) {
		mutex.Lock()
		chunksGotten = []int{}
		mutex.Unlock()

		wg := &sync.WaitGroup{}
		for i := range 3 {
			wg.Go(func() {
				// Sleep for index two, making the expected output for chunksGotten: [2, 1]
				if i == 2 {
					time.Sleep(200 * time.Millisecond)
				}

				output, err := batcher.Do([]string{"duration"})
				assert.Nil(t, err)
				assert.Equal(t, 8, output["duration"])
			})
		}

		wg.Wait()

		assert.Equal(t, 2, len(chunksGotten))
		slices.Sort(chunksGotten)
		assert.Equal(t, []int{1, 2}, chunksGotten)
	})

	t.Run("multiple items in single request", func(t *testing.T) {
		mutex.Lock()
		chunksGotten = []int{}
		mutex.Unlock()

		multipleStrings := []string{"one", "two", "three", "four"}
		output, err := batcher.Do(multipleStrings)

		assert.Nil(t, err)
		assert.Equal(t, 3, output["one"])
		assert.Equal(t, 3, output["two"])
		assert.Equal(t, 5, output["three"])
		assert.Equal(t, 4, output["four"])

		assert.Equal(t, 1, len(chunksGotten))
		assert.Equal(t, 4, chunksGotten[0])
	})
}
