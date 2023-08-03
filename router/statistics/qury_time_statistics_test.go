package statistics_test

import (
	"sync"
	"testing"
	"time"

	"github.com/pg-sharding/spqr/router/statistics"
	"github.com/stretchr/testify/assert"
)

func TestStatisticsForOneUser(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})
	tim := time.Now()

	statistics.RecordStartTime(statistics.Router, tim, "test")
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), "test")
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), "test")

	statistics.RecordStartTime(statistics.Router, tim, "test")
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), "test")
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*3), "test")

	statistics.RecordStartTime(statistics.Router, tim, "test")
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), "test")
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*5), "test")

	statistics.RecordStartTime(statistics.Router, tim, "test")
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), "test")
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*7), "test")

	assert.Equal(4.0, statistics.GetTimeQuantile(statistics.Router, 0.5, "test"))
	assert.Equal(3.0, statistics.GetTimeQuantile(statistics.Shard, 0.5, "test"))
}

func TestStatisticsForDifferentUsers(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})
	tim := time.Now()

	statistics.RecordStartTime(statistics.Router, tim, "first")
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), "first")
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), "first")

	statistics.RecordStartTime(statistics.Router, tim, "first")
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond*2), "first")
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*3), "first")

	statistics.RecordStartTime(statistics.Router, tim, "second")
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond*5), "second")
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*7), "second")

	statistics.RecordStartTime(statistics.Router, tim, "second")
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond*6), "second")
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*7), "second")

	assert.Equal(2.5, statistics.GetTimeQuantile(statistics.Router, 0.5, "first"))
	assert.Equal(1.0, statistics.GetTimeQuantile(statistics.Shard, 0.5, "first"))

	assert.Equal(7.0, statistics.GetTimeQuantile(statistics.Router, 0.5, "second"))
	assert.Equal(1.5, statistics.GetTimeQuantile(statistics.Shard, 0.5, "second"))
}

func TestNoStatisticsForMisingUser(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})

	assert.Equal(0.0, statistics.GetTimeQuantile(statistics.Router, 0.5, "missing"))
}

func TestNoStatisticsWhenNotNeeded(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{})
	tim := time.Now()

	statistics.RecordStartTime(statistics.Router, tim, "useless")
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), "useless")
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), "useless")

	assert.Equal(0.0, statistics.GetTimeQuantile(statistics.Router, 0.5, "useless"))
}

func TestCheckMultithreading(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})

	var wg sync.WaitGroup
	for k := 0; k < 100; k++ {
		wg.Add(1)
		go func() {
			tim := time.Now()
			for i := 0; i < 1000; i++ {
				statistics.RecordStartTime(statistics.Router, tim, "thread")
				statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), "thread")
				statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), "thread")

				statistics.GetTimeQuantile(statistics.Router, 0.5, "useless")
				statistics.GetTimeQuantile(statistics.Router, 0.99, "useless")
			}
			wg.Done()
		}()
	}

	wg.Wait()

	assert.True(true)
}
