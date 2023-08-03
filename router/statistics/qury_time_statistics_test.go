package statistics_test

import (
	"math"
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

	stat1 := statistics.GetClientTimeStatistics(statistics.Router, "test")
	stat2 := statistics.GetClientTimeStatistics(statistics.Shard, "test")

	assert.Equal(4.0, stat1.Quantile(0.5))
	assert.Equal(3.0, stat2.Quantile(0.5))
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

	stat1 := statistics.GetClientTimeStatistics(statistics.Router, "first")
	stat2 := statistics.GetClientTimeStatistics(statistics.Shard, "first")

	stat3 := statistics.GetClientTimeStatistics(statistics.Router, "second")
	stat4 := statistics.GetClientTimeStatistics(statistics.Shard, "second")

	assert.Equal(2.5, stat1.Quantile(0.5))
	assert.Equal(1.0, stat2.Quantile(0.5))

	assert.Equal(7.0, stat3.Quantile(0.5))
	assert.Equal(1.5, stat4.Quantile(0.5))
}

func TestNoStatisticsForMisingUser(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})

	stat := statistics.GetClientTimeStatistics(statistics.Router, "missing")

	assert.True(math.IsNaN(stat.Quantile(0.5)))
}

func TestNoStatisticsWhenNotNeeded(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{})
	tim := time.Now()

	statistics.RecordStartTime(statistics.Router, tim, "useless")
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), "useless")
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), "useless")

	stat := statistics.GetClientTimeStatistics(statistics.Router, "useless")

	assert.True(math.IsNaN(stat.Quantile(0.5)))
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

				stat := statistics.GetClientTimeStatistics(statistics.Router, "thread")
				stat.Quantile(0.99)
				stat.Quantile(0.9)
				stat.Quantile(0.8)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	assert.True(true)
}
