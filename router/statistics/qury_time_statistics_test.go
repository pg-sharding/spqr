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

	statistics.RecordStartTime(statistics.Router, tim, 144)
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), 144)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), 144)

	statistics.RecordStartTime(statistics.Router, tim, 144)
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), 144)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*3), 144)

	statistics.RecordStartTime(statistics.Router, tim, 144)
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), 144)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*5), 144)

	statistics.RecordStartTime(statistics.Router, tim, 144)
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), 144)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*7), 144)

	assert.Equal(4.0, statistics.GetTimeQuantile(statistics.Router, 0.5, 144))
	assert.Equal(3.0, statistics.GetTimeQuantile(statistics.Shard, 0.5, 144))

	assert.Equal(4.0, statistics.GetTotalTimeQuantile(statistics.Router, 0.5))
	assert.Equal(3.0, statistics.GetTotalTimeQuantile(statistics.Shard, 0.5))
}

func TestStatisticsForDifferentUsers(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})
	tim := time.Now()

	statistics.RecordStartTime(statistics.Router, tim, 227)
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), 227)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), 227)

	statistics.RecordStartTime(statistics.Router, tim, 227)
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond*2), 227)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*3), 227)

	statistics.RecordStartTime(statistics.Router, tim, 229)
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond*5), 229)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*7), 229)

	statistics.RecordStartTime(statistics.Router, tim, 229)
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond*6), 229)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*7), 229)

	assert.Equal(2.5, statistics.GetTimeQuantile(statistics.Router, 0.5, 227))
	assert.Equal(1.0, statistics.GetTimeQuantile(statistics.Shard, 0.5, 227))

	assert.Equal(7.0, statistics.GetTimeQuantile(statistics.Router, 0.5, 229))
	assert.Equal(1.5, statistics.GetTimeQuantile(statistics.Shard, 0.5, 229))

	assert.Equal(5.0, statistics.GetTotalTimeQuantile(statistics.Router, 0.5))
	assert.Equal(1.0, statistics.GetTotalTimeQuantile(statistics.Shard, 0.5))
}

func TestNoStatisticsForMissingUser(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})

	assert.Equal(0.0, statistics.GetTimeQuantile(statistics.Router, 0.5, 148))
}

func TestNoStatisticsWhenNotNeeded(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{})
	tim := time.Now()

	statistics.RecordStartTime(statistics.Router, tim, 149)
	statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), 149)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), 149)

	assert.Equal(0.0, statistics.GetTimeQuantile(statistics.Router, 0.5, 149))
	assert.Equal(0.0, statistics.GetTotalTimeQuantile(statistics.Router, 0.5))
}

func TestCheckMultithreading(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			tim := time.Now()
			for range 1000 {
				statistics.RecordStartTime(statistics.Router, tim, 150)
				statistics.RecordStartTime(statistics.Shard, tim.Add(time.Millisecond), 150)
				statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), 150)

				statistics.GetTimeQuantile(statistics.Router, 0.5, 149)
				statistics.GetTimeQuantile(statistics.Router, 0.99, 149)
				statistics.GetTotalTimeQuantile(statistics.Router, 0.99)
				statistics.GetTotalTimeQuantile(statistics.Router, 0.99)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	assert.True(true)
}

func TestStatisticsInit(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})
	q := *(statistics.GetQuantiles())
	assert.Len(q, 1)
	assert.Equal(q[0], 0.5)

	assert.NoError(statistics.InitStatisticsStr([]string{"0.5", ".999"}))
	q = *(statistics.GetQuantiles())
	assert.Len(q, 2)
	assert.Equal(q[0], 0.5)
	assert.Equal(q[1], 0.999)

	assert.ErrorContains(statistics.InitStatisticsStr([]string{"erroneous_str"}), "could not parse time quantile to float")
}
