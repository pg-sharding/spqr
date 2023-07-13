package statistics_test

import (
	"math"
	"testing"
	"time"

	"github.com/pg-sharding/spqr/router/statistics"
	"github.com/stretchr/testify/assert"
)

func TestStatisticsForOneUser(t *testing.T) {
	//init test data
	assert := assert.New(t)

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

	assert.Equal(4e+03, stat1.Quantile(0.5))
	assert.Equal(3e+03, stat2.Quantile(0.5))
}

func TestStatisticsForDifferentUsers(t *testing.T) {
	assert := assert.New(t)

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

	assert.Equal(2.5e+03, stat1.Quantile(0.5))
	assert.Equal(1e+03, stat2.Quantile(0.5))

	assert.Equal(7e+03, stat3.Quantile(0.5))
	assert.Equal(1.5e+03, stat4.Quantile(0.5))
}

func TestNoStatisticsForMisingUser(t *testing.T) {
	assert := assert.New(t)

	stat := statistics.GetClientTimeStatistics(statistics.Router, "missing")

	assert.True(math.IsNaN(stat.Quantile(0.5)))
}
