package statistics_test

import (
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

	stat1 := statistics.GetUserTimeStatistics(statistics.Router, "test")
	stat2 := statistics.GetUserTimeStatistics(statistics.Shard, "test")

	assert.Equal(4e+06, stat1.Quantile(0.5))
	assert.Equal(3e+06, stat2.Quantile(0.5))
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

	stat1 := statistics.GetUserTimeStatistics(statistics.Router, "first")
	stat2 := statistics.GetUserTimeStatistics(statistics.Shard, "first")

	stat3 := statistics.GetUserTimeStatistics(statistics.Router, "second")
	stat4 := statistics.GetUserTimeStatistics(statistics.Shard, "second")

	assert.Equal(2.5e+06, stat1.Quantile(0.5))
	assert.Equal(1e+06, stat2.Quantile(0.5))

	assert.Equal(7e+06, stat3.Quantile(0.5))
	assert.Equal(1.5e+06, stat4.Quantile(0.5))
}

func TestNoStatisticsForMisingUser(t *testing.T) {
	assert := assert.New(t)

	stat := statistics.GetUserTimeStatistics(statistics.Router, "missing")

	assert.Nil(stat)
}
