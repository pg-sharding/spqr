package statistics_test

import (
	"sync"
	"testing"
	"time"

	"github.com/caio/go-tdigest"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/statistics"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	mockcl "github.com/pg-sharding/spqr/router/mock/client"
)

func genTestClient(t *testing.T, tim time.Time) client.RouterClient {
	tinit := tim

	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	td1, _ := tdigest.New()
	td2, _ := tdigest.New()
	tds := map[statistics.StatisticsType]*tdigest.TDigest{
		statistics.StatisticsTypeRouter: td1,
		statistics.StatisticsTypeShard:  td2,
	}

	sttime := &statistics.StartTimes{
		RouterStart: tinit,
		ShardStart:  tinit,
	}

	ca.EXPECT().RecordStartTime(gomock.Any(), gomock.Any()).DoAndReturn(
		func(st statistics.StatisticsType, tt time.Time) {
			if st == statistics.StatisticsTypeRouter {
				sttime.RouterStart = tt
			} else {
				sttime.ShardStart = tt
			}
		},
	).AnyTimes()
	ca.EXPECT().Add(gomock.Any(), gomock.Any()).Do(func(st statistics.StatisticsType, value float64) {
		tds[st].Add(value)
	}).AnyTimes()
	ca.EXPECT().GetTimeQuantile(gomock.Any(), gomock.Any()).DoAndReturn(
		func(st statistics.StatisticsType, q float64) float64 {
			return tds[st].Quantile(q)
		}).AnyTimes()

	ca.EXPECT().GetTimeData().Return(sttime).AnyTimes()
	return ca
}

func TestStatisticsForOneUser(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})
	tim := time.Unix(11, 0)
	ca := genTestClient(t, tim)

	statistics.RecordStartTime(statistics.StatisticsTypeRouter, tim, ca)
	statistics.RecordStartTime(statistics.StatisticsTypeShard, tim.Add(time.Millisecond), ca)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), ca)

	statistics.RecordStartTime(statistics.StatisticsTypeRouter, tim, ca)
	statistics.RecordStartTime(statistics.StatisticsTypeShard, tim.Add(time.Millisecond), ca)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*3), ca)

	statistics.RecordStartTime(statistics.StatisticsTypeRouter, tim, ca)
	statistics.RecordStartTime(statistics.StatisticsTypeShard, tim.Add(time.Millisecond), ca)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*5), ca)

	statistics.RecordStartTime(statistics.StatisticsTypeRouter, tim, ca)
	statistics.RecordStartTime(statistics.StatisticsTypeShard, tim.Add(time.Millisecond), ca)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*7), ca)

	assert.Equal(4.0, statistics.GetTimeQuantile(statistics.StatisticsTypeRouter, 0.5, ca))
	assert.Equal(3.0, statistics.GetTimeQuantile(statistics.StatisticsTypeShard, 0.5, ca))

	assert.Equal(4.0, statistics.GetTotalTimeQuantile(statistics.StatisticsTypeRouter, 0.5))
	assert.Equal(3.0, statistics.GetTotalTimeQuantile(statistics.StatisticsTypeShard, 0.5))
}

func TestStatisticsForDifferentUsers(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})

	tim := time.Unix(11, 0)

	ca := genTestClient(t, tim)
	cb := genTestClient(t, tim)

	statistics.RecordStartTime(statistics.StatisticsTypeRouter, tim, ca)
	statistics.RecordStartTime(statistics.StatisticsTypeShard, tim.Add(time.Millisecond), ca)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), ca)

	statistics.RecordStartTime(statistics.StatisticsTypeRouter, tim, ca)
	statistics.RecordStartTime(statistics.StatisticsTypeShard, tim.Add(time.Millisecond*2), ca)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*3), ca)

	statistics.RecordStartTime(statistics.StatisticsTypeRouter, tim, cb)
	statistics.RecordStartTime(statistics.StatisticsTypeShard, tim.Add(time.Millisecond*5), cb)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*7), cb)

	statistics.RecordStartTime(statistics.StatisticsTypeRouter, tim, cb)
	statistics.RecordStartTime(statistics.StatisticsTypeShard, tim.Add(time.Millisecond*6), cb)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*7), cb)

	assert.Equal(2.5, statistics.GetTimeQuantile(statistics.StatisticsTypeRouter, 0.5, ca))
	assert.Equal(1.0, statistics.GetTimeQuantile(statistics.StatisticsTypeShard, 0.5, ca))

	assert.Equal(7.0, statistics.GetTimeQuantile(statistics.StatisticsTypeRouter, 0.5, cb))
	assert.Equal(1.5, statistics.GetTimeQuantile(statistics.StatisticsTypeShard, 0.5, cb))

	assert.Equal(5.0, statistics.GetTotalTimeQuantile(statistics.StatisticsTypeRouter, 0.5))
	assert.Equal(1.0, statistics.GetTotalTimeQuantile(statistics.StatisticsTypeShard, 0.5))
}

func TestNoStatisticsWhenNotNeeded(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{})
	tim := time.Now()

	ca := genTestClient(t, tim)

	statistics.RecordStartTime(statistics.StatisticsTypeRouter, tim, ca)
	statistics.RecordStartTime(statistics.StatisticsTypeShard, tim.Add(time.Millisecond), ca)
	statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), ca)

	assert.Equal(0.0, statistics.GetTimeQuantile(statistics.StatisticsTypeRouter, 0.5, ca))
	assert.Equal(0.0, statistics.GetTotalTimeQuantile(statistics.StatisticsTypeRouter, 0.5))
}

func TestCheckMultithreading(t *testing.T) {
	assert := assert.New(t)

	statistics.InitStatistics([]float64{0.5})

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			tim := time.Now()

			ca := genTestClient(t, tim)
			cb := genTestClient(t, tim)

			for range 1000 {
				statistics.RecordStartTime(statistics.StatisticsTypeRouter, tim, ca)
				statistics.RecordStartTime(statistics.StatisticsTypeShard, tim.Add(time.Millisecond), ca)
				statistics.RecordFinishedTransaction(tim.Add(time.Millisecond*2), ca)

				statistics.GetTimeQuantile(statistics.StatisticsTypeRouter, 0.5, cb)
				statistics.GetTimeQuantile(statistics.StatisticsTypeRouter, 0.99, cb)
				statistics.GetTotalTimeQuantile(statistics.StatisticsTypeRouter, 0.99)
				statistics.GetTotalTimeQuantile(statistics.StatisticsTypeRouter, 0.99)
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
