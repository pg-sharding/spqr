package pool_test

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	mockinst "github.com/pg-sharding/spqr/pkg/mock/conn"
	mockpool "github.com/pg-sharding/spqr/pkg/mock/pool"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"
)

func TestDbPoolOrderCaching(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)

	underyling_pool := mockpool.NewMockMultiShardPool(ctrl)

	key := kr.ShardKey{
		Name: "sh1",
	}

	clId := uint(1)

	dbpool := pool.NewDBPoolFromMultiPool(map[string]*config.Shard{
		key.Name: {
			RawHosts: []string{
				"h1",
				"h2",
				"h3",
			},
		},
	}, &startup.StartupParams{}, underyling_pool, time.Hour)

	ins1 := mockinst.NewMockDBInstance(ctrl)
	ins1.EXPECT().Hostname().AnyTimes().Return("h1")
	ins1.EXPECT().AvailabilityZone().AnyTimes().Return("")

	ins2 := mockinst.NewMockDBInstance(ctrl)
	ins2.EXPECT().Hostname().AnyTimes().Return("h2")
	ins2.EXPECT().AvailabilityZone().AnyTimes().Return("")

	ins3 := mockinst.NewMockDBInstance(ctrl)
	ins3.EXPECT().Hostname().AnyTimes().Return("h3")
	ins3.EXPECT().AvailabilityZone().AnyTimes().Return("")

	h1 := mockshard.NewMockShard(ctrl)
	h1.EXPECT().Instance().AnyTimes().Return(ins1)

	h2 := mockshard.NewMockShard(ctrl)
	h2.EXPECT().Instance().AnyTimes().Return(ins2)

	h3 := mockshard.NewMockShard(ctrl)
	h3.EXPECT().Instance().AnyTimes().Return(ins3)

	h1.EXPECT().ID().AnyTimes().Return(uint(1))

	h2.EXPECT().ID().AnyTimes().Return(uint(2))

	h3.EXPECT().ID().AnyTimes().Return(uint(3))

	hs := []*mockshard.MockShard{
		h1, h2, h3,
	}

	underyling_pool.EXPECT().ConnectionHost(clId, key, config.Host{Address: "h1"}).Times(1).Return(h1, nil)
	underyling_pool.EXPECT().ConnectionHost(clId, key, config.Host{Address: "h2"}).Times(1).Return(h2, nil)
	underyling_pool.EXPECT().ConnectionHost(clId, key, config.Host{Address: "h3"}).Times(1).Return(h3, nil)
	underyling_pool.EXPECT().ID().Return(uint(17)).AnyTimes()

	for ind, h := range hs {

		if ind < 2 {
			underyling_pool.EXPECT().Put(h).Return(nil)

			h.EXPECT().Sync().Return(int64(0))

			h.EXPECT().TxStatus().Return(txstatus.TXIDLE)
		}

		h.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Times(1)
		h.EXPECT().Receive().Return(&pgproto3.RowDescription{}, nil)
		if ind == 2 {
			h.EXPECT().Receive().DoAndReturn(func() (pgproto3.BackendMessage, error) {

				return &pgproto3.DataRow{
					Values: [][]byte{
						[]byte("off"),
					},
				}, nil
			})
		} else {
			h.EXPECT().Receive().DoAndReturn(func() (pgproto3.BackendMessage, error) {
				return &pgproto3.DataRow{
					Values: [][]byte{
						[]byte("on"),
					},
				}, nil
			})
		}

		h.EXPECT().Receive().Return(&pgproto3.CommandComplete{}, nil)

		h.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}, nil)
	}

	sh, err := dbpool.ConnectionWithTSA(clId, key, config.TargetSessionAttrsRW)

	assert.Equal(sh.Instance().Hostname(), h3.Instance().Hostname())
	assert.Equal(sh.Instance().AvailabilityZone(), h3.Instance().AvailabilityZone())

	assert.NoError(err)

	/* next time expect only one call */
	underyling_pool.EXPECT().ConnectionHost(clId, key, config.Host{Address: "h3"}).Times(1).Return(h3, nil)

	sh, err = dbpool.ConnectionWithTSA(clId, key, config.TargetSessionAttrsRW)

	assert.Equal(sh.Instance().Hostname(), h3.Instance().Hostname())
	assert.Equal(sh.Instance().AvailabilityZone(), h3.Instance().AvailabilityZone())

	assert.NoError(err)
}

func TestDbPoolRaces(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)

	sz := 50

	mp := map[string]map[string][]shard.Shard{}
	var mu sync.Mutex

	hosts := []string{
		"h1",
		"h2",
		"h3",
	}

	shrds := []string{
		"sh1",
		"sh2",
		"sh3",
	}

	for i, shname := range shrds {
		mp[shname] = map[string][]shard.Shard{}

		for hi, hst := range hosts {

			for j := 0; j < sz; j++ {
				sh := mockshard.NewMockShard(ctrl)

				ins1 := mockinst.NewMockDBInstance(ctrl)
				ins1.EXPECT().Hostname().Return(hst).AnyTimes()
				ins1.EXPECT().AvailabilityZone().Return("").AnyTimes()

				sh.EXPECT().Send(gomock.Any()).AnyTimes()

				sh.EXPECT().Sync().Return(int64(0)).AnyTimes()
				sh.EXPECT().TxStatus().Return(txstatus.TXIDLE).AnyTimes()

				sh.EXPECT().ShardKeyName().Return(shname).AnyTimes()

				counter := 0

				sh.EXPECT().Receive().DoAndReturn(func() (pgproto3.BackendMessage, error) {
					if counter == 0 {
						counter = 1
						if rand.Intn(100)%2 == 0 {
							return &pgproto3.DataRow{
								Values: [][]byte{
									{'o', 'n'},
								},
							}, nil
						} else {
							return &pgproto3.DataRow{
								Values: [][]byte{
									{'o', 'f', 'f'},
								},
							}, nil
						}
					}
					counter = 0
					return &pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}, nil
				}).AnyTimes()

				sh.EXPECT().InstanceHostname().Return(hst).AnyTimes()

				sh.EXPECT().ID().Return(uint((3*i+hi)*sz + j)).AnyTimes()
				sh.EXPECT().Instance().Return(ins1).AnyTimes()
				mp[shname][hst] = append(mp[shname][hst], sh)
			}
		}
	}

	cfg := map[string]*config.Shard{}

	for _, sh := range shrds {
		cfg[sh] = &config.Shard{
			RawHosts: hosts,
		}
	}

	dbpool := pool.NewDBPoolWithAllocator(cfg, &startup.StartupParams{}, func(shardKey kr.ShardKey, host config.Host, rule *config.BackendRule) (shard.Shard, error) {
		mu.Lock()
		defer mu.Unlock()

		if len(mp[shardKey.Name][host.Address]) == 0 {
			panic("exeeded!")
		}
		var sh shard.Shard
		sh, mp[shardKey.Name][host.Address] = mp[shardKey.Name][host.Address][0], mp[shardKey.Name][host.Address][1:]

		spqrlog.Zero.Debug().Str("shard", shardKey.Name).Str("host", host.Address).Uint("id", sh.ID()).Msg("test allocation")

		return sh, nil
	})

	dbpool.SetRule(&config.BackendRule{
		ConnectionLimit: sz,
	})

	sem := semaphore.NewWeighted(25)

	for i := range 10 {
		for range 200 {
			assert.NoError(sem.Acquire(context.TODO(), 1))

			go func() {
				defer sem.Release(1)

				sh, err := dbpool.ConnectionWithTSA(uint(i), kr.ShardKey{Name: shrds[i%3]}, config.TargetSessionAttrsPS)
				assert.NoError(err)
				err = dbpool.Put(sh)
				assert.NoError(err)
			}()
		}

	}
}

func TestDbPoolReadOnlyOrderDistribution(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)

	underyling_pool := mockpool.NewMockMultiShardPool(ctrl)

	key := kr.ShardKey{
		Name: "sh1",
	}

	clId := uint(1)

	dbpool := pool.NewDBPoolFromMultiPool(map[string]*config.Shard{
		key.Name: {
			RawHosts: []string{
				"h1",
				"h2",
				"h3",
			},
		},
	}, &startup.StartupParams{}, underyling_pool, time.Hour)

	ins1 := mockinst.NewMockDBInstance(ctrl)
	ins1.EXPECT().Hostname().AnyTimes().Return("h1")
	ins1.EXPECT().AvailabilityZone().AnyTimes().Return("")

	ins2 := mockinst.NewMockDBInstance(ctrl)
	ins2.EXPECT().Hostname().AnyTimes().Return("h2")
	ins2.EXPECT().AvailabilityZone().AnyTimes().Return("")

	ins3 := mockinst.NewMockDBInstance(ctrl)
	ins3.EXPECT().Hostname().AnyTimes().Return("h3")
	ins3.EXPECT().AvailabilityZone().AnyTimes().Return("")

	h1 := mockshard.NewMockShard(ctrl)
	h1.EXPECT().Instance().AnyTimes().Return(ins1)

	h2 := mockshard.NewMockShard(ctrl)
	h2.EXPECT().Instance().AnyTimes().Return(ins2)

	h3 := mockshard.NewMockShard(ctrl)
	h3.EXPECT().Instance().AnyTimes().Return(ins3)

	h1.EXPECT().ID().AnyTimes().Return(uint(1))

	h2.EXPECT().ID().AnyTimes().Return(uint(2))

	h3.EXPECT().ID().AnyTimes().Return(uint(3))

	hs := []*mockshard.MockShard{
		h1, h2, h3,
	}

	underyling_pool.EXPECT().ConnectionHost(clId, key, config.Host{Address: "h1"}).AnyTimes().Return(h1, nil)
	underyling_pool.EXPECT().ConnectionHost(clId, key, config.Host{Address: "h2"}).AnyTimes().Return(h2, nil)
	underyling_pool.EXPECT().ConnectionHost(clId, key, config.Host{Address: "h3"}).Times(1).Return(h3, nil)

	for ind, h := range hs {

		if ind < 2 {
			underyling_pool.EXPECT().Put(h).Return(nil)

			h.EXPECT().Sync().Return(int64(0))

			h.EXPECT().TxStatus().Return(txstatus.TXIDLE)
		}

		h.EXPECT().Send(&pgproto3.Query{String: "SHOW transaction_read_only"}).Times(1)
		h.EXPECT().Receive().Return(&pgproto3.RowDescription{}, nil)
		if ind == 2 {
			h.EXPECT().Receive().DoAndReturn(func() (pgproto3.BackendMessage, error) {

				return &pgproto3.DataRow{
					Values: [][]byte{
						[]byte("off"),
					},
				}, nil
			})
		} else {
			h.EXPECT().Receive().DoAndReturn(func() (pgproto3.BackendMessage, error) {
				return &pgproto3.DataRow{
					Values: [][]byte{
						[]byte("on"),
					},
				}, nil
			})
		}

		h.EXPECT().Receive().Return(&pgproto3.CommandComplete{}, nil)

		h.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}, nil)
	}

	sh, err := dbpool.ConnectionWithTSA(clId, key, config.TargetSessionAttrsRW)

	assert.Equal(sh.Instance().Hostname(), h3.Instance().Hostname())
	assert.Equal(sh.Instance().AvailabilityZone(), h3.Instance().AvailabilityZone())

	assert.NoError(err)

	underyling_pool.EXPECT().ConnectionHost(clId, key, config.Host{Address: "h3"}).MaxTimes(1).Return(h3, nil)

	underyling_pool.EXPECT().Put(h3).Return(nil).MaxTimes(1)

	h3.EXPECT().Sync().Return(int64(0)).MaxTimes(1)

	h3.EXPECT().TxStatus().Return(txstatus.TXIDLE).MaxTimes(1)

	repeattimes := 1000

	cnth1 := 0
	cnth2 := 0

	dbpool.ShuffleHosts = true

	for range repeattimes {
		sh, err = dbpool.ConnectionWithTSA(clId, key, config.TargetSessionAttrsRO)

		// assert.NotEqual(sh, h3)

		if sh == h1 {
			cnth1++
		} else {
			cnth2++
		}

		assert.NoError(err)
	}

	diff := cnth1 - cnth2
	if diff < 0 {
		diff = -diff
	}

	assert.Less(diff, 90)
	assert.Equal(repeattimes, cnth1+cnth2)
}

func TestBuildHostOrder(t *testing.T) {
	ctrl := gomock.NewController(t)

	underyling_pool := mockpool.NewMockMultiShardPool(ctrl)

	key := kr.ShardKey{
		Name: "sh1",
	}

	dbpool := pool.NewDBPoolFromMultiPool(map[string]*config.Shard{
		key.Name: {
			RawHosts: []string{
				"sas-123.db.yandex.net:6432:sas",
				"sas-234.db.yandex.net:6432:sas",
				"vla-123.db.yandex.net:6432:vla",
				"vla-234.db.yandex.net:6432:vla",
				"klg-123.db.yandex.net:6432:klg",
				"klg-234.db.yandex.net:6432:klg",
			},
		},
	}, &startup.StartupParams{}, underyling_pool, time.Hour)

	tests := []struct {
		name               string
		shardKey           kr.ShardKey
		targetSessionAttrs tsa.TSA
		shuffleHosts       bool
		preferAZ           string
		expectedHosts      []string
	}{
		{
			name:               "No shuffle, no preferred AZ",
			shardKey:           kr.ShardKey{Name: "sh1"},
			targetSessionAttrs: config.TargetSessionAttrsAny,
			shuffleHosts:       false,
			preferAZ:           "",
			expectedHosts: []string{
				"sas-123.db.yandex.net:6432",
				"sas-234.db.yandex.net:6432",
				"vla-123.db.yandex.net:6432",
				"vla-234.db.yandex.net:6432",
				"klg-123.db.yandex.net:6432",
				"klg-234.db.yandex.net:6432",
			},
		},
		{
			name:               "Shuffle hosts",
			shardKey:           kr.ShardKey{Name: "sh1"},
			targetSessionAttrs: config.TargetSessionAttrsAny,
			shuffleHosts:       true,
			preferAZ:           "",
			expectedHosts: []string{
				"sas-123.db.yandex.net:6432",
				"sas-234.db.yandex.net:6432",
				"vla-123.db.yandex.net:6432",
				"vla-234.db.yandex.net:6432",
				"klg-123.db.yandex.net:6432",
				"klg-234.db.yandex.net:6432",
			},
		},
		{
			name:               "Preferred AZ",
			shardKey:           kr.ShardKey{Name: "sh1"},
			targetSessionAttrs: config.TargetSessionAttrsAny,
			shuffleHosts:       false,
			preferAZ:           "klg",
			expectedHosts: []string{
				"klg-234.db.yandex.net:6432",
				"klg-123.db.yandex.net:6432",
				"sas-123.db.yandex.net:6432",
				"sas-234.db.yandex.net:6432",
				"vla-123.db.yandex.net:6432",
				"vla-234.db.yandex.net:6432",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbpool.ShuffleHosts = tt.shuffleHosts
			dbpool.PreferAZ = tt.preferAZ

			hostOrder, err := dbpool.BuildHostOrder(tt.shardKey, tt.targetSessionAttrs)
			assert.NoError(t, err)

			var hostAddresses []string
			for _, host := range hostOrder {
				hostAddresses = append(hostAddresses, host.Address)
			}

			if tt.shuffleHosts {
				assert.ElementsMatch(t, tt.expectedHosts, hostAddresses)
			} else {
				assert.Equal(t, tt.expectedHosts, hostAddresses)
			}
		})
	}
}
