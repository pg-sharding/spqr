package route

import (
	"sync"
	"testing"

	mock "github.com/pg-sharding/spqr/pkg/mock/pool"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestSetParams(t *testing.T) {
	t.Run("Set params and cachedParams", func(t *testing.T) {
		r := &Route{
			mu:           sync.Mutex{},
			cachedParams: false,
			params:       nil,
		}
		input := shard.ParameterSet{
			"host": "localhost",
			"port": "test",
		}
		r.SetParams(input)

		assert.True(t, r.cachedParams, "cachedParams must be true after setParams")
		assert.Equal(t, input, r.params, "params must match the input parameters")
	})

	t.Run("Overwrites old parameters", func(t *testing.T) {
		r := &Route{
			cachedParams: true,
			params:       shard.ParameterSet{"old": "value"},
		}
		newParams := shard.ParameterSet{"new": "value"}

		r.SetParams(newParams)

		assert.Equal(t, newParams, r.params, "parameters must be overwritten")
		assert.True(t, r.cachedParams, "cachedParams must remain true")
	})

	t.Run("Works with an empty parameter", func(t *testing.T) {
		r := &Route{}
		empty := shard.ParameterSet{}
		r.SetParams(empty)

		assert.True(t, r.cachedParams, "cachedParams must be true even for an empty parameter")
		assert.Empty(t, r.params, "params must be empty")
	})
}

func TestParams_EmptyShardMapping(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := mock.NewMockMultiShardTSAPool(ctrl)
	// ShardMapping returns an empty topology — no ConnectionWithTSA call should occur.
	mockPool.EXPECT().ShardMapping().Return(topology.TopMgrFromMap(map[string]*topology.DataShard{}))

	r := &Route{
		mShardPool: mockPool,
	}

	ps, err := r.Params()

	assert.Empty(t, ps)
	assert.Error(t, err)
	var spqrErr *spqrerror.SpqrError
	assert.ErrorAs(t, err, &spqrErr)
	assert.Equal(t, spqrerror.SPQR_NO_DATASHARD, spqrErr.ErrorCode)
}
