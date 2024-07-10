package pool_test

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pg-sharding/spqr/pkg/config"
	mockpool "github.com/pg-sharding/spqr/pkg/mock/pool/"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/startup"
	"github.com/stretchr/testify/assert"
)

func TestDbPoolCaching(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)

	underyling_pool := mockpool.NewMockMultiShardPool(ctrl)

	dbpool := pool.NewDBPoolFromMultiPool(map[string]*config.Shard{}, &startup.StartupParams{}, underyling_pool)

	sh, err := dbpool.Connection(1, kr.ShardKey{}, config.TargetSessionAttrsRO)

	assert.NoError(err)
}
