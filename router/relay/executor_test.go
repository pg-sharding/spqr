package relay_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	mockpool "github.com/pg-sharding/spqr/pkg/mock/pool"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/session/sessiontest"
	"github.com/pg-sharding/spqr/router/relay"
	"github.com/stretchr/testify/assert"

	mockcmgr "github.com/pg-sharding/spqr/router/mock/poolmgr"
	"go.uber.org/mock/gomock"

	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockqr "github.com/pg-sharding/spqr/router/mock/qrouter"
)

func TestCleanupConnectionDiscardsPending(t *testing.T) {
	for _, sync := range []int64{0, 1} {
		t.Run(fmt.Sprintf("sync_%d", sync), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cl := mockcl.NewMockRouterClient(ctrl)
			pool := mockpool.NewMockMultiShardTSAPool(ctrl)
			sh := mockshard.NewMockShardHostInstance(ctrl)
			sh.EXPECT().Sync().Return(sync)
			if sync == 0 {
				sh.EXPECT().DataPending().Return(true)
			}
			sh.EXPECT().ShardKeyName().Return("sh1")
			pool.EXPECT().Discard(sh).Return(nil)

			executor := relay.NewQueryStateExecutor(nil, nil, nil, cl)
			assert.NoError(t, executor.CleanupConnection(pool, sh))
		})
	}
}

func TestCleanupConnectionDiscardsOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	cl := mockcl.NewMockRouterClient(ctrl)
	pool := mockpool.NewMockMultiShardTSAPool(ctrl)
	sh := mockshard.NewMockShardHostInstance(ctrl)
	rule := &config.FrontendRule{PoolRollback: true}
	cleanupErr := errors.New("rollback failed")
	sh.EXPECT().Sync().Return(int64(0))
	sh.EXPECT().DataPending().Return(false)
	cl.EXPECT().FindBoolGUC(session.SPQR_SESSION_CONNECTIONS_PIN).
		Return(sessiontest.MustFindBoolGUC(session.SPQR_SESSION_CONNECTIONS_PIN), nil)
	cl.EXPECT().ResolveVirtualBoolParam(session.SPQR_SESSION_CONNECTIONS_PIN, false).Return(false)
	cl.EXPECT().Rule().Return(rule)
	sh.EXPECT().Cleanup(rule).Return(cleanupErr)
	sh.EXPECT().ShardKeyName().Return("sh1")
	pool.EXPECT().Discard(sh).Return(nil)

	executor := relay.NewQueryStateExecutor(nil, nil, nil, cl)
	assert.ErrorIs(t, executor.CleanupConnection(pool, sh), cleanupErr)
}

func TestTxSimpleCommit(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	cl := mockcl.NewMockRouterClient(ctrl)
	cl.EXPECT().CleanupStatementSet().AnyTimes()
	qr := mockqr.NewMockQueryRouter(ctrl)

	mmgr := mockmgr.NewMockEntityMgr(ctrl)
	mmgr.EXPECT().DCStateKeeper().AnyTimes().Return(nil)
	qr.EXPECT().Mgr().Return(mmgr).AnyTimes()

	rst := relay.NewRelayState(qr, cl, cmngr)

	cmngr.EXPECT().ConnectionActive(gomock.Any()).Return(false)

	cl.EXPECT().CommitActiveSet().Times(1)

	err := rst.QueryExecutor().ExecCommit("COMMIT")

	assert.Nil(err)
}

func TestTxSimpleRollback(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	cl := mockcl.NewMockRouterClient(ctrl)

	cl.EXPECT().CleanupStatementSet().AnyTimes()

	qr := mockqr.NewMockQueryRouter(ctrl)

	mmgr := mockmgr.NewMockEntityMgr(ctrl)
	mmgr.EXPECT().DCStateKeeper().AnyTimes().Return(nil)
	qr.EXPECT().Mgr().Return(mmgr).AnyTimes()

	rst := relay.NewRelayState(qr, cl, cmngr)

	cmngr.EXPECT().ConnectionActive(gomock.Any()).Return(false)

	cl.EXPECT().Rollback().Times(1)

	err := rst.QueryExecutor().ExecRollback("ROLLBACK")

	assert.Nil(err)
}
