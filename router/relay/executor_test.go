package relay_test

import (
	"testing"

	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	"github.com/pg-sharding/spqr/router/relay"
	"github.com/stretchr/testify/assert"

	mockcmgr "github.com/pg-sharding/spqr/router/mock/poolmgr"
	"go.uber.org/mock/gomock"

	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockqr "github.com/pg-sharding/spqr/router/mock/qrouter"
)

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
	cl.EXPECT().ReplyCommandComplete("COMMIT").Times(1)

	err := rst.QueryExecutor().ExecCommit(rst, "COMMIT")

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
	cl.EXPECT().ReplyCommandComplete("ROLLBACK").Times(1)

	err := rst.QueryExecutor().ExecRollback(rst, "ROLLBACK")

	assert.Nil(err)
}
