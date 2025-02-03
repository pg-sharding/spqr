package relay_test

import (
	"testing"

	"github.com/pg-sharding/spqr/router/relay"
	"github.com/stretchr/testify/assert"

	"github.com/golang/mock/gomock"
	mockcmgr "github.com/pg-sharding/spqr/router/mock/poolmgr"

	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockqr "github.com/pg-sharding/spqr/router/mock/qrouter"
)

func TestTxSimpleCommit(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	cl := mockcl.NewMockRouterClient(ctrl)
	cl.EXPECT().CleanupLocalSet().AnyTimes()
	qr := mockqr.NewMockQueryRouter(ctrl)

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

	cl.EXPECT().CleanupLocalSet().AnyTimes()

	qr := mockqr.NewMockQueryRouter(ctrl)

	rst := relay.NewRelayState(qr, cl, cmngr)

	cmngr.EXPECT().ConnectionActive(gomock.Any()).Return(false)

	cl.EXPECT().Rollback().Times(1)
	cl.EXPECT().ReplyCommandComplete("ROLLBACK").Times(1)

	err := rst.QueryExecutor().ExecRollback(rst, "ROLLBACK")

	assert.Nil(err)
}
