package poolmgr_test

import (
	"errors"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockmgr "github.com/pg-sharding/spqr/router/mock/poolmgr"
	mocksrv "github.com/pg-sharding/spqr/router/mock/server"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestUnrouteAfterCleanupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	cl := mockcl.NewMockRouterClient(ctrl)
	serv := mocksrv.NewMockServer(ctrl)
	mgr := mockmgr.NewMockGangMgr(ctrl)
	first := mockshard.NewMockShardHostInstance(ctrl)
	second := mockshard.NewMockShardHostInstance(ctrl)
	keys := []kr.ShardKey{{Name: "sh1"}, {Name: "sh2"}}
	cleanupErr := errors.New("shard connection is not synchronized on cleanup")

	cl.EXPECT().Server().Return(serv)
	cl.EXPECT().ID().AnyTimes()
	cl.EXPECT().Route().Return(&route.Route{}).Times(2)
	serv.EXPECT().TxStatus().Return(txstatus.TXACT)
	serv.EXPECT().Cleanup(&config.FrontendRule{PoolRollback: true}).Return(cleanupErr)
	serv.EXPECT().UnRouteShard(keys[0]).Return(first, nil)
	mgr.EXPECT().CleanupConnection(nil, first).Return(errors.New("discard failed"))
	serv.EXPECT().UnRouteShard(keys[1]).Return(second, nil)
	mgr.EXPECT().CleanupConnection(nil, second).Return(nil)
	serv.EXPECT().Datashards().Return(nil)
	cl.EXPECT().Unroute().Return(nil)

	assert.ErrorIs(t, poolmgr.UnrouteCommon(mgr, cl, keys), cleanupErr)
}
