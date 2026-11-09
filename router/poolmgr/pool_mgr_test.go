package poolmgr_test

import (
	"errors"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockcmgr "github.com/pg-sharding/spqr/router/mock/poolmgr"
	mocksrv "github.com/pg-sharding/spqr/router/mock/server"
	"github.com/pg-sharding/spqr/router/poolmgr"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestUnrouteCommonDiscardsShardsAfterCleanupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	cl := mockcl.NewMockRouterClient(ctrl)
	serv := mocksrv.NewMockServer(ctrl)
	gang := mockcmgr.NewMockGangMgr(ctrl)
	sh1 := mockshard.NewMockShardHostInstance(ctrl)
	sh2 := mockshard.NewMockShardHostInstance(ctrl)
	cleanupErr := errors.New("shard connection is not synchronized on cleanup")
	discardErr := errors.New("discard failed")

	cl.EXPECT().Server().Return(serv)
	serv.EXPECT().TxStatus().Return(txstatus.TXACT)
	serv.EXPECT().Cleanup(&config.FrontendRule{PoolRollback: true}).Return(cleanupErr)
	serv.EXPECT().Datashards().Return([]shard.ShardHostInstance{sh1, sh2})
	sh1.EXPECT().SHKey().Return(kr.ShardKey{Name: "sh1"})
	sh2.EXPECT().SHKey().Return(kr.ShardKey{Name: "sh2"})
	serv.EXPECT().UnRouteShard(kr.ShardKey{Name: "sh1"}).Return(sh1, nil)
	serv.EXPECT().UnRouteShard(kr.ShardKey{Name: "sh2"}).Return(sh2, nil)
	gang.EXPECT().Discard(sh1).Return(discardErr)
	gang.EXPECT().Discard(sh2).Return(nil)
	cl.EXPECT().Unroute().Return(nil)

	err := poolmgr.UnrouteCommon(gang, cl, []kr.ShardKey{{Name: "sh1"}})
	require.ErrorIs(t, err, cleanupErr)
	require.ErrorIs(t, err, discardErr)
}
