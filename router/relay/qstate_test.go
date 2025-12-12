package relay

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	distribution "github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/qdb"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockcmgr "github.com/pg-sharding/spqr/router/mock/poolmgr"
	mockqr "github.com/pg-sharding/spqr/router/mock/qrouter"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestAutoDistributionSetFail(t *testing.T) {

	is := assert.New(t)
	ctrl := gomock.NewController(t)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	client := mockcl.NewMockRouterClient(ctrl)
	client.EXPECT().CleanupStatementSet().AnyTimes()
	qr := mockqr.NewMockQueryRouter(ctrl)
	mmgr := mockmgr.NewMockEntityMgr(ctrl)
	qr.EXPECT().Mgr().Return(mmgr).AnyTimes()
	errNotFoundDistr := spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution \"%s\" not found", "distrNotFound")
	mmgr.EXPECT().GetDistribution(gomock.Any(), "distrNotFound").Return(nil, errNotFoundDistr)

	rst := RelayStateImpl{
		activeShards:        nil,
		msgBuf:              nil,
		qse:                 NewQueryStateExecutor(nil, client),
		Qr:                  qr,
		Cl:                  client,
		poolMgr:             cmngr,
		execute:             nil,
		executeMp:           map[string]func() error{},
		saveBind:            pgproto3.Bind{},
		saveBindNamed:       map[string]*pgproto3.Bind{},
		bindQueryPlan:       nil,
		bindQueryPlanMP:     map[string]plan.Plan{},
		savedPortalDesc:     map[string]*PortalDesc{},
		parseCache:          map[string]ParseCacheEntry{},
		unnamedPortalExists: false,
	}
	ctx := context.Background()
	err := rst.processSpqrHint(ctx, "__spqr__auto_distribution", "distrNotFound", false)
	is.Error(err)
}

func TestAutoDistributionSetSuccess(t *testing.T) {

	is := assert.New(t)
	ctrl := gomock.NewController(t)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	client := mockcl.NewMockRouterClient(ctrl)
	client.EXPECT().CleanupStatementSet().AnyTimes()
	qr := mockqr.NewMockQueryRouter(ctrl)
	mmgr := mockmgr.NewMockEntityMgr(ctrl)
	qr.EXPECT().Mgr().Return(mmgr).AnyTimes()
	existsDistr := distribution.NewDistribution("ds1", []string{qdb.ColumnTypeUinteger})
	mmgr.EXPECT().GetDistribution(gomock.Any(), "ds1").Return(existsDistr, nil)

	gomock.InOrder(
		client.EXPECT().SetParam("__spqr__auto_distribution", "ds1"),
		client.EXPECT().ReplyCommandComplete("SET"),
	)

	rst := RelayStateImpl{
		activeShards:        nil,
		msgBuf:              nil,
		qse:                 NewQueryStateExecutor(nil, client),
		Qr:                  qr,
		Cl:                  client,
		poolMgr:             cmngr,
		execute:             nil,
		executeMp:           map[string]func() error{},
		saveBind:            pgproto3.Bind{},
		saveBindNamed:       map[string]*pgproto3.Bind{},
		bindQueryPlan:       nil,
		bindQueryPlanMP:     map[string]plan.Plan{},
		savedPortalDesc:     map[string]*PortalDesc{},
		parseCache:          map[string]ParseCacheEntry{},
		unnamedPortalExists: false,
	}
	ctx := context.Background()
	err := rst.processSpqrHint(ctx, "__spqr__auto_distribution", "ds1", false)
	is.NoError(err)
}

func TestAutoDistributionSetReplicated(t *testing.T) {

	is := assert.New(t)
	ctrl := gomock.NewController(t)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	client := mockcl.NewMockRouterClient(ctrl)
	client.EXPECT().CleanupStatementSet().AnyTimes()
	qr := mockqr.NewMockQueryRouter(ctrl)
	mmgr := mockmgr.NewMockEntityMgr(ctrl)
	qr.EXPECT().Mgr().Return(mmgr).AnyTimes()
	mmgr.EXPECT().GetDistribution(gomock.Any(), "REPLICATED").Return(nil, fmt.Errorf("not found"))

	gomock.InOrder(
		client.EXPECT().SetParam("__spqr__auto_distribution", "REPLICATED"),
		client.EXPECT().ReplyCommandComplete("SET"),
	)

	rst := RelayStateImpl{
		activeShards:    nil,
		msgBuf:          nil,
		qse:             NewQueryStateExecutor(nil, client),
		Qr:              qr,
		Cl:              client,
		poolMgr:         cmngr,
		execute:         nil,
		executeMp:       map[string]func() error{},
		saveBind:        pgproto3.Bind{},
		saveBindNamed:   map[string]*pgproto3.Bind{},
		savedPortalDesc: map[string]*PortalDesc{},
		bindQueryPlan:   nil,
		bindQueryPlanMP: map[string]plan.Plan{},

		parseCache:          map[string]ParseCacheEntry{},
		unnamedPortalExists: false,
	}
	ctx := context.Background()
	err := rst.processSpqrHint(ctx, "__spqr__auto_distribution", "REPLICATED", false)
	is.NoError(err)
}
