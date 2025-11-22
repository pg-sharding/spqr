package frontend_test

import (
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	mocksh "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/frontend"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockqr "github.com/pg-sharding/spqr/router/mock/qrouter"
	mocksrv "github.com/pg-sharding/spqr/router/mock/server"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/pg-sharding/spqr/router/statistics"

	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	mockcmgr "github.com/pg-sharding/spqr/router/mock/poolmgr"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestFrontendSimpleEOF(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	cl := mockcl.NewMockRouterClient(ctrl)
	qr := mockqr.NewMockQueryRouter(ctrl)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	cl.EXPECT().Usr().AnyTimes().Return("user1")
	cl.EXPECT().DB().AnyTimes().Return("db1")
	cl.EXPECT().Close().Times(1)

	cl.EXPECT().Receive().Times(1).Return(nil, io.EOF)

	cmngr.EXPECT().UnRouteCB(gomock.Any(), gomock.Any()).Times(1)

	err := frontend.Frontend(qr, cl, cmngr, nil)

	assert.NoError(err, "")
}

func TestFrontendSimple(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	cl := mockcl.NewMockRouterClient(ctrl)
	srv := mocksrv.NewMockServer(ctrl)
	qr := mockqr.NewMockQueryRouter(ctrl)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	mmgr := mockmgr.NewMockEntityMgr(ctrl)

	frrule := &config.FrontendRule{
		DB:  "db1",
		Usr: "user1",
	}

	_ = statistics.InitStatisticsStr(nil)

	qr.EXPECT().Mgr().Return(mmgr).AnyTimes()

	srv.EXPECT().Datashards().AnyTimes().Return([]shard.ShardHostInstance{})
	srv.EXPECT().Name().AnyTimes().Return("serv1")

	srv.EXPECT().AddDataShard(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	cl.EXPECT().Server().AnyTimes().Return(srv)
	cl.EXPECT().MaintainParams().AnyTimes().Return(false)

	cl.EXPECT().CleanupStatementSet().AnyTimes()

	cl.EXPECT().ShowNoticeMsg().AnyTimes()
	cl.EXPECT().GetTsa().AnyTimes()

	cl.EXPECT().Usr().AnyTimes().Return("user1")
	cl.EXPECT().DB().AnyTimes().Return("db1")

	cl.EXPECT().BindParams().AnyTimes()

	cl.EXPECT().ShardingKey().AnyTimes()
	cl.EXPECT().EnhancedMultiShardProcessing().AnyTimes()
	cl.EXPECT().SetShardingKey(gomock.Any(), gomock.Any()).AnyTimes()

	cl.EXPECT().ID().AnyTimes()

	cl.EXPECT().Close().Times(1)
	cl.EXPECT().Rule().AnyTimes().Return(
		frrule,
	)

	cl.EXPECT().ReplyDebugNotice(gomock.Any()).AnyTimes().Return(nil)
	cl.EXPECT().AssignServerConn(gomock.Any()).AnyTimes().Return(nil)

	cl.EXPECT().ExecuteOn().AnyTimes()
	cl.EXPECT().SetExecuteOn(gomock.Any(), gomock.Any()).AnyTimes()

	cl.EXPECT().DefaultRouteBehaviour().Return("ALLOW").AnyTimes()

	// reroute on first query in this case
	cmngr.EXPECT().ValidateSliceChange(gomock.Any()).AnyTimes().Return(true)

	cmngr.EXPECT().UnRouteCB(gomock.Any(), gomock.Any()).AnyTimes()

	cmngr.EXPECT().TXEndCB(gomock.Any()).AnyTimes()

	qr.EXPECT().AnalyzeQuery(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

	qr.EXPECT().PlanQuery(gomock.Any(), gomock.Any()).Return(&plan.ShardDispatchPlan{
		ExecTarget: kr.ShardKey{
			Name: "sh1",
		},
	}, nil).Times(1)

	route := route.NewRoute(&config.BackendRule{}, frrule, map[string]*config.Shard{
		"sh1": {},
	})

	cl.EXPECT().Route().AnyTimes().Return(route)

	query := &pgproto3.Query{
		String: "SELECT 1",
	}

	cl.EXPECT().Receive().Times(1).Return(query, nil)

	srv.EXPECT().SendShard(query, gomock.Any()).Times(1).Return(nil)

	srv.EXPECT().Receive().Times(1).Return(&pgproto3.RowDescription{}, uint(0), nil)
	srv.EXPECT().Receive().Times(1).Return(&pgproto3.DataRow{
		Values: [][]byte{
			[]byte(
				"1",
			),
		},
	}, uint(0), nil)

	srv.EXPECT().Receive().Times(1).Return(&pgproto3.CommandComplete{
		CommandTag: []byte("SELECT"),
	}, uint(0), nil)
	srv.EXPECT().Receive().Times(1).Return(&pgproto3.ReadyForQuery{
		TxStatus: byte(txstatus.TXIDLE),
	}, uint(0), nil)

	// receive this 4 msgs
	cl.EXPECT().Send(gomock.Any()).Times(4).Return(nil)

	cl.EXPECT().Receive().Times(1).Return(nil, io.EOF)

	err := frontend.Frontend(qr, cl, cmngr, nil)

	assert.NoError(err, "")
}

func TestFrontendXProto(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	cl := mockcl.NewMockRouterClient(ctrl)
	srv := mocksrv.NewMockServer(ctrl)
	sh := mocksh.NewMockShardHostInstance(ctrl)
	qr := mockqr.NewMockQueryRouter(ctrl)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	mmgr := mockmgr.NewMockEntityMgr(ctrl)

	frrule := &config.FrontendRule{
		DB:       "db1",
		Usr:      "user1",
		PoolMode: config.PoolModeTransaction,
	}

	qr.EXPECT().Mgr().Return(mmgr).AnyTimes()

	qr.EXPECT().Mgr().Return(mmgr).AnyTimes()

	sh.EXPECT().ID().AnyTimes()

	srv.EXPECT().Name().AnyTimes().Return("serv1")
	srv.EXPECT().Datashards().AnyTimes().Return([]shard.ShardHostInstance{
		sh,
	})

	srv.EXPECT().AddDataShard(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	/* query Router */

	qr.EXPECT().DataShardsRoutes().AnyTimes().Return([]kr.ShardKey{{Name: "sh1"}})

	cl.EXPECT().ShowNoticeMsg().AnyTimes()
	cl.EXPECT().GetTsa().AnyTimes()

	cl.EXPECT().Server().AnyTimes().Return(srv)
	cl.EXPECT().MaintainParams().AnyTimes().Return(false)

	cl.EXPECT().Usr().AnyTimes().Return("user1")
	cl.EXPECT().DB().AnyTimes().Return("db1")

	cl.EXPECT().CleanupStatementSet().AnyTimes()

	cl.EXPECT().BindParams().AnyTimes()

	cl.EXPECT().ShardingKey().AnyTimes()
	cl.EXPECT().SetShardingKey(gomock.Any(), gomock.Any()).AnyTimes()

	cl.EXPECT().ID().AnyTimes()

	cl.EXPECT().Close().Times(1)
	cl.EXPECT().Rule().AnyTimes().Return(frrule)

	cl.EXPECT().ReplyParseComplete().AnyTimes()

	cl.EXPECT().ReplyDebugNotice(gomock.Any()).AnyTimes().Return(nil)
	cl.EXPECT().AssignServerConn(gomock.Any()).AnyTimes().Return(nil)

	// reroute on first query in this case
	cmngr.EXPECT().ValidateSliceChange(gomock.Any()).AnyTimes().Return(true)

	cmngr.EXPECT().UnRouteCB(gomock.Any(), gomock.Any()).AnyTimes()

	cmngr.EXPECT().TXEndCB(gomock.Any()).AnyTimes()

	route := route.NewRoute(&config.BackendRule{}, frrule, map[string]*config.Shard{
		"sh1": {},
	})

	// route to any route
	cl.EXPECT().Route().AnyTimes().Return(route)

	/* client request simple prep stmt parse */

	cl.EXPECT().Receive().Times(1).Return(&pgproto3.Parse{
		Name:          "stmtcache_1",
		Query:         "select 'Hello, world!'",
		ParameterOIDs: nil,
	}, nil)

	cl.EXPECT().Receive().Times(1).Return(&pgproto3.Describe{
		Name:       "stmtcache_1",
		ObjectType: 'S',
	}, nil)

	cl.EXPECT().Receive().Times(1).Return(&pgproto3.Sync{}, nil)

	def := &prepstatement.PreparedStatementDefinition{
		Name:  "stmtcache_1",
		Query: "select 'Hello, world!'",
	}

	cl.EXPECT().StorePreparedStatement(def).Times(1)
	cl.EXPECT().PreparedStatementDefinitionByName("stmtcache_1").AnyTimes().Return(def)
	cl.EXPECT().PreparedStatementQueryHashByName("stmtcache_1").AnyTimes().Return(uint64(17731273590378676854))

	cl.EXPECT().ExecuteOn().AnyTimes()
	cl.EXPECT().SetExecuteOn(gomock.Any(), gomock.Any()).AnyTimes()

	res := false
	rd := &prepstatement.PreparedStatementDescriptor{}

	sh.EXPECT().HasPrepareStatement(gomock.Any(), gomock.Any()).DoAndReturn(func(any, any) (any, any) {
		return res, rd
	}).AnyTimes()

	sh.EXPECT().StorePrepareStatement(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Do(func(any, any, any, any) {
		res = true
		rd.ParamDesc = &pgproto3.ParameterDescription{}
		rd.RowDesc = &pgproto3.RowDescription{}
	}).AnyTimes()
	/* */

	sh.EXPECT().SHKey().AnyTimes().Return(kr.ShardKey{
		Name: "sh1",
	})

	sh.EXPECT().Send(&pgproto3.Parse{
		Name:          "17731273590378676854",
		Query:         "select 'Hello, world!'",
		ParameterOIDs: nil,
	}).Times(1).Return(nil)

	sh.EXPECT().Send(&pgproto3.Describe{
		Name:       "17731273590378676854",
		ObjectType: 'S',
	}).Times(1).Return(nil)

	sh.EXPECT().Send(&pgproto3.Sync{}).AnyTimes().Return(nil)

	sh.EXPECT().Receive().Times(1).Return(&pgproto3.ParseComplete{}, nil)
	sh.EXPECT().Receive().Times(1).Return(&pgproto3.ParameterDescription{
		ParameterOIDs: nil,
	}, nil)

	sh.EXPECT().Receive().Times(1).Return(&pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("?column?"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25, /* textoid */
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
	}, nil)

	sh.EXPECT().Receive().Times(1).Return(&pgproto3.ReadyForQuery{
		TxStatus: byte(txstatus.TXIDLE),
	}, nil)

	// receive this 4 msgs
	cl.EXPECT().Send(gomock.Any()).Times(4).Return(nil)

	cl.EXPECT().Receive().Times(1).Return(nil, io.EOF)

	err := frontend.Frontend(qr, cl, cmngr, nil)

	assert.NoError(err, "")
}
