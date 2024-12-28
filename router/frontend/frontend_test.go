package frontend_test

import (
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	mocksh "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/router/frontend"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockqr "github.com/pg-sharding/spqr/router/mock/qrouter"
	mocksrv "github.com/pg-sharding/spqr/router/mock/server"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/pg-sharding/spqr/router/routingstate"

	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	mockcmgr "github.com/pg-sharding/spqr/router/mock/poolmgr"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
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

	err := frontend.Frontend(qr, cl, cmngr, &config.Router{}, nil)

	assert.NoError(err, "")
}

func TestFrontendSimple(t *testing.T) {

	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	cl := mockcl.NewMockRouterClient(ctrl)
	srv := mocksrv.NewMockServer(ctrl)
	qr := mockqr.NewMockQueryRouter(ctrl)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	frrule := &config.FrontendRule{
		DB:  "db1",
		Usr: "user1",
	}

	beRule := &config.BackendRule{}

	srv.EXPECT().Datashards().AnyTimes().Return([]shard.Shard{})
	srv.EXPECT().Name().AnyTimes().Return("serv1")

	cl.EXPECT().Server().AnyTimes().Return(srv)
	cl.EXPECT().MaintainParams().AnyTimes().Return(false)

	cl.EXPECT().Usr().AnyTimes().Return("user1")
	cl.EXPECT().DB().AnyTimes().Return("db1")

	cl.EXPECT().SetRouteHint(gomock.Any()).AnyTimes()
	cl.EXPECT().BindParams().AnyTimes()

	cl.EXPECT().ShardingKey().AnyTimes()
	cl.EXPECT().SetShardingKey(gomock.Any()).AnyTimes()

	cl.EXPECT().ID().AnyTimes()

	cl.EXPECT().Close().Times(1)
	cl.EXPECT().Rule().AnyTimes().Return(
		frrule,
	)

	cl.EXPECT().ReplyDebugNotice(gomock.Any()).AnyTimes().Return(nil)
	cl.EXPECT().AssignServerConn(gomock.Any()).AnyTimes().Return(nil)

	cl.EXPECT().ExecuteOn().AnyTimes()
	cl.EXPECT().SetExecuteOn(gomock.Any()).AnyTimes()

	cl.EXPECT().AllowMultishard().AnyTimes()
	cl.EXPECT().SetAllowMultishard(gomock.Any()).AnyTimes()

	// reroute on first query in this case
	cmngr.EXPECT().ValidateReRoute(gomock.Any()).AnyTimes().Return(true)

	cmngr.EXPECT().RouteCB(cl, gomock.Any()).AnyTimes()

	cmngr.EXPECT().UnRouteCB(gomock.Any(), gomock.Any()).AnyTimes()

	cmngr.EXPECT().TXBeginCB(gomock.Any()).AnyTimes()

	cmngr.EXPECT().TXEndCB(gomock.Any()).AnyTimes()

	qr.EXPECT().Route(gomock.Any(), &lyx.Select{
		TargetList: []lyx.Node{
			&lyx.AExprIConst{Value: 1},
		},
		Where: &lyx.AExprEmpty{},
	}, gomock.Any()).Return(routingstate.ShardMatchState{
		Route: &routingstate.DataShardRoute{
			Shkey: kr.ShardKey{
				Name: "sh1",
			},
		},
	}, nil).Times(1)

	route := route.NewRoute(beRule, frrule, map[string]*config.Shard{
		"sh1": {},
	})

	cl.EXPECT().Route().AnyTimes().Return(route)

	query := &pgproto3.Query{
		String: "SELECT 1",
	}

	cl.EXPECT().Receive().Times(1).Return(query, nil)

	srv.EXPECT().Send(query).Times(1).Return(nil)

	srv.EXPECT().Receive().Times(1).Return(&pgproto3.RowDescription{}, nil)
	srv.EXPECT().Receive().Times(1).Return(&pgproto3.DataRow{
		Values: [][]byte{
			[]byte(
				"1",
			),
		},
	}, nil)

	srv.EXPECT().Receive().Times(1).Return(&pgproto3.CommandComplete{
		CommandTag: []byte("SELECT"),
	}, nil)
	srv.EXPECT().Receive().Times(1).Return(&pgproto3.ReadyForQuery{
		TxStatus: byte(txstatus.TXIDLE),
	}, nil)

	// receive this 4 msgs
	cl.EXPECT().Send(gomock.Any()).Times(4).Return(nil)

	cl.EXPECT().Receive().Times(1).Return(nil, io.EOF)

	err := frontend.Frontend(qr, cl, cmngr, &config.Router{}, nil)

	assert.NoError(err, "")
}

func TestFrontendXProto(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	cl := mockcl.NewMockRouterClient(ctrl)
	srv := mocksrv.NewMockServer(ctrl)
	sh := mocksh.NewMockShard(ctrl)
	qr := mockqr.NewMockQueryRouter(ctrl)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)

	frrule := &config.FrontendRule{
		DB:       "db1",
		Usr:      "user1",
		PoolMode: config.PoolModeTransaction,
	}

	beRule := &config.BackendRule{}

	sh.EXPECT().ID().AnyTimes()
	sh.EXPECT().Send(gomock.Any()).AnyTimes()
	sh.EXPECT().Receive().AnyTimes()

	srv.EXPECT().Name().AnyTimes().Return("serv1")
	srv.EXPECT().Datashards().AnyTimes().Return([]shard.Shard{
		sh,
	})

	/* query Router */

	qr.EXPECT().DataShardsRoutes().AnyTimes().Return([]*routingstate.DataShardRoute{
		{
			Shkey: kr.ShardKey{
				Name: "sh1",
			},
		},
	})

	cl.EXPECT().Server().AnyTimes().Return(srv)
	cl.EXPECT().MaintainParams().AnyTimes().Return(false)

	cl.EXPECT().Usr().AnyTimes().Return("user1")
	cl.EXPECT().DB().AnyTimes().Return("db1")

	cl.EXPECT().SetRouteHint(gomock.Any()).AnyTimes()
	cl.EXPECT().BindParams().AnyTimes()

	cl.EXPECT().ShardingKey().AnyTimes()
	cl.EXPECT().SetShardingKey(gomock.Any()).AnyTimes()

	cl.EXPECT().ID().AnyTimes()

	cl.EXPECT().Close().Times(1)
	cl.EXPECT().Rule().AnyTimes().Return(frrule)

	cl.EXPECT().ReplyParseComplete().AnyTimes()

	cl.EXPECT().ReplyDebugNotice(gomock.Any()).AnyTimes().Return(nil)
	cl.EXPECT().AssignServerConn(gomock.Any()).AnyTimes().Return(nil)

	// reroute on first query in this case
	cmngr.EXPECT().ValidateReRoute(gomock.Any()).AnyTimes().Return(true)

	cmngr.EXPECT().RouteCB(cl, gomock.Any()).AnyTimes()

	cmngr.EXPECT().UnRouteCB(gomock.Any(), gomock.Any()).AnyTimes()

	cmngr.EXPECT().TXBeginCB(gomock.Any()).AnyTimes()

	cmngr.EXPECT().TXEndCB(gomock.Any()).AnyTimes()

	route := route.NewRoute(beRule, frrule, map[string]*config.Shard{
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
	cl.EXPECT().SetExecuteOn(gomock.Any()).AnyTimes()

	cl.EXPECT().AllowMultishard().AnyTimes()
	cl.EXPECT().SetAllowMultishard(gomock.Any()).AnyTimes()

	res := false
	rd := &prepstatement.PreparedStatementDescriptor{}

	srv.EXPECT().HasPrepareStatement(gomock.Any()).DoAndReturn(func(interface{}) (interface{}, interface{}) { return res, rd }).AnyTimes()
	srv.EXPECT().StorePrepareStatement(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(interface{}, interface{}, interface{}) {
		res = true
		rd.ParamDesc = &pgproto3.ParameterDescription{}
		rd.RowDesc = &pgproto3.RowDescription{}
	}).AnyTimes()
	/* */

	srv.EXPECT().Send(&pgproto3.Parse{
		Name:          "17731273590378676854",
		Query:         "select 'Hello, world!'",
		ParameterOIDs: nil,
	}).Times(1).Return(nil)

	srv.EXPECT().Send(&pgproto3.Describe{
		Name:       "17731273590378676854",
		ObjectType: 'S',
	}).Times(1).Return(nil)

	srv.EXPECT().Send(&pgproto3.Sync{}).Times(1).Return(nil)

	srv.EXPECT().Receive().Times(1).Return(&pgproto3.ParseComplete{}, nil)
	srv.EXPECT().Receive().Times(1).Return(&pgproto3.ParameterDescription{
		ParameterOIDs: nil,
	}, nil)

	srv.EXPECT().Receive().Times(1).Return(&pgproto3.RowDescription{
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

	srv.EXPECT().Receive().Times(1).Return(&pgproto3.ReadyForQuery{
		TxStatus: byte(txstatus.TXIDLE),
	}, nil)

	// receive this 4 msgs
	cl.EXPECT().Send(gomock.Any()).Times(4).Return(nil)

	cl.EXPECT().Receive().Times(1).Return(nil, io.EOF)

	err := frontend.Frontend(qr, cl, cmngr, &config.Router{}, nil)

	assert.NoError(err, "")
}

func TestFrontendSimpleCopyIn(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	cl := mockcl.NewMockRouterClient(ctrl)
	srv := mocksrv.NewMockServer(ctrl)
	qr := mockqr.NewMockQueryRouter(ctrl)
	cmngr := mockcmgr.NewMockPoolMgr(ctrl)
	mmgr := mockmgr.NewMockEntityMgr(ctrl)

	sh1 := mocksh.NewMockShard(ctrl)
	sh1.EXPECT().Name().AnyTimes().Return("sh1")
	sh1.EXPECT().SHKey().AnyTimes().Return(kr.ShardKey{Name: "sh1"})
	sh1.EXPECT().ID().AnyTimes().Return(uint(1))
	sh2 := mocksh.NewMockShard(ctrl)
	sh2.EXPECT().Name().AnyTimes().Return("sh2")
	sh2.EXPECT().SHKey().AnyTimes().Return(kr.ShardKey{Name: "sh2"})
	sh2.EXPECT().ID().AnyTimes().Return(uint(2))

	frrule := &config.FrontendRule{
		DB:  "db1",
		Usr: "user1",
	}

	beRule := &config.BackendRule{}

	srv.EXPECT().Name().AnyTimes().Return("serv1")
	srv.EXPECT().Datashards().AnyTimes().Return([]shard.Shard{sh1, sh2})

	cl.EXPECT().Server().AnyTimes().Return(srv)
	cl.EXPECT().MaintainParams().AnyTimes().Return(false)

	cl.EXPECT().Usr().AnyTimes().Return("user1")
	cl.EXPECT().DB().AnyTimes().Return("db1")

	/* this is non-multishard tests */
	cl.EXPECT().AllowMultishard().Return(false).AnyTimes()

	cl.EXPECT().SetRouteHint(gomock.Any()).AnyTimes()
	cl.EXPECT().BindParams().AnyTimes()

	cl.EXPECT().ShardingKey().AnyTimes()
	cl.EXPECT().SetShardingKey(gomock.Any()).AnyTimes()

	cl.EXPECT().ID().AnyTimes()

	cl.EXPECT().Close().Times(1)
	cl.EXPECT().Rule().AnyTimes().Return(
		frrule,
	)

	cl.EXPECT().ReplyDebugNotice(gomock.Any()).AnyTimes().Return(nil)
	cl.EXPECT().AssignServerConn(gomock.Any()).AnyTimes().Return(nil)

	cl.EXPECT().ExecuteOn().AnyTimes()
	cl.EXPECT().SetExecuteOn(gomock.Any()).AnyTimes()

	cl.EXPECT().AllowMultishard().AnyTimes()
	cl.EXPECT().SetAllowMultishard(gomock.Any()).AnyTimes()

	// reroute on first query in this case
	cmngr.EXPECT().ValidateReRoute(gomock.Any()).AnyTimes().Return(true)

	cmngr.EXPECT().RouteCB(cl, gomock.Any()).AnyTimes()

	cmngr.EXPECT().UnRouteCB(gomock.Any(), gomock.Any()).AnyTimes()

	cmngr.EXPECT().TXBeginCB(gomock.Any()).AnyTimes()

	cmngr.EXPECT().TXEndCB(gomock.Any()).AnyTimes()

	tableref := &lyx.RangeVar{
		RelationName: "xx",
	}
	d := &distributions.Distribution{
		Id:       "dssd",
		ColTypes: []string{"integer"},
		Relations: map[string]*distributions.DistributedRelation{
			"xx": {
				Name: "xx",
				DistributionKey: []distributions.DistributionKeyEntry{
					{
						Column:       "i",
						HashFunction: "identity",
					},
				},
			},
		},
	}
	krs := []*kr.KeyRange{
		{
			ID:      "id1",
			ShardID: "sh1",
			LowerBound: kr.KeyRangeBound{
				1,
			},
		},
	}

	mmgr.EXPECT().GetRelationDistribution(gomock.Any(), gomock.Any()).Return(d, nil).AnyTimes()
	mmgr.EXPECT().ListKeyRanges(gomock.Any(), gomock.Any()).Return(krs, nil).AnyTimes()

	qr.EXPECT().Mgr().Return(mmgr).AnyTimes()
	qr.EXPECT().DeparseKeyWithRangesInternal(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&routingstate.DataShardRoute{
			Shkey: kr.ShardKey{
				Name: "sh1",
			},
		}, nil).AnyTimes()

	qr.EXPECT().Route(gomock.Any(), &lyx.Copy{
		TableRef: tableref,
		Where:    &lyx.AExprEmpty{},
		IsFrom:   true,
		Columns:  []string{"i"},
	}, gomock.Any()).Return(routingstate.MultiMatchState{}, nil).Times(1)

	qr.EXPECT().DataShardsRoutes().AnyTimes().Return([]*routingstate.DataShardRoute{
		&routingstate.DataShardRoute{Shkey: sh1.SHKey()},
		&routingstate.DataShardRoute{Shkey: sh2.SHKey()}},
	)

	route := route.NewRoute(beRule, frrule, map[string]*config.Shard{
		"sh1": {},
	})

	cl.EXPECT().Route().AnyTimes().Return(route)

	query := &pgproto3.Query{
		String: "COPY xx (i) FROM STDIN",
	}

	cl.EXPECT().Receive().Times(1).Return(query, nil)

	cl.EXPECT().Receive().Times(4).Return(&pgproto3.CopyData{Data: []byte("1\n")}, nil)
	cl.EXPECT().Receive().Times(1).Return(&pgproto3.CopyDone{}, nil)

	srv.EXPECT().Send(query).Times(1).Return(nil)

	sh1.EXPECT().Send(&pgproto3.CopyData{Data: []byte("1\n")}).Times(4).Return(nil)
	srv.EXPECT().Send(&pgproto3.CopyDone{}).Times(1).Return(nil)

	srv.EXPECT().Receive().Times(1).Return(&pgproto3.CopyInResponse{}, nil)

	// expect this msg
	cl.EXPECT().Send(&pgproto3.CopyInResponse{}).Times(1).Return(nil)

	srv.EXPECT().Receive().Times(1).Return(&pgproto3.CommandComplete{
		CommandTag: []byte("SELECT"),
	}, nil)
	srv.EXPECT().Receive().Times(1).Return(&pgproto3.ReadyForQuery{
		TxStatus: byte(txstatus.TXIDLE),
	}, nil)

	// receive last 2 msgs
	cl.EXPECT().Send(&pgproto3.CommandComplete{
		CommandTag: []byte("SELECT"),
	}).Times(1).Return(nil)

	cl.EXPECT().Send(&pgproto3.ReadyForQuery{
		TxStatus: byte(txstatus.TXIDLE),
	}).Times(1).Return(nil)

	cl.EXPECT().Receive().Times(1).Return(nil, io.EOF)

	err := frontend.Frontend(qr, cl, cmngr, &config.Router{}, nil)

	assert.NoError(err, "")
}
