package app_test

import (
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	mocksh "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	app "github.com/pg-sharding/spqr/router"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockqr "github.com/pg-sharding/spqr/router/mock/qrouter"
	mocksrv "github.com/pg-sharding/spqr/router/mock/server"
	"github.com/pg-sharding/spqr/router/route"
	"github.com/pg-sharding/spqr/router/routingstate"

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

	err := app.Frontend(qr, cl, cmngr, &config.Router{}, nil)

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

	cl.EXPECT().Usr().AnyTimes().Return("user1")
	cl.EXPECT().DB().AnyTimes().Return("db1")

	cl.EXPECT().SetRouteHint(gomock.Any()).AnyTimes()
	cl.EXPECT().BindParams().AnyTimes()

	cl.EXPECT().ID().AnyTimes()

	cl.EXPECT().Close().Times(1)
	cl.EXPECT().Rule().AnyTimes().Return(
		frrule,
	)

	cl.EXPECT().ReplyDebugNotice(gomock.Any()).AnyTimes().Return(nil)
	cl.EXPECT().AssignServerConn(gomock.Any()).AnyTimes().Return(nil)

	cl.EXPECT().RLock().AnyTimes()
	cl.EXPECT().RUnlock().AnyTimes()

	// reroute on first query in this case
	cmngr.EXPECT().ValidateReRoute(gomock.Any()).Return(true)

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

	err := app.Frontend(qr, cl, cmngr, &config.Router{}, nil)

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

	cl.EXPECT().Usr().AnyTimes().Return("user1")
	cl.EXPECT().DB().AnyTimes().Return("db1")

	cl.EXPECT().SetRouteHint(gomock.Any()).AnyTimes()
	cl.EXPECT().BindParams().AnyTimes()

	cl.EXPECT().ID().AnyTimes()

	cl.EXPECT().Close().Times(1)
	cl.EXPECT().Rule().AnyTimes().Return(frrule)

	cl.EXPECT().ReplyParseComplete().AnyTimes()

	cl.EXPECT().ReplyDebugNotice(gomock.Any()).AnyTimes().Return(nil)
	cl.EXPECT().AssignServerConn(gomock.Any()).AnyTimes().Return(nil)

	cl.EXPECT().RLock().AnyTimes()
	cl.EXPECT().RUnlock().AnyTimes()

	// reroute on first query in this case
	cmngr.EXPECT().ValidateReRoute(gomock.Any()).Return(true)

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

	cl.EXPECT().StorePreparedStatement("stmtcache_1", "select 'Hello, world!'").Times(1).Return()
	cl.EXPECT().PreparedStatementQueryByName("stmtcache_1").Return("select 'Hello, world!'")

	cl.EXPECT().ServerAcquireUse().AnyTimes()
	cl.EXPECT().ServerReleaseUse().AnyTimes()

	srv.EXPECT().HasPrepareStatement(gomock.Any()).Return(false, shard.PreparedStatementDescriptor{}).AnyTimes()
	srv.EXPECT().PrepareStatement(gomock.Any(), gomock.Any()).AnyTimes()
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

	srv.EXPECT().Send(&pgproto3.Sync{}).Times(2).Return(nil)

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

	srv.EXPECT().Receive().Times(2).Return(&pgproto3.ReadyForQuery{
		TxStatus: byte(txstatus.TXIDLE),
	}, nil)

	// receive this 4 msgs
	cl.EXPECT().Send(gomock.Any()).Times(3).Return(nil)

	cl.EXPECT().Receive().Times(1).Return(nil, io.EOF)

	err := app.Frontend(qr, cl, cmngr, &config.Router{}, nil)

	assert.NoError(err, "")
}

func TestFrontendSimpleCopyIn(t *testing.T) {
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

	srv.EXPECT().Name().AnyTimes().Return("serv1")
	srv.EXPECT().Datashards().AnyTimes().Return([]shard.Shard{})

	cl.EXPECT().Server().AnyTimes().Return(srv)

	cl.EXPECT().Usr().AnyTimes().Return("user1")
	cl.EXPECT().DB().AnyTimes().Return("db1")

	cl.EXPECT().SetRouteHint(gomock.Any()).AnyTimes()
	cl.EXPECT().BindParams().AnyTimes()

	cl.EXPECT().ID().AnyTimes()

	cl.EXPECT().Close().Times(1)
	cl.EXPECT().Rule().AnyTimes().Return(
		frrule,
	)

	cl.EXPECT().ReplyDebugNotice(gomock.Any()).AnyTimes().Return(nil)
	cl.EXPECT().AssignServerConn(gomock.Any()).AnyTimes().Return(nil)

	cl.EXPECT().RLock().AnyTimes()
	cl.EXPECT().RUnlock().AnyTimes()

	// reroute on first query in this case
	cmngr.EXPECT().ValidateReRoute(gomock.Any()).Return(true)

	cmngr.EXPECT().RouteCB(cl, gomock.Any()).AnyTimes()

	cmngr.EXPECT().UnRouteCB(gomock.Any(), gomock.Any()).AnyTimes()

	cmngr.EXPECT().TXBeginCB(gomock.Any()).AnyTimes()

	cmngr.EXPECT().TXEndCB(gomock.Any()).AnyTimes()

	qr.EXPECT().Route(gomock.Any(), &lyx.Copy{
		TableRef: &lyx.RangeVar{
			RelationName: "xx",
		},
		Where:  &lyx.AExprEmpty{},
		IsFrom: true,
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
		String: "COPY xx FROM STDIN",
	}

	cl.EXPECT().Receive().Times(1).Return(query, nil)

	cl.EXPECT().Receive().Times(4).Return(&pgproto3.CopyData{}, nil)
	cl.EXPECT().Receive().Times(1).Return(&pgproto3.CopyDone{}, nil)

	srv.EXPECT().Send(query).Times(1).Return(nil)

	srv.EXPECT().Send(&pgproto3.CopyData{}).Times(4).Return(nil)
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

	err := app.Frontend(qr, cl, cmngr, &config.Router{}, nil)

	assert.NoError(err, "")
}
