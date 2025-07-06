package clientinteractor_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	pkgclient "github.com/pg-sharding/spqr/pkg/client"
	mock "github.com/pg-sharding/spqr/pkg/mock/clientinteractor"
	mockinst "github.com/pg-sharding/spqr/pkg/mock/conn"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"go.uber.org/mock/gomock"

	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/router/client"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

// TestSimpleWhere tests the MatchRow function with a simple where clause.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestSimpleWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseLeaf{
		Op:     "=",
		ColRef: spqrparser.ColumnRef{ColName: "a"},
		Value:  "1",
	}
	expected := true

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.NoError(err)
	assert.Equal(expected, actual)
}

// TestSimpleNoMatchWhere tests the MatchRow function with a simple where clause that is expected to not match.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestSimpleNoMatchWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseLeaf{
		Op:     "=",
		ColRef: spqrparser.ColumnRef{ColName: "a"},
		Value:  "2",
	}
	expected := false

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.NoError(err)
	assert.Equal(expected, actual)
}

// TestAndNoMatchWhere tests the MatchRow function with a complex where clause that is expected to not match.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestAndNoMatchWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseOp{
		Op: "and",
		Left: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "b"},
			Value:  "2",
		},
		Right: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "a"},
			Value:  "2",
		},
	}
	expected := false

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.Nil(err)
	assert.Equal(expected, actual)
}

// TestOrMatchWhere tests the MatchRow function with a complex where clause that uses the OR operator.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestOrMatchWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseOp{
		Op: "or",
		Left: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "a"},
			Value:  "2",
		},
		Right: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "b"},
			Value:  "2",
		},
	}
	expected := true

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.NoError(err)
	assert.Equal(expected, actual)
}

func TestGetColumnsMap(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		name        string
		header      []string
		expectedMap map[string]int
	}{
		{
			name:        "Simple header",
			header:      []string{"a", "b", "c"},
			expectedMap: map[string]int{"a": 0, "b": 1, "c": 2},
		},
		{
			name:        "Empty header",
			header:      []string{},
			expectedMap: map[string]int{},
		},
		{
			name:        "Nil header",
			header:      nil,
			expectedMap: map[string]int{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			tableDescMock := mock.NewMockTableDesc(ctrl)
			tableDescMock.EXPECT().GetHeader().Return(testCase.header)
			assert.Equal(testCase.expectedMap, clientinteractor.GetColumnsMap(tableDescMock))
		})
	}

}

func TestSortableWithContext(t *testing.T) {
	data := [][]string{[]string{"a", "b"}, []string{"b", "a"}}
	rev_data := [][]string{[]string{"b", "a"}, []string{"a", "b"}}
	sortable := clientinteractor.SortableWithContext{data, 0, clientinteractor.DESC}
	sort.Sort(sortable)
	assert.Equal(t, data, rev_data)
}

func TestClientsOrderBy(t *testing.T) {

	ctrl := gomock.NewController(t)

	var v1, v2, v3, v4, v5, v6 proto.UsedShardInfo
	var i1, i2, i3, i4, i5, i6 proto.DBInstanceInfo

	i1.Hostname = "abracadabra1"
	i2.Hostname = "abracadabra2"
	i3.Hostname = "abracadabra14"
	i4.Hostname = "abracadabra52"
	i5.Hostname = "abracadabras"
	i6.Hostname = "abracadabrav"

	v1.Instance = &i1
	v2.Instance = &i2
	v3.Instance = &i3
	v4.Instance = &i4
	v5.Instance = &i5
	v6.Instance = &i6

	var a, b, c proto.ClientInfo

	a.ClientId = 1
	a.Dbname = "Barnaul"
	a.Dsname = "Rjaken"
	a.Shards = []*proto.UsedShardInfo{
		&v1, &v2,
	}

	b.ClientId = 2
	b.Dbname = "Moscow"
	b.Dsname = "Space"
	b.Shards = []*proto.UsedShardInfo{
		&v3, &v4,
	}

	c.ClientId = 2
	c.Dbname = "Ekaterinburg"
	c.Dsname = "Hill"
	c.Shards = []*proto.UsedShardInfo{
		&v5, &v6,
	}

	ca := mockcl.NewMockRouterClient(ctrl)
	cb := client.NewNoopClient(&b, "addr")
	cc := client.NewNoopClient(&c, "addr")
	interactor := clientinteractor.NewPSQLInteractor(ca)
	ci := []pkgclient.ClientInfo{
		pkgclient.ClientInfoImpl{Client: ca},
		pkgclient.ClientInfoImpl{Client: cb},
		pkgclient.ClientInfoImpl{Client: cc},
	}

	ca.EXPECT().Send(gomock.Any()).AnyTimes()
	ca.EXPECT().Shards().AnyTimes()
	ca.EXPECT().ID().AnyTimes()
	ca.EXPECT().Usr().AnyTimes()
	ca.EXPECT().DB().AnyTimes()
	err := interactor.Clients(context.TODO(), ci, &spqrparser.Show{
		Cmd:   spqrparser.ClientsStr,
		Where: spqrparser.WhereClauseEmpty{},
		Order: spqrparser.Order{OptAscDesc: spqrparser.SortByAsc{},
			Col: spqrparser.ColumnRef{ColName: "user"}},
	})
	assert.Nil(t, err)
}

func genShard(ctrl *gomock.Controller, host string, shardName string, shardId uint) shard.ShardHostInfo {
	sh := mockshard.NewMockShardHostInstance(ctrl)

	ins1 := mockinst.NewMockDBInstance(ctrl)
	ins1.EXPECT().Hostname().Return(host).AnyTimes()
	ins1.EXPECT().AvailabilityZone().Return("").AnyTimes()
	sh.EXPECT().Send(gomock.Any()).AnyTimes()
	sh.EXPECT().Pid().Return(uint32(1)).AnyTimes()
	sh.EXPECT().DB().Return("db1").AnyTimes()
	sh.EXPECT().Usr().Return("usr1").AnyTimes()
	sh.EXPECT().Sync().Return(int64(0)).AnyTimes()
	sh.EXPECT().TxStatus().Return(txstatus.TXIDLE).AnyTimes()
	sh.EXPECT().TxServed().Return(int64(10)).AnyTimes()
	sh.EXPECT().ShardKeyName().Return(shardName).AnyTimes()
	sh.EXPECT().InstanceHostname().Return(host).AnyTimes()
	sh.EXPECT().ID().Return(shardId).AnyTimes()
	sh.EXPECT().Instance().Return(ins1).AnyTimes()
	return sh
}

func TestBackendConnections(t *testing.T) {
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	var desc []pgproto3.FieldDescription
	for _, header := range clientinteractor.BackendConnectionsHeaders {
		desc = append(desc, clientinteractor.TextOidFD(header))
	}

	firstRow := pgproto3.DataRow{
		Values: [][]byte{
			[]byte("5"),
			[]byte("no data"),
			[]byte("sh1"),
			[]byte("h1"),
			[]byte("1"),
			[]byte("usr1"),
			[]byte("db1"),
			[]byte("0"),
			[]byte("10"),
			[]byte("IDLE"),
		},
	}
	secondRow := pgproto3.DataRow{
		Values: [][]byte{
			[]byte("6"),
			[]byte("no data"),
			[]byte("sh2"),
			[]byte("h2"),
			[]byte("1"),
			[]byte("usr1"),
			[]byte("db1"),
			[]byte("0"),
			[]byte("10"),
			[]byte("IDLE"),
		},
	}
	thirdRow := pgproto3.DataRow{
		Values: [][]byte{
			[]byte("7"),
			[]byte("no data"),
			[]byte("sh3"),
			[]byte("h1"),
			[]byte("1"),
			[]byte("usr1"),
			[]byte("db1"),
			[]byte("0"),
			[]byte("10"),
			[]byte("IDLE"),
		},
	}

	gomock.InOrder(
		ca.EXPECT().Send(&pgproto3.RowDescription{Fields: desc}),
		ca.EXPECT().Send(&firstRow),
		ca.EXPECT().Send(&secondRow),
		ca.EXPECT().Send(&thirdRow),
		ca.EXPECT().Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 3")}),
		ca.EXPECT().Send(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}),
	)

	interactor := clientinteractor.NewPSQLInteractor(ca)
	ctx := context.Background()
	shards := []shard.ShardHostInfo{
		genShard(ctrl, "h1", "sh1", 5),
		genShard(ctrl, "h2", "sh2", 6),
		genShard(ctrl, "h1", "sh3", 7),
	}
	cmd := &spqrparser.Show{
		Cmd:     spqrparser.BackendConnectionsStr,
		GroupBy: spqrparser.GroupByClauseEmpty{},
	}
	err := interactor.BackendConnections(ctx, shards, cmd)
	assert.Nil(t, err)
}
func TestBackendConnectionsGroupBySuccessDescData(t *testing.T) {
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	var desc []pgproto3.FieldDescription
	desc = append(desc, clientinteractor.TextOidFD("hostname"))
	desc = append(desc, clientinteractor.IntOidFD("count"))
	firstRow := pgproto3.DataRow{
		Values: [][]byte{
			[]byte("h1"),
			[]byte("2"),
		},
	}
	secondRow := pgproto3.DataRow{
		Values: [][]byte{
			[]byte("h2"),
			[]byte("1"),
		},
	}
	gomock.InOrder(
		ca.EXPECT().Send(&pgproto3.RowDescription{Fields: desc}),
		ca.EXPECT().Send(&firstRow),
		ca.EXPECT().Send(&secondRow),
		ca.EXPECT().Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 2")}),
		ca.EXPECT().Send(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}),
	)

	interactor := clientinteractor.NewPSQLInteractor(ca)
	ctx := context.Background()
	shards := []shard.ShardHostInfo{
		genShard(ctrl, "h2", "sh2", 1),
		genShard(ctrl, "h1", "sh1", 2),
		genShard(ctrl, "h1", "sh3", 3),
	}
	cmd := &spqrparser.Show{
		Cmd:     spqrparser.BackendConnectionsStr,
		GroupBy: spqrparser.GroupBy{Col: spqrparser.ColumnRef{ColName: "hostname"}},
	}
	err := interactor.BackendConnections(ctx, shards, cmd)
	assert.Nil(t, err)
}
func TestBackendConnectionsGroupBySuccessAscData(t *testing.T) {
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	var desc []pgproto3.FieldDescription
	desc = append(desc, clientinteractor.TextOidFD("hostname"))
	desc = append(desc, clientinteractor.IntOidFD("count"))
	firstRow := pgproto3.DataRow{
		Values: [][]byte{
			[]byte("h1"),
			[]byte("2"),
		},
	}
	secondRow := pgproto3.DataRow{
		Values: [][]byte{
			[]byte("h2"),
			[]byte("1"),
		},
	}
	gomock.InOrder(
		ca.EXPECT().Send(&pgproto3.RowDescription{Fields: desc}),
		ca.EXPECT().Send(&firstRow),
		ca.EXPECT().Send(&secondRow),
		ca.EXPECT().Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 2")}),
		ca.EXPECT().Send(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}),
	)

	interactor := clientinteractor.NewPSQLInteractor(ca)
	ctx := context.Background()
	shards := []shard.ShardHostInfo{
		genShard(ctrl, "h1", "sh1", 1),
		genShard(ctrl, "h1", "sh3", 2),
		genShard(ctrl, "h2", "sh2", 3),
	}
	cmd := &spqrparser.Show{
		Cmd:     spqrparser.BackendConnectionsStr,
		GroupBy: spqrparser.GroupBy{Col: spqrparser.ColumnRef{ColName: "hostname"}},
	}
	err := interactor.BackendConnections(ctx, shards, cmd)
	assert.Nil(t, err)
}

func TestBackendConnectionsGroupByFail(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	interactor := clientinteractor.NewPSQLInteractor(ca)
	ctx := context.Background()
	shards := []shard.ShardHostInfo{
		genShard(ctrl, "h1", "sh1", 1),
		genShard(ctrl, "h2", "sh2", 2),
		genShard(ctrl, "h1", "sh3", 3),
	}
	cmd := &spqrparser.Show{
		Cmd:     spqrparser.BackendConnectionsStr,
		GroupBy: spqrparser.GroupBy{Col: spqrparser.ColumnRef{ColName: "someColumn"}},
	}
	err := interactor.BackendConnections(ctx, shards, cmd)
	assert.ErrorContains(err, "not found column 'someColumn' for group by statement")
}

func TestMakeSimpleResponseWithData(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	interactor := clientinteractor.NewPSQLInteractor(ca)
	info := []clientinteractor.SimpleResultRow{
		clientinteractor.SimpleResultRow{Name: "test1", Value: "data1"},
		clientinteractor.SimpleResultRow{Name: "test2", Value: "data2"},
	}
	data := clientinteractor.SimpleResultMsg{Header: "test header", Rows: info}

	desc := []pgproto3.FieldDescription{clientinteractor.TextOidFD("test header")}
	firstRow := pgproto3.DataRow{
		Values: [][]byte{[]byte(fmt.Sprintf("%s	-> %s", "test1", "data1"))},
	}
	secondRow := pgproto3.DataRow{
		Values: [][]byte{[]byte(fmt.Sprintf("%s	-> %s", "test2", "data2"))},
	}
	gomock.InOrder(
		ca.EXPECT().Send(&pgproto3.RowDescription{Fields: desc}),
		ca.EXPECT().Send(&firstRow),
		ca.EXPECT().Send(&secondRow),
		ca.EXPECT().Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 2")}),
		ca.EXPECT().Send(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}),
	)
	err := interactor.MakeSimpleResponse(ctx, data)
	assert.Nil(t, err)
}

func TestMakeSimpleResponseEmpty(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	interactor := clientinteractor.NewPSQLInteractor(ca)
	info := []clientinteractor.SimpleResultRow{}
	data := clientinteractor.SimpleResultMsg{Header: "test header", Rows: info}

	desc := []pgproto3.FieldDescription{clientinteractor.TextOidFD("test header")}
	gomock.InOrder(
		ca.EXPECT().Send(&pgproto3.RowDescription{Fields: desc}),
		ca.EXPECT().Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")}),
		ca.EXPECT().Send(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}),
	)
	err := interactor.MakeSimpleResponse(ctx, data)
	assert.Nil(t, err)
}
