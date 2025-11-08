package clientinteractor_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	pkgclient "github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/engine"
	mockinst "github.com/pg-sharding/spqr/pkg/mock/conn"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	"github.com/pg-sharding/spqr/qdb"
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

	row := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := &lyx.AExprOp{
		Op:    "=",
		Left:  &lyx.ColumnRef{ColName: "a"},
		Right: &lyx.AExprSConst{Value: "1"},
	}
	expected := true

	actual, err := engine.MatchRow(row, rowDesc, where)
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

	row := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}

	where := &lyx.AExprOp{
		Op:    "=",
		Left:  &lyx.ColumnRef{ColName: "a"},
		Right: &lyx.AExprSConst{Value: "2"},
	}
	expected := false

	actual, err := engine.MatchRow(row, rowDesc, where)
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

	row := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := &lyx.AExprOp{
		Op: "and",
		Left: &lyx.AExprOp{
			Op:    "=",
			Left:  &lyx.ColumnRef{ColName: "b"},
			Right: &lyx.AExprSConst{Value: "2"},
		},
		Right: &lyx.AExprOp{
			Op:    "=",
			Left:  &lyx.ColumnRef{ColName: "a"},
			Right: &lyx.AExprSConst{Value: "2"},
		},
	}
	expected := false

	actual, err := engine.MatchRow(row, rowDesc, where)
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

	row := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}

	where := &lyx.AExprOp{
		Op: "or",
		Left: &lyx.AExprOp{
			Op:    "=",
			Left:  &lyx.ColumnRef{ColName: "a"},
			Right: &lyx.AExprSConst{Value: "2"},
		},
		Right: &lyx.AExprOp{
			Op:    "=",
			Left:  &lyx.ColumnRef{ColName: "b"},
			Right: &lyx.AExprSConst{Value: "2"},
		},
	}

	expected := true

	actual, err := engine.MatchRow(row, rowDesc, where)
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
			tupleDesc := tupleslot.TupleDesc(
				engine.GetVPHeader(testCase.header...))

			assert.Equal(testCase.expectedMap, tupleDesc.GetColumnsMap())
		})
	}
}

func TestSortableWithContext(t *testing.T) {
	data := [][][]byte{{[]byte("a"), []byte("b")}, {[]byte("b"), []byte("a")}}
	rev_data := [][][]byte{{[]byte("b"), []byte("a")}, {[]byte("a"), []byte("b")}}
	/*XXX: very hacky*/
	op, err := engine.SearchSysCacheOperator(catalog.TEXTOID)
	assert.NoError(t, err)

	sortable := engine.SortableWithContext{
		Data:      data,
		Col_index: 0,
		Order:     engine.DESC,
		Op:        op}
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
	shw := &spqrparser.Show{
		Cmd:   spqrparser.ClientsStr,
		Where: &lyx.AExprEmpty{},
		Order: spqrparser.Order{OptAscDesc: spqrparser.SortByAsc{},
			Col: spqrparser.ColumnRef{ColName: "user"}},
	}

	tts, err := engine.ClientsVirtualRelationScan(context.TODO(), ci)
	assert.NoError(t, err)
	ftts, err := engine.FilterRows(tts, shw.Where)
	assert.NoError(t, err)

	assert.Nil(t, interactor.ReplyTTS(ftts))
}

func genShard(ctrl *gomock.Controller, host string, shardName string, shardId uint) shard.ShardHostCtl {
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
	sh.EXPECT().IsStale().AnyTimes().Return(false)
	sh.EXPECT().CreatedAt().AnyTimes().Return(time.Time(time.Unix(11, 0)))
	return sh
}

func TestBackendConnections(t *testing.T) {
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	var desc []pgproto3.FieldDescription
	for _, header := range engine.BackendConnectionsHeaders {
		desc = append(desc, engine.TextOidFD(header))
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
			[]byte("false"),
			[]byte("1970-01-01T00:00:11Z"),
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
			[]byte("false"),
			[]byte("1970-01-01T00:00:11Z"),
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
			[]byte("false"),
			[]byte("1970-01-01T00:00:11Z"),
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

	shards := []shard.ShardHostCtl{
		genShard(ctrl, "h1", "sh1", 5),
		genShard(ctrl, "h2", "sh2", 6),
		genShard(ctrl, "h1", "sh3", 7),
	}
	cmd := &spqrparser.Show{
		Cmd:     spqrparser.BackendConnectionsStr,
		GroupBy: spqrparser.GroupByClauseEmpty{},
	}

	tts, err := engine.BackendConnectionsVirtualRelationScan(shards)
	assert.NoError(t, err)

	ftts, err := engine.FilterRows(tts, cmd.Where)
	assert.NoError(t, err)

	resTTS, err := engine.GroupBy(ftts, cmd.GroupBy)
	assert.NoError(t, err)

	assert.Nil(t, interactor.ReplyTTS(resTTS))
}

func TestBackendConnectionsWhere(t *testing.T) {
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	var desc []pgproto3.FieldDescription
	for _, header := range engine.BackendConnectionsHeaders {
		desc = append(desc, engine.TextOidFD(header))
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
			[]byte("false"),
			[]byte("1970-01-01T00:00:11Z"),
		},
	}

	gomock.InOrder(
		ca.EXPECT().Send(&pgproto3.RowDescription{Fields: desc}),
		ca.EXPECT().Send(&secondRow),
		ca.EXPECT().Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}),
		ca.EXPECT().Send(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}),
	)

	interactor := clientinteractor.NewPSQLInteractor(ca)
	shards := []shard.ShardHostCtl{
		genShard(ctrl, "h1", "sh1", 5),
		genShard(ctrl, "h2", "sh2", 6),
		genShard(ctrl, "h1", "sh3", 7),
	}
	cmd := &spqrparser.Show{
		Cmd: spqrparser.BackendConnectionsStr,
		Where: &lyx.AExprOp{
			Left: &lyx.ColumnRef{
				ColName: "hostname",
			},
			Right: &lyx.AExprSConst{Value: "h2"},
			Op:    "=",
		},
		GroupBy: spqrparser.GroupByClauseEmpty{},
	}

	tts, err := engine.BackendConnectionsVirtualRelationScan(shards)
	assert.NoError(t, err)

	ftts, err := engine.FilterRows(tts, cmd.Where)
	assert.NoError(t, err)

	resTTS, err := engine.GroupBy(ftts, cmd.GroupBy)
	assert.NoError(t, err)

	assert.Nil(t, interactor.ReplyTTS(resTTS))
}

func TestBackendConnectionsGroupBySuccessDescData(t *testing.T) {
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	var desc []pgproto3.FieldDescription
	desc = append(desc, engine.TextOidFD("hostname"))
	desc = append(desc, engine.IntOidFD("count"))
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
	shards := []shard.ShardHostCtl{
		genShard(ctrl, "h2", "sh2", 1),
		genShard(ctrl, "h1", "sh1", 2),
		genShard(ctrl, "h1", "sh3", 3),
	}
	cmd := &spqrparser.Show{
		Cmd:     spqrparser.BackendConnectionsStr,
		GroupBy: spqrparser.GroupBy{Col: []spqrparser.ColumnRef{{ColName: "hostname"}}},
	}

	tts, err := engine.BackendConnectionsVirtualRelationScan(shards)
	assert.NoError(t, err)

	ftts, err := engine.FilterRows(tts, cmd.Where)
	assert.NoError(t, err)

	resTTS, err := engine.GroupBy(ftts, cmd.GroupBy)
	assert.NoError(t, err)

	assert.NoError(t, interactor.ReplyTTS(resTTS))
}

func TestBackendConnectionsGroupBySuccessAscData(t *testing.T) {
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	var desc []pgproto3.FieldDescription
	desc = append(desc, engine.TextOidFD("hostname"))
	desc = append(desc, engine.IntOidFD("count"))
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

	shards := []shard.ShardHostCtl{
		genShard(ctrl, "h1", "sh1", 1),
		genShard(ctrl, "h1", "sh3", 2),
		genShard(ctrl, "h2", "sh2", 3),
	}
	cmd := &spqrparser.Show{
		Cmd:     spqrparser.BackendConnectionsStr,
		GroupBy: spqrparser.GroupBy{Col: []spqrparser.ColumnRef{{ColName: "hostname"}}},
	}

	tts, err := engine.BackendConnectionsVirtualRelationScan(shards)
	assert.NoError(t, err)

	ftts, err := engine.FilterRows(tts, cmd.Where)
	assert.NoError(t, err)

	resTTS, err := engine.GroupBy(ftts, cmd.GroupBy)
	assert.NoError(t, err)

	assert.NoError(t, interactor.ReplyTTS(resTTS))
}

func TestBackendConnectionsGroupByFail(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)
	shards := []shard.ShardHostCtl{
		genShard(ctrl, "h1", "sh1", 1),
		genShard(ctrl, "h2", "sh2", 2),
		genShard(ctrl, "h1", "sh3", 3),
	}
	cmd := &spqrparser.Show{
		Cmd:     spqrparser.BackendConnectionsStr,
		GroupBy: spqrparser.GroupBy{Col: []spqrparser.ColumnRef{{ColName: "someColumn"}}},
	}
	tts, err := engine.BackendConnectionsVirtualRelationScan(shards)
	assert.NoError(err)

	ftts, err := engine.FilterRows(tts, cmd.Where)
	assert.NoError(err)

	_, err = engine.GroupBy(ftts, cmd.GroupBy)

	assert.ErrorContains(err, "failed to resolve 'someColumn' column offset")
}

func TestMakeSimpleResponseWithData(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	interactor := clientinteractor.NewPSQLInteractor(ca)
	info := []clientinteractor.SimpleResultRow{
		{Name: "test1", Value: "data1"},
		{Name: "test2", Value: "data2"},
	}
	data := clientinteractor.SimpleResultMsg{Header: "test header", Rows: info}

	desc := []pgproto3.FieldDescription{engine.TextOidFD("test header")}
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

	desc := []pgproto3.FieldDescription{engine.TextOidFD("test header")}
	gomock.InOrder(
		ca.EXPECT().Send(&pgproto3.RowDescription{Fields: desc}),
		ca.EXPECT().Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")}),
		ca.EXPECT().Send(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}),
	)
	err := interactor.MakeSimpleResponse(ctx, data)
	assert.Nil(t, err)
}

func TestKeyRangesSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	var desc []pgproto3.FieldDescription
	desc = append(desc, engine.TextOidFD("Key range ID"))
	desc = append(desc, engine.TextOidFD("Shard ID"))
	desc = append(desc, engine.TextOidFD("Distribution ID"))
	desc = append(desc, engine.TextOidFD("Lower bound"))
	desc = append(desc, engine.TextOidFD("Locked"))
	firstRow := pgproto3.DataRow{
		Values: [][]byte{
			[]byte("krid1"),
			[]byte("sh1"),
			[]byte("ds1"),
			[]byte("0"),
			[]byte("false"),
		},
	}
	secondRow := pgproto3.DataRow{
		Values: [][]byte{
			[]byte("krid2"),
			[]byte("sh1"),
			[]byte("ds1"),
			[]byte("30"),

			[]byte("true"),
		},
	}
	thirdRow := pgproto3.DataRow{
		Values: [][]byte{
			[]byte("krid3"),
			[]byte("sh1"),
			[]byte("ds2"),
			[]byte("3"),
			[]byte("false"),
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
	keyRanges := []*kr.KeyRange{
		{ID: "krid1",
			ShardID:      "sh1",
			Distribution: "ds1",
			LowerBound:   []any{0},
			ColumnTypes:  []string{qdb.ColumnTypeInteger},
		},
		{ID: "krid2",
			ShardID:      "sh1",
			Distribution: "ds1",
			LowerBound:   []any{30},
			ColumnTypes:  []string{qdb.ColumnTypeInteger},
		},
		{ID: "krid3",
			ShardID:      "sh1",
			Distribution: "ds2",
			LowerBound:   []any{3},
			ColumnTypes:  []string{qdb.ColumnTypeInteger},
		},
	}
	krLocks := []string{"krid2"}

	vp := engine.KeyRangeVirtualRelationScan(keyRanges, krLocks)
	err := interactor.ReplyTTS(vp)
	assert.Nil(t, err)
}
