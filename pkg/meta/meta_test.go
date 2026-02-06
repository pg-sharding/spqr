package meta_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/meta"
	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/qdb"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

const MemQDBPath = ""

var mockShard1 = &qdb.Shard{
	ID:       "sh1",
	RawHosts: []string{"host1", "host2"},
}
var mockShard2 = &qdb.Shard{
	ID:       "sh2",
	RawHosts: []string{"host3", "host4"},
}

// Mock implementation of ConnectionMgr for testing
type mockConnectionMgr struct {
	clientPoolForeachFn func(func(client.ClientInfo) error) error
}

func (m *mockConnectionMgr) ClientPoolForeach(cb func(client.ClientInfo) error) error {
	return m.clientPoolForeachFn(cb)
}

func (m *mockConnectionMgr) InstanceHealthChecks() map[string]tsa.CachedCheckResult {
	return nil
}

func (m *mockConnectionMgr) TsaCacheEntries() map[pool.TsaKey]pool.CachedEntry {
	return nil
}

func (m *mockConnectionMgr) TotalTcpCount() int64 {
	return 0
}

func (m *mockConnectionMgr) ActiveTcpCount() int64 {
	return 0
}

func (m *mockConnectionMgr) TotalCancelCount() int64 {
	return 0
}

func (m *mockConnectionMgr) ForEachPool(cb func(pool.Pool) error) error {
	return nil
}

func (m *mockConnectionMgr) ForEach(cb func(shard.ShardHostCtl) error) error {
	return nil
}

// Implement client.Pool methods
func (m *mockConnectionMgr) Put(cl client.Client) error {
	return nil
}

func (m *mockConnectionMgr) Pop(id uint) (bool, error) {
	return false, nil
}

func (m *mockConnectionMgr) Shutdown() error {
	return nil
}

// Minimal mock implementation of ClientInfo for testing - only implements RAddr
type mockClientInfo struct {
	raddr string
}

func (m *mockClientInfo) RAddr() string {
	return m.raddr
}

func prepareDB(ctx context.Context) (*qdb.MemQDB, error) {
	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	if err != nil {
		return nil, err
	}
	var chunk []qdb.QdbStatement
	if chunk, err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil)); err != nil {
		return nil, err
	}
	if err = memqdb.ExecNoTransaction(ctx, chunk); err != nil {
		return nil, err
	}
	if err = memqdb.AddShard(ctx, mockShard1); err != nil {
		return nil, err
	}
	if err = memqdb.AddShard(ctx, mockShard2); err != nil {
		return nil, err
	}
	return memqdb, nil
}

func TestNoManualCreateDefaultShardKeyRange(t *testing.T) {
	ctx := context.Background()
	statement := spqrparser.KeyRangeDefinition{
		ShardID:      "sh1",
		KeyRangeID:   "ds1.DEFAULT",
		Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
		LowerBound: &spqrparser.KeyRangeBound{
			Pivots: [][]byte{
				{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				[]byte("a"),
			},
		},
	}
	memqdb, err := prepareDB(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*config.Shard{}, false)
	//
	_, err = meta.ProcessCreate(ctx, &statement, mngr)
	assert.ErrorContains(t, err, "ds1.DEFAULT is reserved")
}

func TestCreateDistrWithDefaultShardSuccess(t *testing.T) {
	ctx := context.Background()
	statement := spqrparser.DistributionDefinition{
		ID:           "dbTestDefault",
		ColTypes:     []string{"integer"},
		DefaultShard: "sh1",
	}
	memqdb, err := prepareDB(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*config.Shard{}, false)

	expectedDistribution := distributions.NewDistribution("dbTestDefault", []string{"integer"})
	actualDistribution, err := meta.CreateNonReplicatedDistribution(ctx, statement, mngr)
	assert.Nil(t, err)
	assert.Equal(t, actualDistribution, expectedDistribution)

	expectedKr := &qdb.KeyRange{
		LowerBound:     [][]byte{{255, 255, 255, 255, 255, 255, 255, 255, 255, 1}},
		ShardID:        "sh1",
		KeyRangeID:     "dbTestDefault.DEFAULT",
		DistributionId: "dbTestDefault",
	}
	actualKr, errKr := memqdb.GetKeyRange(ctx, "dbTestDefault.DEFAULT")
	assert.Nil(t, errKr)
	assert.Equal(t, actualKr, expectedKr)
}
func TestCreteDistrWithDefaultShardFail1(t *testing.T) {
	ctx := context.Background()
	statement := spqrparser.DistributionDefinition{
		ID:           "dbTestDefault",
		ColTypes:     []string{"integer"},
		DefaultShard: "notExistShard",
	}
	memqdb, err := prepareDB(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*config.Shard{}, false)

	actualDistribution, err := meta.CreateNonReplicatedDistribution(ctx, statement, mngr)
	assert.Nil(t, actualDistribution)
	assert.Equal(t, err, fmt.Errorf("shard '%s' does not exist", "notExistShard"))

}

func TestMoveKeyRangeReplyIncludesHint(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	mmgr := mockmgr.NewMockEntityMgr(ctrl)
	cl := mockcl.NewMockRouterClient(ctrl)

	cl.EXPECT().Rule().Return((*config.FrontendRule)(nil)).AnyTimes()

	mmgr.EXPECT().
		Move(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, move *kr.MoveKeyRange) error {
			assert.Equal(t, "krid3", move.Krid)
			assert.Equal(t, "sh2", move.ShardId)
			return nil
		})

	rows := make([]string, 0, 4)
	cl.EXPECT().
		Send(gomock.Any()).
		AnyTimes().
		DoAndReturn(func(msg pgproto3.BackendMessage) error {
			if dr, ok := msg.(*pgproto3.DataRow); ok {
				if len(dr.Values) > 0 {
					rows = append(rows, string(dr.Values[0]))
				}
			}
			return nil
		})

	stmt := &spqrparser.MoveKeyRange{
		DestShardID: "sh2",
		KeyRangeID:  "krid3",
	}

	tts, err := meta.ProcMetadataCommand(ctx, stmt, mmgr, nil, cl.Rule(), nil, false)
	assert.NoError(t, err)

	cli := clientinteractor.NewPSQLInteractor(cl)
	_ = cli.ReplyTTS(tts)

	assert.Contains(t, rows, "move key range krid3 to shard sh2")
	assert.Contains(t, rows, "HINT: MOVE KEY RANGE only updates metadata. Use REDISTRIBUTE KEY RANGE to also migrate data.")
}

func TestShowRoutersOutputFormat(t *testing.T) {
	// Test that SHOW routers returns proper output structure with all columns
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	// Create mock EntityMgr
	mmgr := mockmgr.NewMockEntityMgr(ctrl)

	// Set up routers
	routers := []*topology.Router{
		{ID: "router1", Address: "router1.example.com:7000", State: qdb.OPENED},
		{ID: "router2", Address: "router2.example.com:7000", State: qdb.CLOSED},
	}

	// Mock ListRouters
	mmgr.EXPECT().
		ListRouters(ctx).
		Return(routers, nil)

	// Create a minimal mock ConnectionMgr that only implements ClientPoolForeach
	// For this test, no clients will be returned (empty client pool)
	connMgr := &mockConnectionMgr{
		clientPoolForeachFn: func(cb func(client.ClientInfo) error) error {
			// No clients - all routers will have 0 connections
			return nil
		},
	}

	// Call ProcessShow
	stmt := &spqrparser.Show{
		Cmd: spqrparser.RoutersStr,
	}

	tts, err := meta.ProcessShow(ctx, stmt, mmgr, connMgr, false)
	assert.NoError(t, err)

	// Verify the table descriptor (columns)
	assert.NotNil(t, tts)
	assert.Equal(t, 5, len(tts.Desc), "should have 5 columns")
	assert.Equal(t, "router", string(tts.Desc[0].Name))
	assert.Equal(t, "status", string(tts.Desc[1].Name))
	assert.Equal(t, "client_connections", string(tts.Desc[2].Name))
	assert.Equal(t, "version", string(tts.Desc[3].Name))
	assert.Equal(t, "metadata_version", string(tts.Desc[4].Name))

	// Verify data rows
	assert.Equal(t, 2, len(tts.Raw), "should have 2 router rows")

	// Check first router
	row1 := tts.Raw[0]
	assert.Equal(t, "router1.example.com:7000", string(row1[0]))
	assert.Equal(t, "OPENED", string(row1[1]))
	assert.Equal(t, "0", string(row1[2]))
	assert.NotEmpty(t, string(row1[3]))
	assert.Equal(t, "N/A", string(row1[4]))

	// Check second router
	row2 := tts.Raw[1]
	assert.Equal(t, "router2.example.com:7000", string(row2[0]))
	assert.Equal(t, "CLOSED", string(row2[1]))
	assert.Equal(t, "0", string(row2[2]))
	assert.NotEmpty(t, string(row2[3]))
	assert.Equal(t, "N/A", string(row2[4]))
}
