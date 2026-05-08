package meta_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/engine"
	"github.com/pg-sharding/spqr/pkg/meta"
	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	"github.com/pg-sharding/spqr/pkg/models/acl"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/qdb"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	"github.com/pg-sharding/spqr/router/rfqn"
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
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)
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
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	expectedDistribution := distributions.NewDistribution("dbTestDefault", []string{"integer"})
	actualDistribution, err := meta.CreateNonReplicatedDistribution(ctx, statement, mngr)
	assert.Nil(t, err)
	assert.Equal(t, actualDistribution, expectedDistribution)

	expectedKr := &qdb.KeyRange{
		LowerBound:     [][]byte{{255, 255, 255, 255, 255, 255, 255, 255, 255, 1}},
		ShardID:        "sh1",
		KeyRangeID:     "dbTestDefault.DEFAULT",
		DistributionId: "dbTestDefault",
		Version:        1,
	}
	actualKr, errKr := memqdb.GetKeyRange(ctx, "dbTestDefault.DEFAULT")
	assert.Nil(t, errKr)
	assert.Equal(t, actualKr, expectedKr)
}

func TestCreateShardValidatesReachableHosts(t *testing.T) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer func() {
		_ = listener.Close()
	}()

	statement := spqrparser.ShardDefinition{
		Id:      "sh-new",
		Options: []spqrparser.GenericOption{{Name: "host", Arg: listener.Addr().String()}},
	}

	memqdb, err := prepareDB(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	_, err = meta.ProcessCreate(ctx, &statement, mngr)
	assert.NoError(t, err)

	_, err = memqdb.GetShard(ctx, "sh-new")
	assert.NoError(t, err)
}

func TestCreateShardRejectsUnreachableHosts(t *testing.T) {
	ctx := context.Background()
	addr := func() string {
		const maxAttempts = 10
		for i := 0; i < maxAttempts; i++ {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			assert.NoError(t, err)
			candidate := listener.Addr().String()
			_ = listener.Close()

			conn, dialErr := net.DialTimeout("tcp", candidate, 100*time.Millisecond)
			if dialErr != nil {
				return candidate
			}
			_ = conn.Close()
		}
		t.Fatalf("failed to get an unreachable tcp address")
		return ""
	}()

	statement := spqrparser.ShardDefinition{
		Id:      "sh-bad",
		Options: []spqrparser.GenericOption{{Name: "host", Arg: addr}},
	}

	memqdb, err := prepareDB(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	_, err = meta.ProcessCreate(ctx, &statement, mngr)
	assert.ErrorContains(t, err, "not reachable")

	_, err = memqdb.GetShard(ctx, "sh-bad")
	assert.Error(t, err)
}

func TestCreateShardAllowsGrpcWrappedUnknownShardError(t *testing.T) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer func() {
		_ = listener.Close()
	}()

	statement := spqrparser.ShardDefinition{
		Id:      "sh-new",
		Options: []spqrparser.GenericOption{{Name: "host", Arg: listener.Addr().String()}},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mngr := mockmgr.NewMockEntityMgr(ctrl)
	mngr.EXPECT().GetShard(ctx, "sh-new").Return(nil, fmt.Errorf("rpc error: code = Unknown desc = unknown shard sh-new"))
	mngr.EXPECT().AddDataShard(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, shard *topology.DataShard) error {
		assert.Equal(t, "sh-new", shard.ID)
		assert.Equal(t, []string{listener.Addr().String()}, shard.Hosts())
		return nil
	})

	_, err = meta.ProcessCreate(ctx, &statement, mngr)
	assert.NoError(t, err)
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
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

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
			assert.Equal(t, "krid3", move.KeyRangeID)
			assert.Equal(t, "sh2", move.ShardID)
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

func TestCreateReferenceRelation(t *testing.T) {
	t.Run("shard is non defined", func(t *testing.T) {
		is := assert.New(t)
		ctx := context.TODO()
		memqdb, err := prepareDbTestValidate(ctx)
		assert.NoError(t, err)
		mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

		createCmd := &spqrparser.ReferenceRelationDefinition{
			TableName: &rfqn.RelationFQN{
				RelationName: "xtab",
			},
		}
		tupleslotExpected := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("create reference table"),
			Raw: [][][]byte{
				{
					fmt.Appendf(nil, "table    -> xtab"),
				},
				{
					fmt.Appendf(nil, "shard id -> sh1,sh2"),
				},
			},
		}
		tupleslotActual, err := meta.CreateReferenceRelation(ctx, mngr, createCmd)
		is.NoError(err)
		is.Equal(tupleslotExpected, tupleslotActual)
		relActual, err := mngr.GetReferenceRelation(ctx, rfqn.RelationFQNFromFullName("", "xtab"))
		relExpected := rrelation.ReferenceRelation{
			RelationName:          rfqn.RelationFQNFromFullName("", "xtab"),
			ShardIDs:              []string{"sh1", "sh2"},
			SchemaVersion:         1,
			ColumnSequenceMapping: make(map[string]string),
			ACL:                   []acl.ACLItem{},
		}
		is.NoError(err)
		is.Equal(relExpected, *relActual)
	})
	t.Run("shard is defined", func(t *testing.T) {
		is := assert.New(t)
		ctx := context.TODO()
		memqdb, err := prepareDbTestValidate(ctx)
		assert.NoError(t, err)
		mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

		createCmd := &spqrparser.ReferenceRelationDefinition{
			TableName: &rfqn.RelationFQN{
				RelationName: "xtab",
			},
			ShardIDs: []string{"sh2"},
		}
		tupleslotExpected := &tupleslot.TupleTableSlot{
			Desc: engine.GetVPHeader("create reference table"),
			Raw: [][][]byte{
				{
					fmt.Appendf(nil, "table    -> xtab"),
				},
				{
					fmt.Appendf(nil, "shard id -> sh2"),
				},
			},
		}
		tupleslotActual, err := meta.CreateReferenceRelation(ctx, mngr, createCmd)
		is.NoError(err)
		is.Equal(tupleslotExpected, tupleslotActual)
		relActual, err := mngr.GetReferenceRelation(ctx, rfqn.RelationFQNFromFullName("", "xtab"))
		relExpected := rrelation.ReferenceRelation{
			RelationName:          rfqn.RelationFQNFromFullName("", "xtab"),
			ShardIDs:              []string{"sh2"},
			SchemaVersion:         1,
			ColumnSequenceMapping: make(map[string]string),
			ACL:                   []acl.ACLItem{},
		}
		is.NoError(err)
		is.Equal(relExpected, *relActual)
	})
}

func TestRenameDistributionColumnSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mmgr := mockmgr.NewMockEntityMgr(ctrl)

	existingDist := &distributions.Distribution{
		Id:       "ds1",
		ColTypes: []string{"integer"},
		Relations: map[string]*distributions.DistributedRelation{
			"t": {
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "SITEID", HashFunction: "identity"},
				},
			},
		},
	}

	mmgr.EXPECT().
		GetDistribution(gomock.Any(), "ds1").
		Return(existingDist, nil)

	mmgr.EXPECT().
		AlterDistributedRelationDistributionKey(gomock.Any(), "ds1", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, rel *rfqn.RelationFQN, newKey []distributions.DistributionKeyEntry) error {
			assert.Equal(t, "t", rel.RelationName)
			assert.Len(t, newKey, 1)
			assert.Equal(t, "siteid", newKey[0].Column)
			assert.Equal(t, "identity", newKey[0].HashFunction)
			return nil
		})

	stmt := &spqrparser.Alter{
		Element: &spqrparser.AlterDistribution{
			Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
			Element: &spqrparser.AlterRelationV2{
				RelationName: &rfqn.RelationFQN{RelationName: "t"},
				Element: &spqrparser.RenameDistributionColumn{
					OldName: "SITEID",
					NewName: "siteid",
				},
			},
		},
	}

	tts, err := meta.ProcMetadataCommand(ctx, stmt, mmgr, nil, nil, nil, false)
	assert.NoError(t, err)
	assert.NotNil(t, tts)
}

func TestRenameDistributionColumnNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mmgr := mockmgr.NewMockEntityMgr(ctrl)

	existingDist := &distributions.Distribution{
		Id:       "ds1",
		ColTypes: []string{"integer"},
		Relations: map[string]*distributions.DistributedRelation{
			"t": {
				DistributionKey: []distributions.DistributionKeyEntry{
					{Column: "id", HashFunction: "identity"},
				},
			},
		},
	}

	mmgr.EXPECT().
		GetDistribution(gomock.Any(), "ds1").
		Return(existingDist, nil)

	stmt := &spqrparser.Alter{
		Element: &spqrparser.AlterDistribution{
			Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
			Element: &spqrparser.AlterRelationV2{
				RelationName: &rfqn.RelationFQN{RelationName: "t"},
				Element: &spqrparser.RenameDistributionColumn{
					OldName: "nonexistent",
					NewName: "new_col",
				},
			},
		},
	}

	tts, err := meta.ProcMetadataCommand(ctx, stmt, mmgr, nil, nil, nil, false)
	assert.Nil(t, tts)
	assert.ErrorContains(t, err, "column \"nonexistent\" not found in distribution key")
}

func TestRenameDistributionColumnRelationNotAttached(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mmgr := mockmgr.NewMockEntityMgr(ctrl)

	existingDist := &distributions.Distribution{
		Id:        "ds1",
		ColTypes:  []string{"integer"},
		Relations: map[string]*distributions.DistributedRelation{},
	}

	mmgr.EXPECT().
		GetDistribution(gomock.Any(), "ds1").
		Return(existingDist, nil)

	stmt := &spqrparser.Alter{
		Element: &spqrparser.AlterDistribution{
			Distribution: &spqrparser.DistributionSelector{ID: "ds1"},
			Element: &spqrparser.AlterRelationV2{
				RelationName: &rfqn.RelationFQN{RelationName: "missing_rel"},
				Element: &spqrparser.RenameDistributionColumn{
					OldName: "col",
					NewName: "new_col",
				},
			},
		},
	}

	tts, err := meta.ProcMetadataCommand(ctx, stmt, mmgr, nil, nil, nil, false)
	assert.Nil(t, tts)
	assert.ErrorContains(t, err, "relation \"missing_rel\" is not attached to distribution \"ds1\"")
}
