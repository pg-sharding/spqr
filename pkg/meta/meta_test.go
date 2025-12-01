package meta_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/meta"
	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
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
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*config.Shard{})
	//
	_, err = meta.ProcessCreate(ctx, &statement, mngr)
	assert.ErrorContains(t, err, "ds1.DEFAULT is reserved")
}

func TestCreteDistrWithDefaultShardSuccess(t *testing.T) {
	ctx := context.Background()
	statement := spqrparser.DistributionDefinition{
		ID:           "dbTestDefault",
		ColTypes:     []string{"integer"},
		DefaultShard: "sh1",
	}
	memqdb, err := prepareDB(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*config.Shard{})

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
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, map[string]*config.Shard{})

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
