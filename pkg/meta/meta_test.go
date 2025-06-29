package meta_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/txstatus"
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
	if err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", nil)); err != nil {
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
	ctrl := gomock.NewController(t)
	ca := mockcl.NewMockRouterClient(ctrl)
	interactor := clientinteractor.NewPSQLInteractor(ca)
	statement := spqrparser.KeyRangeDefinition{
		ShardID:      "sh1",
		KeyRangeID:   "ds1.DEFAULT",
		Distribution: "ds1",
		LowerBound: &spqrparser.KeyRangeBound{
			Pivots: [][]byte{
				{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				[]byte("a"),
			},
		},
	}
	memqdb, err := prepareDB(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil)
	gomock.InOrder(
		ca.EXPECT().Send(&pgproto3.ErrorResponse{Severity: "ERROR",
			Message: "Error kay range ds1.DEFAULT is reserved",
		}),
		ca.EXPECT().Send(&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)}),
	)
	res := meta.ProcessCreate(ctx, &statement, mngr, interactor)
	assert.Nil(t, res)
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
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil)

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
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil)

	actualDistribution, err := meta.CreateNonReplicatedDistribution(ctx, statement, mngr)
	assert.Nil(t, actualDistribution)
	assert.Equal(t, err, fmt.Errorf("shard '%s' does not exists", "notExistShard"))

}
