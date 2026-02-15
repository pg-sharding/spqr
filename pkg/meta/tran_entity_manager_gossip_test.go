package meta

import (
	"context"
	"testing"

	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// here are tests of command list in state TranEntityManager after exec write command
const MemQDBPath = ""

func prepareDbTestValidate(ctx context.Context) (*qdb.MemQDB, error) {
	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	if err != nil {
		return nil, err
	}
	var chunk []qdb.QdbStatement
	if chunk, err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds1", []string{qdb.ColumnTypeInteger})); err != nil {
		return nil, err
	}
	if err = memqdb.ExecNoTransaction(ctx, chunk); err != nil {
		return nil, err
	}
	if chunk, err = memqdb.CreateDistribution(ctx, qdb.NewDistribution("ds2", []string{qdb.ColumnTypeInteger})); err != nil {
		return nil, err
	}
	if err = memqdb.ExecNoTransaction(ctx, chunk); err != nil {
		return nil, err
	}
	return memqdb, nil
}

func TestTranDropKeyRange(t *testing.T) {
	is := assert.New(t)
	ctrl := gomock.NewController(t)
	mmgr := mockmgr.NewMockEntityMgr(ctrl)
	ctx := context.Background()

	tranMngr := NewTranEntityManager(mmgr)
	err := tranMngr.DropKeyRange(ctx, "kr2")
	is.NoError(err)
	err = tranMngr.DropKeyRange(ctx, "kr1")
	is.NoError(err)

	//check statements in manager state
	expected := []*proto.MetaTransactionGossipCommand{
		{DropKeyRange: &proto.DropKeyRangeGossip{
			Id: []string{"kr2"},
		},
		},
		{DropKeyRange: &proto.DropKeyRangeGossip{
			Id: []string{"kr1"},
		},
		},
	}
	is.Equal(expected, tranMngr.state.Chunk.GossipRequests)
}
