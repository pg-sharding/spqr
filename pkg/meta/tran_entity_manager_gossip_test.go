package meta

import (
	"context"
	"testing"

	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

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

func TestTranCreateSequence(t *testing.T) {
	is := assert.New(t)
	ctrl := gomock.NewController(t)
	mmgr := mockmgr.NewMockEntityMgr(ctrl)
	ctx := context.Background()

	tranMngr := NewTranEntityManager(mmgr)
	err := tranMngr.CreateSequence(ctx, "seq1", 1)
	is.NoError(err)
	err = tranMngr.CreateSequence(ctx, "seq2", 2)
	is.NoError(err)
	err = tranMngr.CreateSequence(ctx, "seq1", 2)
	is.Error(err)

	//check statements in manager state
	expected := []*proto.MetaTransactionGossipCommand{
		{CreateSequence: &proto.CreateSequenceGossip{
			SeqName:      "seq1",
			InitialValue: 1,
		},
		},
		{
			CreateSequence: &proto.CreateSequenceGossip{
				SeqName:      "seq2",
				InitialValue: 2,
			},
		},
	}
	is.Equal(expected, tranMngr.state.Chunk.GossipRequests)
}
