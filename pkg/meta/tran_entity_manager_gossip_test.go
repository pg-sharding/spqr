package meta

import (
	"context"
	"testing"

	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/stretchr/testify/assert"
)

func TestTranDropKeyRange(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()

	tranMngr := NewTranEntityManager(nil)
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
	ctx := context.Background()

	tranMngr := NewTranEntityManager(nil)
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
