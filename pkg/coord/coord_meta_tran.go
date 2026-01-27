package coord

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

// Here are methods for converting gossip commands into QDB commands.

func createDistributionPrepare(ctx context.Context, mngr meta.EntityMgr, gossip *proto.CreateDistributionGossip) ([]qdb.QdbStatement, error) {
	result := make([]qdb.QdbStatement, 0, len(gossip.GetDistributions()))
	for _, ds := range gossip.GetDistributions() {
		mds, err := distributions.DistributionFromProto(ds)
		if err != nil {
			return nil, err
		}
		if qdbStatements, err := mngr.CreateDistribution(ctx, mds); err != nil {
			return nil, err
		} else {
			if len(qdbStatements) == 0 {
				return nil, fmt.Errorf("transaction chunk must have a qdb statement (createDistributionPrepare)")
			}
			result = append(result, qdbStatements...)
		}
	}
	return result, nil
}

func transactionChunkToQdbStatements(ctx context.Context, mngr meta.EntityMgr, chunk *mtran.MetaTransactionChunk) ([]qdb.QdbStatement, error) {
	qdbCmds := make([]qdb.QdbStatement, 0, len(chunk.GossipRequests))
	for _, gossipCommand := range chunk.GossipRequests {
		cmdType := mtran.GetGossipRequestType(gossipCommand)
		switch cmdType {
		case mtran.GR_CreateDistributionRequest:
			if cmdList, err := createDistributionPrepare(ctx, mngr, gossipCommand.CreateDistribution); err != nil {
				return nil, err
			} else {
				if len(cmdList) == 0 {
					return nil, fmt.Errorf("no QDB changes in gossip request:%d", cmdType)
				}
				qdbCmds = append(qdbCmds, cmdList...)
			}
		// TODO: run handlers converting gossip commands to chunk with qdb commands
		default:
			return nil, fmt.Errorf("invalid meta gossip request:%d", cmdType)
		}
	}
	return qdbCmds, nil
}
