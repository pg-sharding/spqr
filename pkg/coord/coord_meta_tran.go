package coord

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
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
		qdbStatements, err := mngr.CreateDistribution(ctx, mds)
		if err != nil {
			return nil, err
		}
		if len(qdbStatements) == 0 {
			return nil, fmt.Errorf("transaction chunk must have a qdb statement (createDistributionPrepare)")
		}
		result = append(result, qdbStatements...)
	}
	return result, nil
}

// prepare QDB CreateKeyRange commands
// TODO: unit tests
func createKeyRangePrepare(ctx context.Context,
	mngr meta.EntityMgr,
	gossip *proto.CreateKeyRangeGossip) ([]qdb.QdbStatement, error) {
	result := make([]qdb.QdbStatement, 0)
	ds, err := mngr.GetDistribution(ctx, gossip.KeyRangeInfo.DistributionId)
	if err != nil {
		return nil, err
	}
	krToCreate, err := kr.KeyRangeFromProto(gossip.KeyRangeInfo, ds.ColTypes)
	if err != nil {
		return nil, err
	}
	qdbStatements, err := mngr.CreateKeyRange(ctx, krToCreate)
	if err != nil {
		return nil, err
	}
	if len(qdbStatements) == 0 {
		return nil, fmt.Errorf("transaction chunk must have a qdb statement (createKeyRangePrepare)")
	}
	result = append(result, qdbStatements...)
	return result, nil
}

func transactionChunkToQdbStatements(ctx context.Context, mngr meta.EntityMgr, chunk *mtran.MetaTransactionChunk) ([]qdb.QdbStatement, error) {
	qdbCmds := make([]qdb.QdbStatement, 0, len(chunk.GossipRequests))
	for _, gossipCommand := range chunk.GossipRequests {
		cmdType, _ := mtran.GetGossipRequestType(gossipCommand)
		switch cmdType {
		case mtran.GR_CreateDistributionRequest:
			cmdList, err := createDistributionPrepare(ctx, mngr, gossipCommand.CreateDistribution)
			if err != nil {
				return nil, err
			}
			if len(cmdList) == 0 {
				return nil, fmt.Errorf("no QDB changes in gossip request:%d", cmdType)
			}
			qdbCmds = append(qdbCmds, cmdList...)
		case mtran.GR_CreateKeyRange:
			cmdList, err := createKeyRangePrepare(ctx, mngr, gossipCommand.CreateKeyRange)
			if err != nil {
				return nil, err
			}
			if len(cmdList) == 0 {
				return nil, fmt.Errorf("no QDB changes in gossip request:%d", cmdType)
			}
			qdbCmds = append(qdbCmds, cmdList...)
		// TODO: run handlers converting gossip commands to chunk with qdb commands
		default:
			return nil, fmt.Errorf("invalid meta gossip request:%d", cmdType)
		}
	}
	return qdbCmds, nil
}
