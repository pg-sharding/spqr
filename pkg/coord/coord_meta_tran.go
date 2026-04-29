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
	krToCreate, err := kr.KeyRangeFromProto(gossip.KeyRangeInfo, gossip.ColumnTypes)
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

func updateKeyRangePrepare(ctx context.Context,
	mngr meta.EntityMgr,
	gossip *proto.UpdateKeyRangeGossip) ([]qdb.QdbStatement, error) {
	result := make([]qdb.QdbStatement, 0)
	krToCreate, err := kr.KeyRangeFromProto(gossip.KeyRangeInfo, gossip.ColumnTypes)
	if err != nil {
		return nil, err
	}
	qdbStatements, err := mngr.UpdateKeyRange(ctx, krToCreate)
	if err != nil {
		return nil, err
	}
	if len(qdbStatements) == 0 {
		return nil, fmt.Errorf("transaction chunk must have a qdb statement (createKeyRangePrepare)")
	}
	result = append(result, qdbStatements...)
	return result, nil
}

func dropKeyRangePrepare(ctx context.Context, mngr meta.EntityMgr, gossip *proto.DropKeyRangeGossip) ([]qdb.QdbStatement, error) {
	result := make([]qdb.QdbStatement, 0, len(gossip.GetId()))
	for _, idKeyRange := range gossip.GetId() {
		qdbStatements, err := mngr.DropKeyRange(ctx, idKeyRange)
		if err != nil {
			return nil, err
		}
		if len(qdbStatements) > 0 {
			result = append(result, qdbStatements...)
		}
	}
	return result, nil
}

func transactionChunkToQdbStatements(ctx context.Context, mngr meta.EntityMgr, chunk *mtran.MetaTransactionChunk) ([]qdb.QdbStatement, error) {
	qdbCmds := make([]qdb.QdbStatement, 0, len(chunk.GossipRequests))
	for _, gossipCommand := range chunk.GossipRequests {
		var cmdList []qdb.QdbStatement
		var err error
		cmdType, _ := mtran.GetGossipRequestType(gossipCommand)
		switch cmdType {
		case mtran.GRCreateDistributionRequest:
			cmdList, err = createDistributionPrepare(ctx, mngr, gossipCommand.CreateDistribution)
		case mtran.GRCreateKeyRange:
			cmdList, err = createKeyRangePrepare(ctx, mngr, gossipCommand.CreateKeyRange)
		case mtran.GRUpdateKeyRange:
			cmdList, err = updateKeyRangePrepare(ctx, mngr, gossipCommand.UpdateKeyRange)
		case mtran.GRDropKeyRange:
			cmdList, err = dropKeyRangePrepare(ctx, mngr, gossipCommand.DropKeyRange)
		// TODO: run handlers converting gossip commands to chunk with qdb commands
		default:
			return nil, fmt.Errorf("invalid meta gossip request:%d", cmdType)
		}
		if err != nil {
			return nil, err
		}
		qdbCmds = append(qdbCmds, cmdList...)
	}
	return qdbCmds, nil
}
