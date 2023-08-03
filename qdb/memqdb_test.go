package qdb_test

import (
	"context"
	"sync"
	"testing"

	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

const MemQDBPath = "memqdb.json"

var mockDataspace *qdb.Dataspace = &qdb.Dataspace{"123"}
var mockShard *qdb.Shard = &qdb.Shard{
	ID:    "shard_id",
	Hosts: []string{"host1", "host2"},
}
var mockKeyRange *qdb.KeyRange = &qdb.KeyRange{
	LowerBound: []byte{1, 2},
	UpperBound: []byte{3, 4},
	ShardID:    mockShard.ID,
	KeyRangeID: "key_range_id",
}
var mockRouter *qdb.Router = &qdb.Router{
	Address: "address",
	ID:      "router_id",
	State:   qdb.CLOSED,
}
var mockShardingRule *qdb.ShardingRule = &qdb.ShardingRule{
	ID:        "sharding_rule_id",
	TableName: "fake_table",
	Entries: []qdb.ShardingRuleEntry{
		{
			Column: "i",
		},
	},
}
var mockDataTransferTransaction *qdb.DataTransferTransaction = &qdb.DataTransferTransaction{
	ToShardId:   mockShard.ID,
	FromShardId: mockShard.ID,
	FromTxName:  "fake_tx_1",
	ToTxName:    "fake_tx_2",
	FromStatus:  "fake_st_1",
	ToStatus:    "fake_st_2",
}

// must run with -race
func TestMemqdbRacing(t *testing.T) {
	assert := assert.New(t)

	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	assert.NoError(err)

	var wg sync.WaitGroup

	ctx := context.TODO()

	methods := []func(){
		func() { memqdb.AddDataspace(ctx, mockDataspace) },
		func() { memqdb.AddKeyRange(ctx, mockKeyRange) },
		func() { memqdb.AddRouter(ctx, mockRouter) },
		func() { memqdb.AddShard(ctx, mockShard) },
		func() { memqdb.AddShardingRule(ctx, mockShardingRule) },
		func() {
			memqdb.RecordTransferTx(ctx, mockDataTransferTransaction.FromShardId, mockDataTransferTransaction)
		},
		func() { memqdb.ListDataspaces(ctx) },
		func() { memqdb.ListKeyRanges(ctx) },
		func() { memqdb.ListRouters(ctx) },
		func() { memqdb.ListShardingRules(ctx) },
		func() { memqdb.ListShards(ctx) },
		func() { memqdb.GetKeyRange(ctx, mockKeyRange.KeyRangeID) },
		func() { memqdb.GetShard(ctx, mockShard.ID) },
		func() { memqdb.GetShardingRule(ctx, mockShardingRule.ID) },
		func() { memqdb.GetTransferTx(ctx, mockDataTransferTransaction.FromShardId) },
		func() { memqdb.ShareKeyRange(mockKeyRange.KeyRangeID) },
		func() { memqdb.DropKeyRange(ctx, mockKeyRange.KeyRangeID) },
		func() { memqdb.DropKeyRangeAll(ctx) },
		func() { memqdb.DropShardingRule(ctx, mockShardingRule.ID) },
		func() { memqdb.DropShardingRuleAll(ctx) },
		func() { memqdb.RemoveTransferTx(ctx, mockDataTransferTransaction.FromShardId) },
	}

	for _, m := range methods {
		wg.Add(1)
		go func(m func()) {
			m()
			wg.Done()
		}(m)
	}

	wg.Wait()
}
