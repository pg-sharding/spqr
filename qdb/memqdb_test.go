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
		func() { _ = memqdb.AddDataspace(ctx, mockDataspace) },
		func() { _ = memqdb.AddKeyRange(ctx, mockKeyRange) },
		func() { _ = memqdb.AddRouter(ctx, mockRouter) },
		func() { _ = memqdb.AddShard(ctx, mockShard) },
		func() { _ = memqdb.AddShardingRule(ctx, mockShardingRule) },
		func() {
			_ = memqdb.RecordTransferTx(ctx, mockDataTransferTransaction.FromShardId, mockDataTransferTransaction)
		},
		func() { _, _ = memqdb.ListDataspaces(ctx) },
		func() { _, _ = memqdb.ListKeyRanges(ctx) },
		func() { _, _ = memqdb.ListRouters(ctx) },
		func() { _, _ = memqdb.ListShardingRules(ctx) },
		func() { _, _ = memqdb.ListShards(ctx) },
		func() { _, _ = memqdb.GetKeyRange(ctx, mockKeyRange.KeyRangeID) },
		func() { _, _ = memqdb.GetShard(ctx, mockShard.ID) },
		func() { _, _ = memqdb.GetShardingRule(ctx, mockShardingRule.ID) },
		func() { _, _ = memqdb.GetTransferTx(ctx, mockDataTransferTransaction.FromShardId) },
		func() { _ = memqdb.ShareKeyRange(mockKeyRange.KeyRangeID) },
		func() { _ = memqdb.DropKeyRange(ctx, mockKeyRange.KeyRangeID) },
		func() { _ = memqdb.DropKeyRangeAll(ctx) },
		func() { _ = memqdb.DropShardingRule(ctx, mockShardingRule.ID) },
		func() { _, _ = memqdb.DropShardingRuleAll(ctx) },
		func() { _ = memqdb.RemoveTransferTx(ctx, mockDataTransferTransaction.FromShardId) },
		func() { _ = memqdb.LockRouter(ctx, mockRouter.ID) },
		func() {
			_, _ = memqdb.LockKeyRange(ctx, mockKeyRange.KeyRangeID)
			_ = memqdb.UnlockKeyRange(ctx, mockKeyRange.KeyRangeID)
		},
		func() { _ = memqdb.UpdateKeyRange(ctx, mockKeyRange) },
		func() { _ = memqdb.DeleteRouter(ctx, mockRouter.ID) },
	}
	for i := 0; i < 10; i++ {
		for _, m := range methods {
			wg.Add(1)
			go func(m func()) {
				m()
				wg.Done()
			}(m)
		}
		wg.Wait()

	}
	wg.Wait()
}
