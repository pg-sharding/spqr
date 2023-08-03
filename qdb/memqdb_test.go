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

	methods := []func() error{
		func() error { return memqdb.AddDataspace(ctx, mockDataspace) },
		func() error { return memqdb.AddKeyRange(ctx, mockKeyRange) },
		func() error { return memqdb.AddRouter(ctx, mockRouter) },
		func() error { return memqdb.AddShard(ctx, mockShard) },
		func() error { return memqdb.AddShardingRule(ctx, mockShardingRule) },
		func() error {
			return memqdb.RecordTransferTx(ctx, mockDataTransferTransaction.FromShardId, mockDataTransferTransaction)
		},
		func() error {
			_, err_local := memqdb.ListDataspaces(ctx)
			return err_local
		},
		func() error {
			_, err_local := memqdb.ListKeyRanges(ctx)
			return err_local
		},
		func() error {
			_, err_local := memqdb.ListRouters(ctx)
			return err_local
		},
		func() error {
			_, err_local := memqdb.ListShardingRules(ctx)
			return err_local
		},
		func() error {
			_, err_local := memqdb.ListShards(ctx)
			return err_local
		},
		func() error {
			_, err_local := memqdb.GetKeyRange(ctx, mockKeyRange.KeyRangeID)
			return err_local
		},
		func() error {
			_, err_local := memqdb.GetShard(ctx, mockShard.ID)
			return err_local
		},
		func() error {
			_, err_local := memqdb.GetShardingRule(ctx, mockShardingRule.ID)
			return err_local
		},
		func() error {
			_, err_local := memqdb.GetTransferTx(ctx, mockDataTransferTransaction.FromShardId)
			return err_local
		},
		func() error { return memqdb.ShareKeyRange(mockKeyRange.KeyRangeID) },
		func() error { return memqdb.DropKeyRange(ctx, mockKeyRange.KeyRangeID) },
		func() error { return memqdb.DropKeyRangeAll(ctx) },
		func() error { return memqdb.DropShardingRule(ctx, mockShardingRule.ID) },
		func() error {
			_, err_local := memqdb.DropShardingRuleAll(ctx)
			return err_local
		},
		func() error { return memqdb.RemoveTransferTx(ctx, mockDataTransferTransaction.FromShardId) },
	}

	for i := 0; i < 10; i++ {
		for _, m := range methods {
			wg.Add(1)
			go func(m func() error) {
				_ = m()
				wg.Done()
			}(m)
		}
	}
	wg.Wait()
}
