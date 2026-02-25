package qdb

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackMemqdbCommands(t *testing.T) {
	is := assert.New(t)

	memqdb, err := NewMemQDB("")
	is.NoError(err)
	relation := &DistributedRelation{
		Name: "r1",
		DistributionKey: []DistributionKeyEntry{
			{
				Column:       "c1",
				HashFunction: "",
			},
		},
	}
	distribution1 := NewDistribution("ds1", []string{ColumnTypeUinteger})
	distribution2 := NewDistribution("ds2", []string{ColumnTypeInteger})
	dataDistribution1, err := json.Marshal(distribution1)
	is.NoError(err)
	dataDistribution2, err := json.Marshal(distribution2)
	is.NoError(err)
	t.Run("test happy path pack commands", func(t *testing.T) {
		commands := []QdbStatement{
			{CmdType: CMD_PUT, Key: distribution1.ID, Value: string(dataDistribution1), Extension: MapDistributions},
			{CmdType: CMD_PUT, Key: relation.Name, Value: distribution1.ID, Extension: MapRelationDistribution},
			{CmdType: CMD_PUT, Key: distribution2.ID, Value: string(dataDistribution2), Extension: MapDistributions},
		}
		actual, err := memqdb.packMemqdbCommands(commands)
		is.NoError(err)
		expected := []Command{
			NewUpdateCommand(memqdb.Distributions, distribution1.ID, distribution1),
			NewUpdateCommand(memqdb.RelationDistribution, relation.Name, distribution1.ID),
			NewUpdateCommand(memqdb.Distributions, distribution2.ID, distribution2),
		}
		is.Equal(expected, actual)
	})
	t.Run("fail: invalid extension", func(t *testing.T) {
		commands := []QdbStatement{
			{CmdType: CMD_PUT, Key: distribution1.ID, Value: string(dataDistribution1), Extension: MapDistributions},
			{CmdType: CMD_PUT, Key: distribution2.ID, Value: string(dataDistribution2), Extension: "testMap1"},
		}
		_, err := memqdb.packMemqdbCommands(commands)
		is.EqualError(err, "not implemented for transaction memqdb part testMap1")
	})
}

func TestMemQdbTransactions(t *testing.T) {
	is := assert.New(t)
	ctx := context.TODO()

	distribution1 := NewDistribution("ds1", []string{ColumnTypeUinteger})
	distribution2 := NewDistribution("ds2", []string{ColumnTypeInteger})
	distribution3 := NewDistribution("ds3", []string{ColumnTypeVarchar})
	dataDistribution1, err := json.Marshal(distribution1)
	is.NoError(err)
	dataDistribution2, err := json.Marshal(distribution2)
	is.NoError(err)
	dataDistribution3, err := json.Marshal(distribution3)
	is.NoError(err)
	t.Run("test Begin tran", func(t *testing.T) {
		t.Run("simple begin tran success", func(t *testing.T) {
			memqdb, err := NewMemQDB("")
			is.NoError(err)
			tran, err := NewTransaction()
			is.NoError(err)
			err = memqdb.BeginTransaction(ctx, tran)
			is.NoError(err)
			is.Equal(tran.transactionId, memqdb.activeTransaction)
		})
		t.Run("2 begin tran success", func(t *testing.T) {
			memqdb, err := NewMemQDB("")
			is.NoError(err)
			tran1, err := NewTransaction()
			is.NoError(err)
			err = memqdb.BeginTransaction(ctx, tran1)
			is.NoError(err)
			is.Equal(tran1.transactionId, memqdb.activeTransaction)

			tran2, err := NewTransaction()
			is.NoError(err)
			err = memqdb.BeginTransaction(ctx, tran2)
			is.NoError(err)
			is.Equal(tran2.transactionId, memqdb.activeTransaction)
		})
	})
	t.Run("test exec no tran", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			memqdb, err := NewMemQDB("")
			is.NoError(err)
			commands := []QdbStatement{
				{CmdType: CMD_PUT, Key: distribution1.ID, Value: string(dataDistribution1), Extension: MapDistributions},
				{CmdType: CMD_PUT, Key: distribution2.ID, Value: string(dataDistribution2), Extension: MapDistributions},
			}
			err = memqdb.ExecNoTransaction(ctx, commands)
			is.NoError(err)
			actual, err := memqdb.ListDistributions(ctx)
			is.NoError(err)
			is.Equal([]*Distribution{distribution1, distribution2}, actual)
		})
		t.Run("2 sequential runs", func(t *testing.T) {
			memqdb, err := NewMemQDB("")
			is.NoError(err)
			commands := []QdbStatement{
				{CmdType: CMD_PUT, Key: distribution1.ID, Value: string(dataDistribution1), Extension: MapDistributions},
				{CmdType: CMD_PUT, Key: distribution2.ID, Value: string(dataDistribution2), Extension: MapDistributions},
			}
			err = memqdb.ExecNoTransaction(ctx, commands)
			is.NoError(err)

			commands = []QdbStatement{
				{CmdType: CMD_PUT, Key: distribution3.ID, Value: string(dataDistribution3), Extension: MapDistributions},
			}
			err = memqdb.ExecNoTransaction(ctx, commands)
			is.NoError(err)

			actual, err := memqdb.ListDistributions(ctx)
			is.NoError(err)
			is.Equal([]*Distribution{distribution1, distribution2, distribution3}, actual)
		})
	})
	t.Run("test commit tran", func(t *testing.T) {
		t.Run("happy path commit tran", func(t *testing.T) {
			memqdb, err := NewMemQDB("")
			is.NoError(err)
			tran, err := NewTransaction()
			is.NoError(err)
			err = memqdb.BeginTransaction(ctx, tran)
			is.NoError(err)
			commands := []QdbStatement{
				{CmdType: CMD_PUT, Key: distribution1.ID, Value: string(dataDistribution1), Extension: MapDistributions},
				{CmdType: CMD_PUT, Key: distribution2.ID, Value: string(dataDistribution2), Extension: MapDistributions},
			}
			err = tran.Append(commands)
			is.NoError(err)
			err = memqdb.CommitTransaction(ctx, tran)
			is.NoError(err)
			actual, err := memqdb.ListDistributions(ctx)
			is.NoError(err)
			is.Equal([]*Distribution{distribution1, distribution2}, actual)
		})
		t.Run("fail commit tran after begin another tran", func(t *testing.T) {
			memqdb, err := NewMemQDB("")
			is.NoError(err)
			tran1, err := NewTransaction()
			is.NoError(err)
			err = memqdb.BeginTransaction(ctx, tran1)
			is.NoError(err)
			commands := []QdbStatement{
				{CmdType: CMD_PUT, Key: distribution1.ID, Value: string(dataDistribution1), Extension: MapDistributions},
				{CmdType: CMD_PUT, Key: distribution2.ID, Value: string(dataDistribution2), Extension: MapDistributions},
			}
			err = tran1.Append(commands)
			is.NoError(err)

			tran2, err := NewTransaction()
			is.NoError(err)
			err = memqdb.BeginTransaction(ctx, tran2)
			is.NoError(err)

			err = memqdb.CommitTransaction(ctx, tran1)
			is.EqualError(err, fmt.Sprintf("transaction '%s' can't be committed", tran1.Id()))
		})
		t.Run("fails invalid tran", func(t *testing.T) {
			memqdb, err := NewMemQDB("")
			is.NoError(err)
			tran1, err := NewTransaction()
			is.NoError(err)
			err = memqdb.BeginTransaction(ctx, tran1)
			is.NoError(err)
			statements := []QdbStatement{}
			_ = tran1.Append(statements) //handling this error was skipped intentionally
			err = memqdb.CommitTransaction(ctx, tran1)
			is.EqualError(err, fmt.Sprintf("invalid transaction %s: transaction %s haven't statements", tran1.Id(), tran1.Id()))
		})
	})
}

func TestCreateKeyRangeQdbStatements(t *testing.T) {
	t.Run("no locked key range", func(t *testing.T) {
		is := assert.New(t)
		memQdb, err := NewMemQDB("")
		is.NoError(err)

		keyRange := &KeyRange{
			KeyRangeID:     "krid1",
			LowerBound:     [][]byte{[]byte("1")},
			ShardID:        "sh1",
			DistributionId: "ds1",
			Locked:         false,
		}
		actual, err := memQdb.createKeyRangeQdbStatements(keyRange)
		is.NoError(err)
		expected := []QdbStatement{
			{
				CmdType:   CMD_PUT,
				Key:       "krid1",
				Value:     "{\"LowerBound\":[\"MQ==\"],\"ShardID\":\"sh1\",\"KeyRangeID\":\"krid1\",\"DistributionId\":\"ds1\",\"Locked\":false}",
				Extension: "Krs",
			},
			{
				CmdType:   CMD_PUT,
				Key:       "krid1",
				Value:     "false",
				Extension: "Locks",
			},
			{
				CmdType:   CMD_PUT,
				Key:       "krid1",
				Value:     "false",
				Extension: "Freq",
			},
		}
		is.Equal(expected, actual)
	})
	t.Run("key range, no defined lock", func(t *testing.T) {
		is := assert.New(t)
		memQdb, err := NewMemQDB("")
		is.NoError(err)

		keyRange := &KeyRange{
			KeyRangeID:     "krid1",
			LowerBound:     [][]byte{[]byte("1")},
			ShardID:        "sh1",
			DistributionId: "ds1",
		}
		actual, err := memQdb.createKeyRangeQdbStatements(keyRange)
		is.NoError(err)
		expected := []QdbStatement{
			{
				CmdType:   CMD_PUT,
				Key:       "krid1",
				Value:     "{\"LowerBound\":[\"MQ==\"],\"ShardID\":\"sh1\",\"KeyRangeID\":\"krid1\",\"DistributionId\":\"ds1\",\"Locked\":false}",
				Extension: "Krs",
			},
			{
				CmdType:   CMD_PUT,
				Key:       "krid1",
				Value:     "false",
				Extension: "Locks",
			},
			{
				CmdType:   CMD_PUT,
				Key:       "krid1",
				Value:     "false",
				Extension: "Freq",
			},
		}
		is.Equal(expected, actual)
	})
	t.Run("locked key range", func(t *testing.T) {
		is := assert.New(t)
		memQdb, err := NewMemQDB("")
		is.NoError(err)

		keyRange := &KeyRange{
			KeyRangeID:     "krid1",
			LowerBound:     [][]byte{[]byte("1")},
			ShardID:        "sh1",
			DistributionId: "ds1",
			Locked:         true,
		}
		actual, err := memQdb.createKeyRangeQdbStatements(keyRange)
		is.NoError(err)
		expected := []QdbStatement{
			{
				CmdType:   CMD_PUT,
				Key:       "krid1",
				Value:     "{\"LowerBound\":[\"MQ==\"],\"ShardID\":\"sh1\",\"KeyRangeID\":\"krid1\",\"DistributionId\":\"ds1\",\"Locked\":true}",
				Extension: "Krs",
			},
			{
				CmdType:   CMD_PUT,
				Key:       "krid1",
				Value:     "true",
				Extension: "Locks",
			},
			{
				CmdType:   CMD_PUT,
				Key:       "krid1",
				Value:     "true",
				Extension: "Freq",
			},
		}
		is.Equal(expected, actual)
	})
}

func TestDropKeyRangeQdbStatements(t *testing.T) {
	t.Run("test drop key range happy path", func(t *testing.T) {
		is := assert.New(t)
		memQdb, err := NewMemQDB("")
		is.NoError(err)
		actual, err := memQdb.dropKeyRangeQdbStatements("testKr")
		is.NoError(err)
		expected := []QdbStatement{
			{
				CmdType:   CMD_DELETE,
				Key:       "testKr",
				Value:     "",
				Extension: "Krs",
			},
			{
				CmdType:   CMD_DELETE,
				Key:       "testKr",
				Value:     "",
				Extension: "Locks",
			},
			{
				CmdType:   CMD_DELETE,
				Key:       "testKr",
				Value:     "",
				Extension: "Freq",
			},
		}
		is.Equal(expected, actual)
	})
}
