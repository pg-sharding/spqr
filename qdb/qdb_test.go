package qdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestPackToEtcdCommands(t *testing.T) {
	t.Run("test happy path pack commands", func(t *testing.T) {
		is := assert.New(t)
		statements := []XRecord{
			{CmdType: CmdPut, Key: "test1", Value: "val1"},
			{CmdType: CmdDelete, Key: "test3"},
		}
		expectedOps := []clientv3.Op{
			clientv3.OpPut("test1", "val1"),
			clientv3.OpDelete("test3"),
		}
		actualOps, err := packEtcdCommands(statements)
		is.NoError(err)
		is.Equal(expectedOps, actualOps)
	})
	t.Run("test unknown type", func(t *testing.T) {
		is := assert.New(t)
		statements := []XRecord{
			{CmdType: 7, Key: "test1", Value: "val1"},
			{CmdType: CmdDelete, Key: "test3"},
		}
		_, err := packEtcdCommands(statements)
		is.EqualError(err, "not found operation type: 7")

	})
}
