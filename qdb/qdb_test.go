package qdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestPackToEtcdCommands(t *testing.T) {
	t.Run("test happy path pack commands", func(t *testing.T) {
		is := assert.New(t)
		statements := []QdbStatement{
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
		statements := []QdbStatement{
			{CmdType: 7, Key: "test1", Value: "val1"},
			{CmdType: CmdDelete, Key: "test3"},
		}
		_, err := packEtcdCommands(statements)
		is.EqualError(err, "not found operation type: 7")

	})
}

func TestExactMatchedKVs(t *testing.T) {
	t.Run("filters out prefixed keys", func(t *testing.T) {
		is := assert.New(t)
		resp := &clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{Key: []byte("/reference_relations/zz"), Value: []byte("zz")},
				{Key: []byte("/reference_relations/zzx"), Value: []byte("zzx")},
			},
		}

		matched := exactMatchedKVs(resp, "/reference_relations/zz")
		is.Len(matched, 1)
		is.Equal([]byte("/reference_relations/zz"), matched[0].Key)
	})

	t.Run("returns empty for missing exact key", func(t *testing.T) {
		is := assert.New(t)
		resp := &clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{Key: []byte("/reference_relations/zzx"), Value: []byte("zzx")},
			},
		}

		matched := exactMatchedKVs(resp, "/reference_relations/zz")
		is.Empty(matched)
	})
}
