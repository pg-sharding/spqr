package main

import (
	"math"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

func TestGetMaxTxnBatchSize(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		is := assert.New(t)
		actual := getMaxTxnBatchSize(&config.Coordinator{EtcdMaxTxnOps: 110})
		is.Equal(uint16(110), actual)
	})

	t.Run("etcd_max_txn_ops has not been set", func(t *testing.T) {
		is := assert.New(t)
		actual := getMaxTxnBatchSize(&config.Coordinator{})
		is.Equal(uint16(qdb.DefaultMaxTxnSize), actual)
	})

	t.Run("too much", func(t *testing.T) {
		is := assert.New(t)
		actual := getMaxTxnBatchSize(&config.Coordinator{EtcdMaxTxnOps: 300000})
		is.Equal(uint16(math.MaxUint16), actual)
	})
	t.Run("incorrect val ", func(t *testing.T) {
		is := assert.New(t)
		actual := getMaxTxnBatchSize(&config.Coordinator{EtcdMaxTxnOps: 0})
		is.Equal(uint16(qdb.DefaultMaxTxnSize), actual)
	})
}
