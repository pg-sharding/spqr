package main

import (
	"math"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

func TestGetMaxTxnBatchSize(t *testing.T) {
	t.Run("qdb_max_txn_ops is not setted, coord config is not setted", func(t *testing.T) {
		is := assert.New(t)
		actual := getMaxTxnBatchSize(&config.Router{}, nil)
		is.Equal(uint16(qdb.DefaultMaxTxnSize), actual)
	})
	t.Run("qdb_max_txn_ops is setted, coord config is not setted", func(t *testing.T) {
		is := assert.New(t)
		actual := getMaxTxnBatchSize(&config.Router{QdbMaxTxnOps: 56}, nil)
		is.Equal(uint16(56), actual)
	})

	t.Run("qdb_max_txn_ops incorrect val", func(t *testing.T) {
		is := assert.New(t)
		actual := getMaxTxnBatchSize(&config.Router{QdbMaxTxnOps: -56}, nil)
		is.Equal(uint16(qdb.DefaultMaxTxnSize), actual)
	})

	t.Run("qdb_max_txn_ops incorrect val zero", func(t *testing.T) {
		is := assert.New(t)
		actual := getMaxTxnBatchSize(&config.Router{QdbMaxTxnOps: 0}, nil)
		is.Equal(uint16(qdb.DefaultMaxTxnSize), actual)
	})

	t.Run("coord config is stronger", func(t *testing.T) {
		is := assert.New(t)
		actual := getMaxTxnBatchSize(&config.Router{QdbMaxTxnOps: 56}, &config.Coordinator{EtcdMaxTxnOps: 110})
		is.Equal(uint16(110), actual)
	})

	t.Run("coord config is stronger but it must be correct", func(t *testing.T) {
		is := assert.New(t)
		actual := getMaxTxnBatchSize(&config.Router{QdbMaxTxnOps: 56}, &config.Coordinator{EtcdMaxTxnOps: 300000})
		is.Equal(uint16(math.MaxUint16), actual)
	})
	t.Run("coord config is stronger but it must be correct (positive)", func(t *testing.T) {
		is := assert.New(t)
		actual := getMaxTxnBatchSize(&config.Router{QdbMaxTxnOps: 56}, &config.Coordinator{EtcdMaxTxnOps: -5})
		is.Equal(uint16(56), actual)
	})
}
