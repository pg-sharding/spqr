package prepstatement

import (
	"github.com/jackc/pgx/v5/pgproto3"
)

type PreparedStatementDefinition struct {
	Name          string
	Query         string
	ParameterOIDs []uint32
}

type PreparedStatementDescriptor struct {
	NoData    bool
	ParamDesc *pgproto3.ParameterDescription
	RowDesc   *pgproto3.RowDescription
}

type PreparedStatementHolder interface {
	HasPrepareStatement(hash uint64, shardId uint) (bool, *PreparedStatementDescriptor)
	StorePrepareStatement(hash uint64, shardId uint, d *PreparedStatementDefinition, rd *PreparedStatementDescriptor) error
}

type PreparedStatementMapper interface {
	PreparedStatementQueryByName(name string) string
	PreparedStatementDefinitionByName(name string) *PreparedStatementDefinition
	PreparedStatementQueryHashByName(name string) uint64
	StorePreparedStatement(d *PreparedStatementDefinition)
}
