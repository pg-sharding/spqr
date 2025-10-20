package prepstatement

import (
	"github.com/jackc/pgx/v5/pgproto3"
)

type PreparedStatementDefinition struct {
	Name          string
	Query         string
	ParameterOIDs []uint32
	// if we did prepared statement override for any reason,
	// store here some state to make client see what it actaully wants

	// XXX: Currently always one last column
	OverwriteRemoveParamIds map[uint32]struct{}
	SeqName                 string
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
