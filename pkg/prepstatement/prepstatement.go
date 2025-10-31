package prepstatement

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/router/xproto"
)

type PreparedStatementDefinition struct {
	Name          string
	Query         string
	ParameterOIDs []uint32
	// if we did prepared statement override for any reason,
	// store here some state to make client see what it actually wants

	// XXX: Currently always one last column
	OverwriteRemoveParamIds map[int]struct{}
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

func GetParams(paramsFormatCodes []int16, bindParams [][]byte) []int16 {
	var queryParamsFormatCodes []int16
	paramsLen := len(bindParams)

	/* https://github.com/postgres/postgres/blob/c65bc2e1d14a2d4daed7c1921ac518f2c5ac3d17/src/backend/tcop/pquery.c#L664-L691 */ /* #no-spell-check-line */
	if len(paramsFormatCodes) > 1 {
		queryParamsFormatCodes = paramsFormatCodes
	} else if len(paramsFormatCodes) == 1 {

		/* single format specified, use for all columns */
		queryParamsFormatCodes = make([]int16, paramsLen)

		for i := range paramsLen {
			queryParamsFormatCodes[i] = paramsFormatCodes[0]
		}
	} else {
		/* use default format for all columns */
		queryParamsFormatCodes = make([]int16, paramsLen)
		for i := range paramsLen {
			queryParamsFormatCodes[i] = xproto.FormatCodeText
		}
	}
	return queryParamsFormatCodes
}
