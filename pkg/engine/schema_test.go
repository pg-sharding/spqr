package engine

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/stretchr/testify/assert"
)

func TestOidFD(t *testing.T) {
	tests := []struct {
		name    string
		stmt    string
		buildFD func(string) pgproto3.FieldDescription
		expOID  uint32
		expSize int16
	}{
		{
			name:    "TextOidFD",
			stmt:    "SELECT 1",
			buildFD: TextOidFD,
			expOID:  catalog.TEXTOID,
			expSize: -1,
		},
		{
			name:    "TextOidFD empty input",
			stmt:    "",
			buildFD: TextOidFD,
			expOID:  catalog.TEXTOID,
			expSize: -1,
		},
		{
			name:    "FloatOidFD",
			stmt:    "SELECT 1.23::float8",
			buildFD: FloatOidFD,
			expOID:  catalog.DOUBLEOID,
			expSize: 8,
		},
		{
			name:    "IntOidFD",
			stmt:    "SELECT 42::int8",
			buildFD: IntOidFD,
			expOID:  catalog.INT8OID,
			expSize: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := assert.New(t)

			fd := tt.buildFD(tt.stmt)

			is.Equal([]byte(tt.stmt), fd.Name)
			is.EqualValues(0, fd.TableOID)
			is.EqualValues(0, fd.TableAttributeNumber)
			is.Equal(tt.expOID, fd.DataTypeOID)
			is.Equal(tt.expSize, fd.DataTypeSize)
			is.EqualValues(-1, fd.TypeModifier)
			is.EqualValues(0, fd.Format)
		})
	}
}
