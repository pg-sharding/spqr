package engine

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/catalog"
)

// TODO : unit tests

// TextOidFD generates a pgproto3.FieldDescription object with the provided statement text.
//
// Parameters:
// - stmt (string): The statement text to use in the FieldDescription.
//
// Returns:
// - A pgproto3.FieldDescription object initialized with the provided statement text and default values.
func TextOidFD(stmt string) pgproto3.FieldDescription {
	return pgproto3.FieldDescription{
		Name:                 []byte(stmt),
		TableOID:             0,
		TableAttributeNumber: 0,
		DataTypeOID:          catalog.TEXTOID,
		DataTypeSize:         -1,
		TypeModifier:         -1,
		Format:               0,
	}
}

// FloatOidFD generates a pgproto3.FieldDescription object of FLOAT8 type with the provided statement text.
//
// Parameters:
// - stmt (string): The statement text to use in the FieldDescription.
//
// Returns:
// - A pgproto3.FieldDescription object initialized with the provided statement text and default values.
func FloatOidFD(stmt string) pgproto3.FieldDescription {
	return pgproto3.FieldDescription{
		Name:                 []byte(stmt),
		TableOID:             0,
		TableAttributeNumber: 0,
		DataTypeOID:          catalog.DOUBLEOID,
		DataTypeSize:         8,
		TypeModifier:         -1,
		Format:               0,
	}
}

// IntOidFD generates a pgproto3.FieldDescription object of INT type with the provided statement text.
//
// Parameters:
// - stmt (string): The statement text to use in the FieldDescription.
//
// Returns:
// - A pgproto3.FieldDescription object initialized with the provided statement text and default values.
func IntOidFD(stmt string) pgproto3.FieldDescription {
	return pgproto3.FieldDescription{
		Name:                 []byte(stmt),
		TableOID:             0,
		TableAttributeNumber: 0,
		DataTypeOID:          catalog.INT8OID,
		DataTypeSize:         8,
		TypeModifier:         -1,
		Format:               0,
	}
}
