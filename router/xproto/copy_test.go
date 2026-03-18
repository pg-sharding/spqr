package xproto

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

func TestCopyFieldDescriptions_NilInput(t *testing.T) {
	if got := CopyFieldDescriptions(nil); got != nil {
		t.Errorf("CopyFieldDescriptions(nil) = %v, want nil", got)
	}
}

func TestCopyFieldDescriptions_DeepCopy(t *testing.T) {
	src := []pgproto3.FieldDescription{
		{Name: []byte("original"), TableOID: 1, DataTypeOID: 25},
		{Name: []byte("second"), TableOID: 2, DataTypeOID: 23},
	}
	exp := []pgproto3.FieldDescription{
		{Name: []byte("original"), TableOID: 1, DataTypeOID: 25},
		{Name: []byte("second"), TableOID: 2, DataTypeOID: 23},
	}
	dst := CopyFieldDescriptions(src)

	assert.Equal(t, exp, dst)
}

func TestCopyFieldDescriptions_NilName(t *testing.T) {
	src := []pgproto3.FieldDescription{
		{Name: nil, DataTypeOID: 25},
	}
	exp := []pgproto3.FieldDescription{
		{Name: nil, DataTypeOID: 25},
	}
	dst := CopyFieldDescriptions(src)

	assert.Equal(t, exp, dst)
}

func TestCopyByteSlices_NilInput(t *testing.T) {
	if got := CopyByteSlices(nil); got != nil {
		t.Errorf("CopyByteSlices(nil) = %v, want nil", got)
	}
}

func TestCopyByteSlices_DeepCopy(t *testing.T) {
	src := [][]byte{[]byte("hello"), []byte("world"), nil}
	exp := [][]byte{[]byte("hello"), []byte("world"), nil}

	dst := CopyByteSlices(src)

	assert.Equal(t, exp, dst)
}
