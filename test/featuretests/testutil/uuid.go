package testutil

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
)

func NewUUID(t *testing.T) uuid.UUID {
	v, err := uuid.NewV4()
	require.NoError(t, err)
	return v
}

func NewUUIDStr(t *testing.T) string {
	return NewUUID(t).String()
}
