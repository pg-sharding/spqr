package xproto

import (
	"slices"

	"github.com/jackc/pgx/v5/pgproto3"
)

// CopyFieldDescriptions returns a deep copy of a []pgproto3.FieldDescription.
// It clones the Name byte slice for each field to prevent data contamination
// from shared buffers.
func CopyFieldDescriptions(src []pgproto3.FieldDescription) []pgproto3.FieldDescription {
	if src == nil {
		return nil
	}
	dst := make([]pgproto3.FieldDescription, len(src))
	copy(dst, src)
	for i := range dst {
		if dst[i].Name != nil {
			dst[i].Name = slices.Clone(dst[i].Name)
		}
	}
	return dst
}

// CopyByteSlices returns a deep copy of a [][]byte.
// Each non-nil element is copied into freshly allocated memory,
// ensuring no references to the original backing arrays remain.
//
// This is necessary because pgproto3 decodes message fields like
// Bind.Parameters and DataRow.Values as sub-slices of a shared
// read buffer (chunkReader) backed by a global sync.Pool.
// Retaining those sub-slices past the next Receive() call risks
// cross-query data contamination.
func CopyByteSlices(src [][]byte) [][]byte {
	if src == nil {
		return nil
	}
	dst := make([][]byte, len(src))
	for i, s := range src {
		if s != nil {
			dst[i] = slices.Clone(s)
		}
	}
	return dst
}
