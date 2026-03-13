package xproto

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
			dst[i] = append([]byte(nil), s...)
		}
	}
	return dst
}
