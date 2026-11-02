package engine

// BoolAnd returns the logical AND of two PostgreSQL boolean datums.
// Values use text t/f or binary 1/0; nil represents NULL.
func BoolAnd(left, right []byte) []byte {
	switch {
	case len(left) == 1 && (left[0] == 'f' || left[0] == 0):
		return left
	case len(right) == 1 && (right[0] == 'f' || right[0] == 0):
		return right
	case left == nil || right == nil:
		return nil
	default:
		return left
	}
}
