package engine

// BoolOr returns the logical OR of two PostgreSQL boolean datums.
// Values use text t/f or binary 1/0; nil represents NULL.
func BoolOr(left, right []byte) []byte {
	switch {
	case len(left) == 1 && (left[0] == 't' || left[0] == 1):
		return left
	case len(right) == 1 && (right[0] == 't' || right[0] == 1):
		return right
	case left == nil || right == nil:
		return nil
	default:
		return left
	}
}
