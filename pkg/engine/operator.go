package engine

/*
*
 */
type Operator interface {
	Less(l []byte, r []byte) bool
}

type TEXTOperator struct {
}

func (t *TEXTOperator) Less(l []byte, r []byte) bool {
	return string(l) < string(r)
}

var _ Operator = &TEXTOperator{}
