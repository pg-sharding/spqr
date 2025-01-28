package txstatus

type TXStatus byte

const (
	TXIDLE = TXStatus(73)
	TXERR  = TXStatus(69)
	TXACT  = TXStatus(84)
)

type TxStatusMgr interface {
	SetTxStatus(status TXStatus)
	TxStatus() TXStatus
}

// String returns the string representation of the TXStatus value.
// It maps the TXStatus value to its corresponding string representation.
func (s TXStatus) String() string {
	switch s {
	case TXIDLE:
		return "IDLE"
	case TXERR:
		return "ERROR"
	case TXACT:
		return "ACTIVE"
	}
	return "invalid"
}
