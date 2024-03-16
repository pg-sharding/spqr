package txstatus

type TXStatus byte

const (
	TXIDLE = TXStatus(73)
	TXERR  = TXStatus(69)
	TXACT  = TXStatus(84)
	TXCONT = TXStatus(1)
)

type TxStatusMgr interface {
	SetTxStatus(status TXStatus)
	TxStatus() TXStatus
}

func (s TXStatus) String() string {
	switch s {
	case TXIDLE:
		return "IDLE"
	case TXERR:
		return "ERROR"
	case TXACT:
		return "ACTIVE"
	case TXCONT:
		return "INTERNAL STATE"
	}
	return "invalid"
}
