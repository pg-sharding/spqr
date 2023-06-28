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
