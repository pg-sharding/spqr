package twophasetx

type TwoPhaseTxMetaMgr interface {
	GetTwoPhaseTxMetaStorage() (string, error)
}
