package twophasetxmeta

import "context"

type TwoPhaseTxMetaMgr interface {
	GetTwoPhaseTxMetaStorage(context.Context) ([]string, error)
}
