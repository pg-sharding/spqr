package twophasetxmeta

import "context"

type TwoPhaseTxMetaMgr interface {
	SetTwoPhaseTxMetaStorage(context.Context, []string) error
	GetTwoPhaseTxMetaStorage(context.Context) ([]string, error)
}
