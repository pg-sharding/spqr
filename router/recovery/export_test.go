package recovery

import "github.com/pg-sharding/spqr/qdb"

func NewTwoPCWatchDogForTest(d qdb.XDCStateKeeper) *TwoPCWatchDog {
	return &TwoPCWatchDog{d: d}
}
