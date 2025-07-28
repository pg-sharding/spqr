package planner

import (
	"context"
	"fmt"
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/sequences"
	"github.com/pg-sharding/spqr/qdb"
)

const DEFAULT_ID_RANGE_SIZE uint64 = 1

type IdentityRouterCache interface {
	NextVal(ctx context.Context, sequenceName string) (int64, error)
}

type CachedIdRange struct {
	idRange *qdb.SequenceIdRange
	mu      sync.Mutex
}

func NewCachedIdRange() CachedIdRange {
	return CachedIdRange{mu: sync.Mutex{}}
}
func (cir *CachedIdRange) nextVal() (int64, bool) {
	if cir.idRange == nil {
		return 0, false
	} else {
		if cir.idRange.Left < cir.idRange.Right {
			res := cir.idRange.Left
			cir.idRange.Left++
			return res, true
		} else if cir.idRange.Left == cir.idRange.Right {
			res := cir.idRange.Left
			cir.idRange = nil //the range is over
			return res, true
		}

		return 0, false
	}
}

type IdentityRouterCacheImpl struct {
	cachedIdentities map[string]*CachedIdRange
	defaultRangeSize uint64
	mu               sync.Mutex
	mngr             *sequences.SequenceMgr
}

func NewIdentityRouterCache(defaultRangeSize uint64, mgr *sequences.SequenceMgr) *IdentityRouterCacheImpl {
	var rngSize = DEFAULT_ID_RANGE_SIZE
	if defaultRangeSize > 0 {
		rngSize = defaultRangeSize
	}

	return &IdentityRouterCacheImpl{defaultRangeSize: rngSize,
		cachedIdentities: make(map[string]*CachedIdRange, 0),
		mu:               sync.Mutex{},
		mngr:             mgr,
	}
}

func (irc *IdentityRouterCacheImpl) getRangeForSeq(sequenceName string) *CachedIdRange {
	irc.mu.Lock()
	defer irc.mu.Unlock()
	if _, ok := irc.cachedIdentities[sequenceName]; !ok {
		newRng := NewCachedIdRange()
		irc.cachedIdentities[sequenceName] = &newRng
	}
	return irc.cachedIdentities[sequenceName]
}

func (irc *IdentityRouterCacheImpl) NextVal(ctx context.Context, sequenceName string) (int64, error) {
	rng := irc.getRangeForSeq(sequenceName)
	rng.mu.Lock()
	defer rng.mu.Unlock()
	if nextVal, ok := rng.nextVal(); ok {
		return nextVal, nil
	} else {
		mngr := *irc.mngr
		if newRange, err := mngr.NextRange(ctx, sequenceName, irc.defaultRangeSize); err != nil {
			return 0, err
		} else {
			rng.idRange = newRange
			if nextVal, ok := rng.nextVal(); ok {
				return nextVal, nil
			}
			return 0, fmt.Errorf("can`t get next value from fresh id range! sequence='%s'", sequenceName)
		}
	}
}
