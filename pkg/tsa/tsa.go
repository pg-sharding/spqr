package tsa

import (
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/txstatus"
)

type TSAChecker interface {
	CheckTSA(sh shard.Shard) (bool, string, error)
}

type CacheEntry struct {
	result    bool
	comment   string
	lastCheck int64
}

type CachedTSAChecker struct {
	mu            sync.Mutex
	recheckPeriod time.Duration
	cache         map[string]CacheEntry
}

// NewTSAChecker creates a new instance of TSAChecker.
// It returns a TSAChecker interface that can be used to perform TSA checks.
//
// Returns:
//   - TSAChecker: A new instance of TSAChecker.
func NewTSAChecker() TSAChecker {
	return &CachedTSAChecker{
		mu:            sync.Mutex{},
		recheckPeriod: time.Second,
		cache:         map[string]CacheEntry{},
	}
}

func NewTSACheckerWithDuration(tsaRecheckDuration time.Duration) TSAChecker {
	return &CachedTSAChecker{
		mu:            sync.Mutex{},
		recheckPeriod: tsaRecheckDuration,
		cache:         map[string]CacheEntry{},
	}
}

// CheckTSA checks the TSA for a given shard and returns the result, comment, and error.
// If the TSA check result is already cached and not expired, it returns the cached result.
// Otherwise, it performs the TSA check and updates the cache with the new result.
// The function returns a boolean indicating whether the shard is in a read-write state,
// a string describing the reason for the state, and an error if any occurred during the process.
//
// Parameters:
//   - sh: The shard to check the TSA for.
//
// Returns:
//   - bool: A boolean indicating whether the shard is in a read-write state.
//   - string: A string describing the reason for the state.
//   - error: An error if any occurred during the process.
func (ctsa *CachedTSAChecker) CheckTSA(sh shard.Shard) (bool, string, error) {
	ctsa.mu.Lock()
	defer ctsa.mu.Unlock()

	n := time.Now().UnixNano()
	if e, ok := ctsa.cache[sh.Instance().Hostname()]; ok && n-e.lastCheck < ctsa.recheckPeriod.Nanoseconds() {
		return e.result, e.comment, nil
	}

	res, comment, err := CheckTSA(sh)
	if err != nil {
		return res, comment, err
	}
	ctsa.cache[sh.Instance().Hostname()] = CacheEntry{
		lastCheck: n,
		comment:   comment,
		result:    res,
	}
	return res, comment, nil
}

/* target session attr utility */

// CheckTSA checks if the given shard is in a read-only state.
// It sends a query to the shard to retrieve the value of the "transaction_read_only" setting.
// If the query is successful and the value is "off", it means the shard is in a read-write state.
// If the value is not "off", it means the shard is in a read-only state.
// The function returns a boolean indicating whether the shard is in a read-write state,
// a string describing the reason for the state, and an error if any occurred during the process.
//
// Parameters:
//   - sh: The shard to check the TSA for.
//
// Returns:
//   - bool: A boolean indicating whether the shard is in a read-write state.
//   - string: A string describing the reason for the state.
//   - error: An error if any occurred during the process.
func CheckTSA(sh shard.Shard) (bool, string, error) {
	if err := sh.Send(&pgproto3.Query{
		String: "SHOW transaction_read_only",
	}); err != nil {
		spqrlog.Zero.Debug().
			Uint("shard", sh.ID()).
			Err(err).
			Msg("encounter error while sending read-write check")
		return false, "", err
	}

	res := false
	reason := "zero datarow recieved"

	for {
		msg, err := sh.Receive()
		if err != nil {
			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Err(err).
				Msg("shard recieved error during check rw")
			return false, reason, err
		}

		spqrlog.Zero.Debug().
			Uint("shard", sh.ID()).
			Interface("message", msg).
			Msg("shard recieved msg during check rw")
		switch qt := msg.(type) {
		case *pgproto3.DataRow:
			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Interface("datarow", qt).
				Msg("shard checking read-write")
			if len(qt.Values) == 1 && len(qt.Values[0]) == 3 && qt.Values[0][0] == 'o' && qt.Values[0][1] == 'f' && qt.Values[0][2] == 'f' {
				res = true
				reason = "is primary"
			} else {
				reason = fmt.Sprintf("transaction_read_only is %+v", qt.Values)
			}

		case *pgproto3.ReadyForQuery:
			if txstatus.TXStatus(qt.TxStatus) != txstatus.TXIDLE {
				spqrlog.Zero.Debug().
					Uint("shard", sh.ID()).
					Msg("shard got unsync connection while calculating RW")
				return false, reason, fmt.Errorf("connection unsync while acquiring it")
			}

			spqrlog.Zero.Debug().
				Uint("shard", sh.ID()).
				Bool("result", res).
				Msg("shard calculated RW result")
			return res, reason, nil
		}
	}
}
