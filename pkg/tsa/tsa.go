package tsa

import (
	"github.com/pg-sharding/spqr/pkg/shard"
)

// TSA is stands for target_session_attrs,
type TSA string

type CheckResult struct {
	Alive  bool
	RW     bool
	Reason string
}

type CachedCheckResult struct {
	Hostname string
	Result   CheckResult
}

type TSAChecker interface {
	CheckTSA(sh shard.Shard) (CheckResult, error)
}

type CachedResultsGetter interface {
	GetCachedResults() map[string]CachedCheckResult
}
