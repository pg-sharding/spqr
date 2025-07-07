package tsa

import (
	"time"

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
	CR            CheckResult
	LastCheckTime time.Time
}

type TSAChecker interface {
	CheckTSA(sh shard.ShardHostInstance) (CheckResult, error)
}

type CachedTSAChecker interface {
	CheckTSA(sh shard.ShardHostInstance) (CachedCheckResult, error)
}
