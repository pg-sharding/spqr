package tsa

import (
	"github.com/pg-sharding/spqr/pkg/shard"
)

// TSA is stands for target_session_attrs,
type TSA string

type CheckResult struct {
	RW     bool
	Reason string
}

type TSAChecker interface {
	CheckTSA(sh shard.Shard) (CheckResult, error)
}
