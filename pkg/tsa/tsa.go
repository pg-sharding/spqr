package tsa

import (
	"time"
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
