//go:build !linux

package conn

import (
	"syscall"
	"time"
)

// TCP_USER_TIMEOUT is Linux-specific; other systems use their TCP defaults.
func setTCPUserTimeout(_ time.Duration) func(string, string, syscall.RawConn) error {
	return nil
}
