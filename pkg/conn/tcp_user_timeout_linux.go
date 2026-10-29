package conn

import (
	"os"
	"syscall"
	"time"
)

const tcpUserTimeout = 0x12 // TCP_USER_TIMEOUT on Linux.

func setTCPUserTimeout(d time.Duration) func(string, string, syscall.RawConn) error {
	return func(_, _ string, c syscall.RawConn) error {
		var sysErr error
		err := c.Control(func(fd uintptr) {
			sysErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpUserTimeout, int(d.Milliseconds()))
		})
		if sysErr != nil {
			return os.NewSyscallError("setsockopt", sysErr)
		}
		return err
	}
}
