package conn

import (
	"os"
	"syscall"
	"time"
)

func setTCPUserTimeout(d time.Duration) func(string, string, syscall.RawConn) error {
	return func(_, _ string, c syscall.RawConn) error {
		var sysErr error
		var err = c.Control(func(fd uintptr) {
			/*
				#define TCP_USER_TIMEOUT	 18 // How long for loss retry before timeout
			*/

			sysErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, 0x12, int(d.Milliseconds()))
		})
		if sysErr != nil {
			return os.NewSyscallError("setsockopt", sysErr)
		}
		return err
	}
}
