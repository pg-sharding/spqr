//go:build linux

package netutil

import (
	"net"
	"syscall"
	"unsafe"
)

// tcpInfo is a subset of the linux tcp_info struct (from linux/tcp.h)
// We only need the State field for aliveness checking
type tcpInfo struct {
	State uint8
	// Remaining fields omitted - we only need State
}

// TCP states from linux/tcp.h
const (
	tcpEstablished = 1
	tcpSynSent     = 2
	tcpSynRecv     = 3
	tcpFinWait1    = 4
	tcpFinWait2    = 5
	tcpTimeWait    = 6
	tcpClose       = 7
	tcpCloseWait   = 8
	tcpLastAck     = 9
	tcpListen      = 10
	tcpClosing     = 11
)

// TCP_CheckAliveness checks if a TCP connection is still alive by querying
// the kernel's TCP state via getsockopt(TCP_INFO).
//
// This is more reliable than peek-based checks because it directly queries
// the TCP state machine and can detect half-closed (CLOSE_WAIT) connections.
//
// Returns true if the connection is in ESTABLISHED state, false otherwise.
func TCP_CheckAliveness(conn net.Conn) bool {
	if conn == nil {
		return false
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return false
	}

	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return false
	}

	var alive bool
	controlErr := rawConn.Control(func(fd uintptr) {
		var info tcpInfo
		infoLen := uint32(unsafe.Sizeof(info))

		_, _, errno := syscall.Syscall6(
			syscall.SYS_GETSOCKOPT,
			fd,
			syscall.SOL_TCP,
			syscall.TCP_INFO,
			uintptr(unsafe.Pointer(&info)),
			uintptr(unsafe.Pointer(&infoLen)),
			0,
		)

		if errno != 0 {
			alive = false
			return
		}

		// Only ESTABLISHED state is considered alive for pooled connections
		alive = info.State == tcpEstablished
	})

	if controlErr != nil {
		return false
	}

	return alive
}
