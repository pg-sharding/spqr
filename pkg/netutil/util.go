package netutil

import (
	"net"
	"syscall"
)

func TCPisConnected(conn net.Conn) bool {
	/* some caller may pass nil here */
	if conn == nil {
		return false
	}
	f, err := conn.(*net.TCPConn).File()
	if err != nil {
		return false
	}

	b := []byte{0}
	/* Note we are doing this syscall without any additional synchronization
	* This should not however induce any harm. MSG_PEEK makes sure subsequent read will
	* still receive some bytes, and we use MSG_DONTWAIT to not make this check time-consuming
	* because we are not interested in result anyways  */
	_, _, err = syscall.Recvfrom(int(f.Fd()), b, syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
	return err != nil
}
