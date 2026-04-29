//go:build !linux

package netutil

import "net"

// TCPCheckAliveness is a stub for non-Linux platforms.
// The TCP_INFO socket option is Linux-specific, so on other platforms
// this function always returns true (assumes connection is alive).
//
// For production deployments, SPQR should run on Linux where the full
// TCP state checking is available.
func TCPCheckAliveness(_ net.Conn) bool {
	// On non-Linux platforms, assume the connection is alive.
	// The actual connection health will be detected on first use.
	return true
}
