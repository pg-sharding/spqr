package app

import (
	"fmt"
	"net"
	"time"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"golang.org/x/sys/unix"
)

const Timeout = 4 * time.Second

type Notifier struct {
	sock  net.Conn
	debug bool
}

func NewNotifier(ns string, debug bool) (*Notifier, error) {
	sock, err := net.Dial("unixgram", ns)
	if err != nil {
		sock = nil
		if debug {
			return nil, err
		}
	}

	return &Notifier{sock: sock, debug: debug}, nil
}

func monotonicNsec() uint64 {
	var ts unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts)
	return uint64(ts.Sec)*1_000_000_000 + uint64(ts.Nsec)
}

func (n *Notifier) Reloading() error {
	return n.send([]byte(fmt.Sprintf("RELOADING=1\nMONOTONIC_USEC=%d\n", monotonicNsec())))
}

func (n *Notifier) Ready() error {
	return n.send([]byte("READY=1\n"))
}

func (n *Notifier) Notify() error {
	return n.send([]byte("WATCHDOG=1\n"))
}

func (n *Notifier) send(msg []byte) error {
	spqrlog.Zero.Trace().Msg(fmt.Sprintf("sending systemd notification: %s", msg))

	if n.sock == nil {
		return nil
	}

	_, err := n.sock.Write(msg)

	if n.debug {
		return err
	}
	return nil
}
