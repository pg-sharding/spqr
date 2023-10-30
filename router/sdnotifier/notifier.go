package app

import (
	"fmt"
	"net"
	"time"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

/*
#include <time.h>
static unsigned long long get_nsecs(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (unsigned long long)ts.tv_sec * 1000000000UL + ts.tv_nsec;
}
*/
import "C"

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

func (n *Notifier) Reloading() error {
	return n.send([]byte(fmt.Sprintf("RELOADING=1\nMONOTONIC_USEC=%d\n", uint64(C.get_nsecs()))))
}

func (n *Notifier) Ready() error {
	return n.send([]byte("READY=1\n"))
}

func (n *Notifier) Notify() error {
	return n.send([]byte("WATCHDOG=1\n"))
}

func (n *Notifier) send(msg []byte) error {
	spqrlog.Zero.Debug().Msg(fmt.Sprintf("sending systemd notification: %s", msg))

	if n.sock == nil {
		return nil
	}

	_, err := n.sock.Write(msg)

	if n.debug {
		return err
	}
	return nil
}
