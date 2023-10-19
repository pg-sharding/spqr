package app

import (
	"net"
	"os"
)

type Notifier struct {
	sock  net.Conn
	debug bool
}

func NewNotifier(debug bool) (*Notifier, error) {
	addr := os.Getenv("NOTIFY_SOCKET")

	sock, err := net.Dial("unix", addr)
	if err != nil {
		sock = nil
		if debug {
			return nil, err
		}
	}

	return &Notifier{sock: sock, debug: debug}, nil
}

func (n *Notifier) Ready() error {
	return n.send([]byte("READY=1\n"))
}

func (n *Notifier) Notify() error {
	return n.send([]byte("WATCHDOG=1\n"))
}

func (n *Notifier) send(msg []byte) error {
	_, err := n.sock.Write(msg)

	if n.debug {
		return err
	}
	return nil
}
