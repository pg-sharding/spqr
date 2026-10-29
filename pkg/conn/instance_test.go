package conn

import (
	"net"
	"testing"
	"time"
)

func TestNewInstanceConn(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	instance, err := NewInstanceConn(listener.Addr().String(), "", "test", nil, time.Second, time.Second, 9500*time.Millisecond)
	if err != nil {
		t.Fatalf("connect to backend: %v", err)
	}
	defer instance.Close()
}
