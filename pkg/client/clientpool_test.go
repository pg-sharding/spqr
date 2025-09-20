package client_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pg-sharding/spqr/pkg/client"
)

func newCtrl(t *testing.T) *gomock.Controller {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	return ctrl
}

func TestPutAndPop_Success(t *testing.T) {
	ctrl := newCtrl(t)

	mockCl := NewMockClient(ctrl)
	mockCl.EXPECT().ID().Return(uint(42)).Times(1)
	mockCl.EXPECT().Close().Return(nil).Times(1)

	p := client.NewClientPool()
	if err := p.Put(mockCl); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	ok, err := p.Pop(42)
	if err != nil {
		t.Fatalf("Pop() error = %v", err)
	}
	if !ok {
		t.Fatalf("Pop() ok = false, want true")
	}
}

func TestPop_NotFound(t *testing.T) {
	p := client.NewClientPool()

	ok, err := p.Pop(777)
	if err != nil {
		t.Fatalf("Pop() error = %v, want nil", err)
	}
	if ok {
		t.Fatalf("Pop() ok = true, want false")
	}
}

func TestPop_CloseErrorBubbled(t *testing.T) {
	ctrl := newCtrl(t)

	mockCl := NewMockClient(ctrl)
	mockCl.EXPECT().ID().Return(uint(1)).Times(1)
	mockCl.EXPECT().Close().Return(errors.New("boom")).Times(1)

	p := client.NewClientPool()
	if err := p.Put(mockCl); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	ok, err := p.Pop(1)
	if !ok {
		t.Fatalf("Pop() ok = false, want true")
	}
	if err == nil || err.Error() != "boom" {
		t.Fatalf("Pop() err = %v, want boom", err)
	}
}

func TestShutdown_InvokesShutdownForAllClients(t *testing.T) {
	ctrl := newCtrl(t)

	const n = 5
	p := client.NewClientPool()

	var counter atomic.Int32

	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		id := uint(i + 1)
		m := NewMockClient(ctrl)
		m.EXPECT().ID().Return(id).Times(1)
		m.EXPECT().Shutdown().DoAndReturn(func() error {
			defer wg.Done()
			counter.Add(1)
			return nil
		}).Times(1)

		if err := p.Put(m); err != nil {
			t.Fatalf("Put() err: %v", err)
		}
	}

	if err := p.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	wg.Wait()

	if counter.Load() != n {
		t.Fatalf("expected %d Shutdown() calls, got %d", n, counter.Load())
	}
}

func TestClientPoolForeach_AllVisitedAndRAddrIsLocal(t *testing.T) {
	ctrl := newCtrl(t)

	a := NewMockClient(ctrl)
	b := NewMockClient(ctrl)
	a.EXPECT().ID().Return(uint(1)).AnyTimes()
	b.EXPECT().ID().Return(uint(2)).AnyTimes()

	p := client.NewClientPool()
	if err := p.Put(a); err != nil {
		t.Fatalf("Put(a) error = %v", err)
	}
	if err := p.Put(b); err != nil {
		t.Fatalf("Put(b) error = %v", err)
	}

	var (
		mu   sync.Mutex
		used = map[uint]bool{}
	)
	err := p.ClientPoolForeach(func(ci client.ClientInfo) error {
		if ci.RAddr() != "local" {
			t.Fatalf("ClientInfo.RAddr() = %q, want %q", ci.RAddr(), "local")
		}
		mu.Lock()
		used[ci.ID()] = true
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("ClientPoolForeach() error = %v, want nil", err)
	}

	if len(used) != 2 || !used[1] || !used[2] {
		t.Fatalf("visited IDs = %#v, want {1,2}", used)
	}
}

func TestClientPoolForeach_StopsOnErrorButReturnsNil(t *testing.T) {
	ctrl := newCtrl(t)

	a := NewMockClient(ctrl)
	b := NewMockClient(ctrl)
	a.EXPECT().ID().Return(uint(100)).AnyTimes()
	b.EXPECT().ID().Return(uint(200)).AnyTimes()

	p := client.NewClientPool()
	_ = p.Put(a)
	_ = p.Put(b)

	var calls atomic.Int32
	
	err := p.ClientPoolForeach(func(ci client.ClientInfo) error {
		if calls.Add(1) == 1 {
			return errors.New("stop")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ClientPoolForeach() returned err = %v, want nil", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("callback calls = %d, want 1 (iteration should stop on error)", calls.Load())
	}
}
