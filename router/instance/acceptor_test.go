package instance

import (
	"errors"
	"net"
	"testing"
	"time"

	mockconn "github.com/pg-sharding/spqr/pkg/mock/conn"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

/* XXX: mock for this? */
type fakeListener struct {
	acceptFn func() (net.Conn, error)
}

func (f *fakeListener) Accept() (net.Conn, error) { return f.acceptFn() }
func (f *fakeListener) Close() error              { return nil }
func (f *fakeListener) Addr() net.Addr            { return &net.TCPAddr{} }

const (
	testTimeout       = 2 * time.Second
	testMaxRetries    = 5
	testRetrySleep    = time.Millisecond
	testRetrySleepMax = 2 * time.Millisecond
)

/*
* XXX: net listener is not easily cancellable via
* context, so we have to workaround with aux channel here
 */

func TestAcceptLoopContinuesAfterError(t *testing.T) {
	conn := mockconn.NewMockRawConn(gomock.NewController(t))

	conn.EXPECT().RemoteAddr().AnyTimes().Return(&net.TCPAddr{})

	calls := 0
	block := make(chan struct{})
	ln := &fakeListener{
		acceptFn: func() (net.Conn, error) {
			calls++
			switch calls {
			case 1:
				return nil, errors.New("transient error")
			case 2:
				return conn, nil
			default:
				<-block
				return nil, errors.New("blocked")
			}
		},
	}

	cChan := make(chan net.Conn, 2)
	go acceptLoop(ln, cChan, "test", testMaxRetries, testRetrySleep, testRetrySleepMax)

	select {
	case c, ok := <-cChan:
		assert.True(t, ok, "channel must not be closed after a single error")

		assert.NotNil(t, c, "expected a non-nil connection")

	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for connection")
	}

	close(block)
}

func TestAcceptLoopClosesOnMaxErrors(t *testing.T) {
	errCount := 0
	ln := &fakeListener{
		acceptFn: func() (net.Conn, error) {
			errCount++
			return nil, errors.New("persistent error")
		},
	}

	cChan := make(chan net.Conn, 1)
	go acceptLoop(ln, cChan, "test", testMaxRetries, testRetrySleep, testRetrySleepMax)

	select {
	case _, ok := <-cChan:

		assert.True(t, ok, "channel should be closed")

	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for channel to be closed")
	}

	assert.Equal(t, testMaxRetries, errCount, "expected %d errors before close, got %d", testMaxRetries, errCount)
}

func TestAcceptLoopResetsOnSuccess(t *testing.T) {
	conn := mockconn.NewMockRawConn(gomock.NewController(t))

	conn.EXPECT().RemoteAddr().AnyTimes().Return(&net.TCPAddr{})

	calls := 0
	block := make(chan struct{})
	ln := &fakeListener{
		acceptFn: func() (net.Conn, error) {
			calls++
			if calls < testMaxRetries {
				return nil, errors.New("first batch error")
			}
			if calls == testMaxRetries {
				return conn, nil
			}
			offset := calls - testMaxRetries
			if offset < testMaxRetries {
				return nil, errors.New("second batch error")
			}
			if offset == testMaxRetries {
				return conn, nil
			}
			<-block
			return nil, errors.New("blocked")
		},
	}

	cChan := make(chan net.Conn, 2)
	go acceptLoop(ln, cChan, "test", testMaxRetries, testRetrySleep, testRetrySleepMax)

	for i := range 2 {
		select {
		case c, ok := <-cChan:

			assert.True(t, ok, "channel closed on delivery %d", i+1)
			assert.NotNil(t, c, "expected a non-nil connection, %d", i+1)

		case <-time.After(testTimeout):
			t.Fatalf("timed out on delivery %d", i+1)
		}
	}

	close(block)
}
