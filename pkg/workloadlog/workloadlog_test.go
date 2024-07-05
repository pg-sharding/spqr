package workloadlog

import (
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

// TestEncodeMsg tests the EncodeMessage function.
func TestEncodeMsg(t *testing.T) {
	assert := assert.New(t)
	tm := TimedMessage{
		Timestamp: time.Now(),
		Msg:       &pgproto3.Terminate{},
		Session:   1,
	}

	byt, err := EncodeMessage(tm)
	assert.NoError(err)
	assert.Equal(24, len(byt))
}

// TestFlushErrorOnUnexistingDir tests the behavior of the flush function when trying to flush data to a non-existing directory.
func TestFlushErrorOnUnexistingDir(t *testing.T) {
	assert := assert.New(t)

	actualStr := "Hello world!!!"
	err := flush([]byte(actualStr), "IAmShureThereIsNoSuchDir/testfile.txt")
	assert.Error(err)
}

// TestStartLoggingForAll tests the StartLogging method of the Logger struct
// when the mode is set to All.
func TestStartLoggingForAll(t *testing.T) {
	assert := assert.New(t)

	logger := NewLogger(10, "testData/file")
	logger.StartLogging(true, 0)

	assert.Equal(All, logger.GetMode())
}

// TestStartLoggingForClients tests the StartLogging method of the Logger struct
// to ensure that it correctly starts logging for clients and matches the client IDs.
func TestStartLoggingForClients(t *testing.T) {
	assert := assert.New(t)

	logger := NewLogger(10, "testData/file")
	logger.StartLogging(false, 123)
	logger.StartLogging(false, 124)

	assert.Equal(Client, logger.GetMode())

	assert.True(logger.ClientMatches(123))
	assert.True(logger.ClientMatches(124))
	assert.False(logger.ClientMatches(145))
}

// TestStopLogging tests the StopLogging function of the Logger type.
func TestStopLogging(t *testing.T) {
	assert := assert.New(t)

	logger := NewLogger(10, "testData/file")
	assert.Equal(None, logger.GetMode())

	logger.StartLogging(true, 0)
	assert.Equal(All, logger.GetMode())

	err := logger.StopLogging()
	assert.NoError(err)
	assert.Equal(None, logger.GetMode())
}

// TestStopLoggingWhenNotLogging tests the behavior of the StopLogging method when the logger is not logging.
func TestStopLoggingWhenNotLogging(t *testing.T) {
	assert := assert.New(t)

	logger := NewLogger(10, "testData/file")
	assert.Equal(None, logger.GetMode())

	err := logger.StopLogging()
	assert.Error(err)
}

// TestStopLoggingClearsClientList tests the behavior of the StopLogging method in the WorkloadLogger struct.
// It verifies that calling StopLogging clears the client list in the logger.
func TestStopLoggingClearsClientList(t *testing.T) {
	assert := assert.New(t)

	logger := &WorkloadLogger{
		mode:         None,
		clients:      map[uint]int{},
		messageQueue: make(chan TimedMessage),
		curSession:   0,
		batchSize:    12,
		logFile:      "testData/file",
		mutex:        sync.RWMutex{},
	}
	logger.StartLogging(false, 123)

	assert.Equal(1, len(logger.clients))

	err := logger.StopLogging()
	assert.NoError(err)

	time.Sleep(time.Second)
	logger.mutex.Lock()
	defer logger.mutex.Unlock()
	assert.Equal(0, len(logger.clients))
}
