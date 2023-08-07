package workloadlog

import (
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

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

func TestFlushErrorOnUnexistingDir(t *testing.T) {
	assert := assert.New(t)

	actualStr := "Hello world!!!"
	err := flush([]byte(actualStr), "IAmShureThereIsNoSuchDir/testfile.txt")
	assert.Error(err)
}

func TestStartLoggingForAll(t *testing.T) {
	assert := assert.New(t)

	logger := NewLogger(10, "testData/file")
	logger.StartLogging(true, "")

	assert.Equal(All, logger.GetMode())
}

func TestStartLoggingForClients(t *testing.T) {
	assert := assert.New(t)

	logger := NewLogger(10, "testData/file")
	logger.StartLogging(false, "123")
	logger.StartLogging(false, "abc")

	assert.Equal(Client, logger.GetMode())

	assert.True(logger.ClientMatches("123"))
	assert.True(logger.ClientMatches("abc"))
	assert.False(logger.ClientMatches("0x45632"))
}

func TestStopLogging(t *testing.T) {
	assert := assert.New(t)

	logger := NewLogger(10, "testData/file")
	assert.Equal(None, logger.GetMode())

	logger.StartLogging(true, "")
	assert.Equal(All, logger.GetMode())

	err := logger.StopLogging()
	assert.NoError(err)
	assert.Equal(None, logger.GetMode())
}

func TestStopLoggingWhenNotLogging(t *testing.T) {
	assert := assert.New(t)

	logger := NewLogger(10, "testData/file")
	assert.Equal(None, logger.GetMode())

	err := logger.StopLogging()
	assert.Error(err)
}

func TestStopLoggingClearsClientList(t *testing.T) {
	assert := assert.New(t)

	logger := &WorkloadLogger{
		mode:         None,
		clients:      map[string]int{},
		messageQueue: make(chan TimedMessage),
		curSession:   0,
		batchSize:    12,
		logFile:      "testData/file",
		mutex:        sync.RWMutex{},
	}
	logger.StartLogging(false, "123")

	assert.Equal(1, len(logger.clients))

	err := logger.StopLogging()
	assert.NoError(err)

	time.Sleep(time.Second)
	logger.mutex.Lock()
	defer logger.mutex.Unlock()
	assert.Equal(0, len(logger.clients))
}
