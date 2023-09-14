package workloadlog

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
)

func TestEncodeMsg(t *testing.T) {
	assert := assert.New(t)
	tm := TimedMessage{
		timestamp: time.Now(),
		msg:       &pgproto3.Terminate{},
		session:   1,
	}

	byt, err := encodeMessage(tm)
	assert.NoError(err)
	assert.Equal(24, len(byt))
}

func TestFlushSavesToFile(t *testing.T) {
	assert := assert.New(t)

	err := os.Remove("testData/testfile.txt")
	if !(err == nil || errors.Is(err, os.ErrNotExist)) {
		assert.Fail("file is invalid")
	}

	actualStr := "Hello world!!!"
	err = flush([]byte(actualStr), "testData/testfile.txt")
	assert.NoError(err)

	f, err := os.OpenFile("testData/testfile.txt", os.O_RDONLY, 0600)
	assert.NoError(err)
	defer f.Close()

	bytes := make([]byte, len([]byte(actualStr)))
	length, err := f.Read(bytes)
	assert.NoError(err)
	assert.Equal(len([]byte(actualStr)), length)
	assert.Equal(actualStr, string(bytes))
}

func TestFlushAppendsToFile(t *testing.T) {
	assert := assert.New(t)

	err := os.Remove("testData/testfile.txt")
	if !(err == nil || errors.Is(err, os.ErrNotExist)) {
		assert.Fail("file is invalid")
	}

	actualStr := "Hello world!!!"
	err = flush([]byte(actualStr[:5]), "testData/testfile.txt")
	assert.NoError(err)
	err = flush([]byte(actualStr[5:]), "testData/testfile.txt")
	assert.NoError(err)

	f, err := os.OpenFile("testData/testfile.txt", os.O_RDONLY, 0600)
	assert.NoError(err)
	defer f.Close()

	bytes := make([]byte, len([]byte(actualStr)))
	length, err := f.Read(bytes)
	assert.NoError(err)
	assert.Equal(len([]byte(actualStr)), length)
	assert.Equal(actualStr, string(bytes))
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
	}
	logger.StartLogging(false, "123")

	assert.Equal(1, len(logger.clients))

	err := logger.StopLogging()
	assert.NoError(err)

	time.Sleep(time.Second)
	assert.Equal(0, len(logger.clients))
}

func TestLoggerLogsMessages(t *testing.T) {
	assert := assert.New(t)

	err := os.Remove("testData/testfile.txt")
	if !(err == nil || errors.Is(err, os.ErrNotExist)) {
		assert.Fail("file is invalid")
	}
	actualQuery := "Hello world!!!"

	logger := NewLogger(10, "testData/testfile.txt")
	assert.Equal(None, logger.GetMode())

	logger.StartLogging(true, "")
	assert.Equal(All, logger.GetMode())

	logger.RecordWorkload(&pgproto3.Query{String: actualQuery}, "myClient")

	err = logger.StopLogging()
	assert.NoError(err)
	time.Sleep(time.Second)

	f, err := os.OpenFile("testData/testfile.txt", os.O_RDONLY, 0600)
	assert.NoError(err)
	defer f.Close()

	bytes := make([]byte, 100)
	_, err = f.Read(bytes)
	assert.NoError(err)
	assert.True(strings.Contains(string(bytes), actualQuery))
}
