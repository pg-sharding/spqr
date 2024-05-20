package workloadlog

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type WorkloadLogMode string

const (
	All    = WorkloadLogMode("all")
	Client = WorkloadLogMode("client")
	None   = WorkloadLogMode("none")
)

type WorkloadLog interface {
	StartLogging(bool, uint)
	GetMode() WorkloadLogMode
	IsLogging() bool
	ClientMatches(uint) bool
	RecordWorkload(pgproto3.FrontendMessage, uint)
	StopLogging() error
}

type TimedMessage struct {
	Timestamp time.Time
	Msg       pgproto3.FrontendMessage
	Session   int
}

type WorkloadLogger struct {
	mode         WorkloadLogMode
	clients      map[uint]int
	curSession   int
	messageQueue chan TimedMessage
	ctx          context.Context
	cancelCtx    context.CancelFunc
	batchSize    int
	logFile      string
	mutex        sync.RWMutex
}

// NewLogger creates a new instance of WorkloadLog with the specified batch size and log file.
// It returns a pointer to the created WorkloadLogger.
//
// Parameters:
//   - batchSize: The number of messages to batch before writing to the log file.
//   - logFile: The path to the log file where the workload will be saved.
//
// Returns:
//   - WorkloadLog: A pointer to the created WorkloadLogger.
func NewLogger(batchSize int, logFile string) WorkloadLog {
	return &WorkloadLogger{
		mode:         None,
		clients:      map[uint]int{},
		messageQueue: make(chan TimedMessage),
		curSession:   0,
		batchSize:    batchSize,
		logFile:      logFile,
		mutex:        sync.RWMutex{},
	}
}

// StartLogging starts the logging process for the WorkloadLogger.
// If the 'all' parameter is set to true, it enables logging for all clients.
// If the 'all' parameter is set to false, it enables logging for a specific client identified by the 'id' parameter.
// The 'id' parameter is used to associate a client with a session.
// This function acquires a lock on the WorkloadLogger mutex to ensure thread safety.
// If the WorkloadLogger mode is set to None, it creates a new context and cancels any existing context.
// If the 'all' parameter is true, it sets the mode to All.
// If the 'all' parameter is false, it sets the mode to Client and associates the client with a session.
// The session ID is incremented for each new client.
// The logging process is executed in a separate goroutine by calling the 'serv' method.
//
// Parameters:
//   - all: A boolean flag to enable logging for all clients.
//   - id: The ID of the client to enable logging for.
func (wl *WorkloadLogger) StartLogging(all bool, id uint) {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()
	if wl.mode == None {
		wl.ctx, wl.cancelCtx = context.WithCancel(context.Background())
	}
	if all {
		wl.mode = All
	} else {
		wl.mode = Client
		wl.clients[id] = wl.curSession
		wl.curSession++
	}
	go wl.serv()
}

// IsLogging checks if the workload logger is currently logging.
// It returns true if the logger is in any mode other than None, and false otherwise.
func (wl *WorkloadLogger) IsLogging() bool {
	return !(wl.mode == None)
}

// GetMode returns the current mode of the WorkloadLogger.
func (wl *WorkloadLogger) GetMode() WorkloadLogMode {
	return wl.mode
}

// ClientMatches checks if the given client ID matches any client in the workload logger.
// It returns true if the client ID is found, otherwise it returns false.
//
// Parameters:
//   - client: The ID of the client to check.
//
// Returns:
//   - bool: A boolean value indicating if the client ID matches any client in the workload logger.
func (wl *WorkloadLogger) ClientMatches(client uint) bool {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()
	_, ok := wl.clients[client]
	return ok
}

// RecordWorkload records the workload by adding the given message, timestamp, and session to the message queue.
// It acquires a lock on the WorkloadLogger mutex to ensure thread safety.
// The message is added to the message queue along with the timestamp and session.
// The message queue is a channel of TimedMessage.
// The message queue is read by the 'serv' goroutine, which writes the messages to the log file.
//
// Parameters:
//   - msg: The message to record.
//   - client: The ID of the client that sent the message.
func (wl *WorkloadLogger) RecordWorkload(msg pgproto3.FrontendMessage, client uint) {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()
	wl.messageQueue <- TimedMessage{
		Msg:       msg,
		Timestamp: time.Now(),
		Session:   wl.clients[client],
	}
}

// StopLogging stops the logging session.
// It checks if there is an active logging session and cancels it.
// Returns an error if there was no active logging session.
func (wl *WorkloadLogger) StopLogging() error {
	if !wl.IsLogging() {
		return fmt.Errorf("was no active logging session")
	}
	wl.mode = None
	wl.cancelCtx()
	return nil
}

// serv is a goroutine that continuously listens for incoming messages in the messageQueue and saves them to a log file.
// It also handles the termination signal from the context and performs necessary cleanup operations.
func (wl *WorkloadLogger) serv() {
	interData := []byte{}

	for {
		select {
		case <-wl.ctx.Done():
			err := flush(interData, wl.logFile)
			if err != nil {
				spqrlog.Zero.Err(err).Msg("failed to save data to file")
			}
			wl.mutex.Lock()
			defer wl.mutex.Unlock()
			wl.clients = map[uint]int{}
			return
		case tm := <-wl.messageQueue:
			byt, err := EncodeMessage(tm)
			if err != nil {
				spqrlog.Zero.Err(err).Any("data", tm).Msg("failed to encode message")
			}
			interData = append(interData, byt...)
			if len(interData) > wl.batchSize {
				err = flush(interData, wl.logFile)
				if err != nil {
					spqrlog.Zero.Err(err).Msg("failed to save data to file")
				}
				interData = []byte{}
			}
		}
	}
}

// flush writes the intercepted data to the specified file.
// It opens the file in append mode, writes the data, and then closes the file.
// If the file does not exist, it will be created.
// The function returns an error if any error occurs during the file operations.
//
// Parameters:
//   - interceptedData: The data to write to the file.
//   - file: The path to the file where the data will be written.
//
// Returns:
//   - error: An error if any error occurs during the file operations.
func flush(interceptedData []byte, file string) error {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(interceptedData)
	if err != nil {
		return err
	}

	return nil
}

/*
Gets pgproto3.FrontendMessage and encodes it in binary with timestamp.
15 byte - timestamp
4 bytes - session number
1 byte - message header
4 bytes - message length (except header)
?? bytes - message bytes
*/
// EncodeMessage encodes a TimedMessage into a byte slice.
// It encodes the message and timestamp into binary format and appends them together.
// The encoded byte slice is returned along with any error encountered during encoding.
//
// Parameters:
//   - tm: The TimedMessage to encode.
//
// Returns:
//   - []byte: The encoded byte slice.
//   - error: An error if any error occurs during encoding.
func EncodeMessage(tm TimedMessage) ([]byte, error) {
	binMsg, err := tm.Msg.Encode(nil)
	if err != nil {
		return nil, err
	}

	binTime, err := tm.Timestamp.MarshalBinary()
	if err != nil {
		return nil, err
	}

	binSessionNum := make([]byte, 4)
	binary.BigEndian.PutUint32(binSessionNum, uint32(tm.Session))

	compl := append(binTime, binSessionNum...)
	compl = append(compl, binMsg...)
	return compl, nil
}
