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

func (wl *WorkloadLogger) IsLogging() bool {
	return !(wl.mode == None)
}

func (wl *WorkloadLogger) GetMode() WorkloadLogMode {
	return wl.mode
}

func (wl *WorkloadLogger) ClientMatches(client uint) bool {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()
	_, ok := wl.clients[client]
	return ok
}

func (wl *WorkloadLogger) RecordWorkload(msg pgproto3.FrontendMessage, client uint) {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()
	wl.messageQueue <- TimedMessage{
		Msg:       msg,
		Timestamp: time.Now(),
		Session:   wl.clients[client],
	}
}

func (wl *WorkloadLogger) StopLogging() error {
	if !wl.IsLogging() {
		return fmt.Errorf("was no active logging session")
	}
	wl.mode = None
	wl.cancelCtx()
	return nil
}

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
