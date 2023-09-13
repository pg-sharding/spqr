package workloadlog

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type WorkloadLogMode string

const (
	All          = WorkloadLogMode("all")
	SingleClient = WorkloadLogMode("singleClient")
	None         = WorkloadLogMode("none")
)

type WorkloadLog interface {
	StartLogging(bool, string) error
	GetMode() WorkloadLogMode
	IsLogging() bool
	ClientMatches(string) bool
	RecordWorkload(pgproto3.FrontendMessage, string)
	StopLogging() error
}

type TimedMessage struct {
	timestamp time.Time
	msg       pgproto3.FrontendMessage
	session   int
}

type WorkloadLogger struct {
	Mode         WorkloadLogMode
	Clients      map[string]int
	curSession   int
	messageQueue chan TimedMessage
	ctx          context.Context
	cancelCtx    context.CancelFunc
	batchSize    int
	logFile      string
}

func NewLogger(batchSize int, logFile string) WorkloadLog {
	return &WorkloadLogger{
		Mode:         None,
		Clients:      map[string]int{},
		messageQueue: make(chan TimedMessage),
		curSession:   0,
		batchSize:    batchSize,
		logFile:      logFile,
	}
}

func (wl *WorkloadLogger) StartLogging(all bool, id string) error {
	if all {
		wl.Mode = All
	} else {
		wl.Mode = SingleClient
		wl.Clients[id] = wl.curSession
		wl.curSession++
	}
	wl.ctx, wl.cancelCtx = context.WithCancel(context.Background())
	go serv(wl.messageQueue, wl.ctx, wl.batchSize, wl.logFile)
	return nil
}

func (wl *WorkloadLogger) IsLogging() bool {
	return !(wl.Mode == None)
}

func (wl *WorkloadLogger) GetMode() WorkloadLogMode {
	return wl.Mode
}

func (wl *WorkloadLogger) ClientMatches(client string) bool {
	_, ok := wl.Clients[client]
	return ok
}

func (wl *WorkloadLogger) RecordWorkload(msg pgproto3.FrontendMessage, client string) {
	wl.messageQueue <- TimedMessage{
		msg:       msg,
		timestamp: time.Now(),
		session:   wl.Clients[client],
	}
}

func (wl *WorkloadLogger) StopLogging() error {
	if !wl.IsLogging() {
		return fmt.Errorf("was no active logging session")
	}
	wl.Mode = None
	wl.cancelCtx()
	return nil
}

func serv(ch chan TimedMessage, ctx context.Context, batchSize int, logFile string) {
	interData := []byte{}

	for {
		select {
		case <-ctx.Done():
			err := flush(interData, logFile)
			if err != nil {
				spqrlog.Zero.Err(err).Msg("")
			}
			return
		case tm := <-ch:
			byt, err := encodeMessage(tm)
			if err != nil {
				spqrlog.Zero.Err(err).Msg("")
			}
			interData = append(interData, byt...)
			if len(interData) > batchSize {
				err = flush(interData, logFile)
				if err != nil {
					spqrlog.Zero.Err(err).Msg("")
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
func encodeMessage(tm TimedMessage) ([]byte, error) {
	binMsg := tm.msg.Encode(nil)

	binTime, err := tm.timestamp.MarshalBinary()
	if err != nil {
		return nil, err
	}

	binSessionNum := make([]byte, 4)
	binary.BigEndian.PutUint32(binSessionNum, uint32(tm.session))

	compl := append(binTime, binSessionNum...)
	compl = append(compl, binMsg...)
	return compl, nil
}
