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

type WorcloadLogMode string

const (
	All          = WorcloadLogMode("all")
	SingleClient = WorcloadLogMode("singleClient")
	None         = WorcloadLogMode("none")
)

type WorkloadLogIface interface {
	StartLogging(bool, string) error
	IsLogging() bool
	WriteLog(msg pgproto3.FrontendMessage, client string)
	StopLogging() error
}

type TimedMessage struct {
	timestamp time.Time
	msg       pgproto3.FrontendMessage
	session   int
}

type WorkloadLogger struct {
	Mode     WorcloadLogMode
	ClientId string
	ch       chan TimedMessage
	ctx      context.Context
	cl       context.CancelFunc
}

func NewLogger() WorkloadLogIface {
	return &WorkloadLogger{
		Mode: None,
		ch:   make(chan TimedMessage),
	}
}

func (wl *WorkloadLogger) StartLogging(all bool, id string) error {
	if all {
		wl.Mode = All
	} else {
		wl.Mode = SingleClient
		wl.ClientId = id
	}
	wl.ctx, wl.cl = context.WithCancel(context.Background()) //TODO many clients
	spqrlog.Zero.Error().Msg("starting gorutine")
	go serv(wl.ch, wl.ctx)
	spqrlog.Zero.Error().Msg("started gorutine")
	return nil
}

func (wl *WorkloadLogger) IsLogging() bool {
	return !(wl.Mode == None)
}

func (wl *WorkloadLogger) WriteLog(msg pgproto3.FrontendMessage, client string) {
	wl.ch <- TimedMessage{
		msg:       msg,
		timestamp: time.Now(),
		session:   0, //TODO convert id to session
	}
}

func (wl *WorkloadLogger) StopLogging() error {
	if !wl.IsLogging() {
		return fmt.Errorf("was no active logging session")
	}
	spqrlog.Zero.Error().Msg("stopping logs")
	wl.Mode = None
	wl.cl()
	return nil
}

func serv(ch chan TimedMessage, ctx context.Context) error {
	interData := []byte{}

	for {
		select {
		case <-ctx.Done():
			spqrlog.Zero.Error().Msg("context done")
			err := flush(interData)
			if err != nil {
				spqrlog.Zero.Err(err).Msg("")
			}
			return nil
		case tm := <-ch:
			byt, err := encodeMessage(tm)
			if err != nil {
				spqrlog.Zero.Err(err).Msg("")
			}
			interData = append(interData, byt...)
			if len(interData) > 1000000 {
				err = flush(interData)
				if err != nil {
					spqrlog.Zero.Err(err).Msg("")
				}
				interData = []byte{}
			}
		}
	}
}

func flush(interceptedData []byte) error {
	spqrlog.Zero.Error().Msg("saving file logs")
	f, err := os.OpenFile("mylogs.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
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
