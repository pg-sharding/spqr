package workloadreplay

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
)

func ReplayLogs(host string, port string, user string, db string, file string) error {
	f, err := os.OpenFile(file, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	sessionsMessageBuffer := map[int](chan workloadlog.TimedMessage){}

	for {
		//read next
		msg, err := parseFile(f)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		spqrlog.Zero.Info().Int("session", msg.Session).Msg("session num")
		// if session not exist, create
		if _, ok := sessionsMessageBuffer[msg.Session]; !ok {
			sessionsMessageBuffer[msg.Session] = make(chan workloadlog.TimedMessage)
			go startNewSession(host, port, user, db, sessionsMessageBuffer[msg.Session])
		}

		//send to session
		sessionsMessageBuffer[msg.Session] <- msg
	}
}

func startNewSession(host string, port string, user string, db string, ch chan workloadlog.TimedMessage) {
	ctx := context.Background()

	startupMessage := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     user,
			"database": db,
		},
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		spqrlog.Zero.Error().Err(fmt.Errorf("failed to establish connection to host %s - %w", fmt.Sprintf("%s:%s", host, port), err))
	}

	frontend := pgproto3.NewFrontend(bufio.NewReader(conn), conn)

	frontend.Send(startupMessage)
	if err := frontend.Flush(); err != nil {
		spqrlog.Zero.Error().Err(fmt.Errorf("failed to send msg to bd %w", err))
	}
	err = recieveBackend(frontend)
	if err != nil {
		spqrlog.Zero.Error().Err(err)
	}

	var tm workloadlog.TimedMessage
	var prevSentTime, prevMsgTime time.Time
	for {
		select {
		case <-ctx.Done():
			os.Exit(1)
		case tm = <-ch:
			timeNow := time.Now()
			timer := time.NewTimer(tm.Timestamp.Sub(prevMsgTime) - timeNow.Sub(prevSentTime))
			prevSentTime = timeNow
			prevMsgTime = tm.Timestamp
			<-timer.C

			spqrlog.Zero.Info().Any("msg %+v", tm.Msg).Msg("read query")
			frontend.Send(tm.Msg)
			if err := frontend.Flush(); err != nil {
				spqrlog.Zero.Error().Err(fmt.Errorf("failed to send msg to bd %w", err))
			}
			switch tm.Msg.(type) {
			case *pgproto3.Terminate:
				return
			default:
				err = recieveBackend(frontend)
				if err != nil {
					spqrlog.Zero.Error().Err(err)
				}
			}
		}
	}
}

func parseFile(f *os.File) (workloadlog.TimedMessage, error) {
	// 15 byte - timestamp
	// 4 bytes - session number
	// 1 byte - message header
	// 4 bytes - message length (except header)
	// ?? bytes - message bytes

	tm := workloadlog.TimedMessage{
		Timestamp: time.Now(),
		Msg:       nil,
	}

	//timestamp
	timeb := make([]byte, 15)
	_, err := f.Read(timeb)
	if err != nil {
		return tm, err
	}
	var ti time.Time
	err = ti.UnmarshalBinary(timeb)
	if err != nil {
		return tm, err
	}

	//session
	rawSes := make([]byte, 4)

	_, err = f.Read(rawSes)
	if err != nil {
		return tm, err
	}

	sesNum := int(binary.BigEndian.Uint32(rawSes) - 4)

	//header
	tip := make([]byte, 1)

	_, err = f.Read(tip)
	if err != nil {
		return tm, err
	}

	//size
	rawSize := make([]byte, 4)

	_, err = f.Read(rawSize)
	if err != nil {
		return tm, err
	}

	msgSize := int(binary.BigEndian.Uint32(rawSize) - 4)

	//message
	msg := make([]byte, msgSize)
	_, err = f.Read(msg)
	if err != nil {
		return tm, err
	}

	var fm pgproto3.FrontendMessage
	switch string(tip) {
	case "Q":
		fm = &pgproto3.Query{}
	case "X":
		fm = &pgproto3.Terminate{}
	}
	err = fm.Decode(msg)
	if err != nil {
		return tm, err
	}

	tm.Timestamp = ti
	tm.Msg = fm
	tm.Session = sesNum

	return tm, nil
}

func recieveBackend(frontend *pgproto3.Frontend) error {
	for {
		retmsg, err := frontend.Receive()
		if err != nil {
			return fmt.Errorf("failed to receive msg from db %w", err)
		}

		switch retmsg.(type) {
		case *pgproto3.ReadyForQuery:
			return nil
		default:
			continue
		}
	}
}
