package workloadreplay

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
)

// ReplayLogs replays logs from a file to a specified host, port, user, and database.
// It reads log messages from the file and sends them to the corresponding session.
// If a session does not exist, a new session is created.
// The function returns an error if there is any issue with opening the file or parsing the log messages.
//
// Parameters:
// - host: the hostname of the PostgreSQL server
// - port: the port number of the PostgreSQL server
// - user: the username to connect to the PostgreSQL server
// - db: the database name to connect to
// - file: the file containing the log messages
//
// Returns:
// - error: an error if there is any issue with opening the file or parsing the log messages
//
// TODO : unit tests
func ReplayLogs(host string, port string, user string, db string, file string) error {
	f, err := os.OpenFile(file, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close file")
		}
	}()

	sessionsMessageBuffer := map[int](chan workloadlog.TimedMessage){}
	var wg sync.WaitGroup
	for {
		//read next
		msg, err := parseFile(f)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		wg.Add(1)
		spqrlog.Zero.Info().Int("session", msg.Session).Msg("session num")
		// if session not exist, create
		if _, ok := sessionsMessageBuffer[msg.Session]; !ok {
			sessionsMessageBuffer[msg.Session] = make(chan workloadlog.TimedMessage)
			go startNewSession(host, port, user, db, sessionsMessageBuffer[msg.Session], &wg)
		}

		//send to session
		sessionsMessageBuffer[msg.Session] <- msg
	}
	wg.Wait()
	return nil
}

// startNewSession establishes a connection to a PostgreSQL server and starts a new session.
// It takes the host, port, user, db parameters as input and a channel to receive timed messages.
// The function sends a startup message to the server, receives the backend reply, and then enters a loop
// where it listens for timed messages from the channel. It sends the messages to the server and receives
// the corresponding replies. The loop continues until the context is done or a Terminate message is received.
// If an error occurs during the connection establishment, sending or receiving messages, the function logs
// the error and exits.
//
// Parameters:
// - host: the hostname of the PostgreSQL server
// - port: the port number of the PostgreSQL server
// - user: the username to connect to the PostgreSQL server
// - db: the database name to connect to
// - ch: a channel to receive timed messages
//
// TODO : unit tests
func startNewSession(host string, port string, user string, db string,
	ch chan workloadlog.TimedMessage, wg *sync.WaitGroup) {
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
		spqrlog.Zero.Error().Err(err).Msg(fmt.Sprintf("failed to establish connection to host %s:%s", host, port))
	}

	frontend := pgproto3.NewFrontend(bufio.NewReader(conn), conn)

	frontend.Send(startupMessage)
	if err := frontend.Flush(); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("failed to send msg to db")
	}
	err = receiveBackend(frontend)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error while receiving reply")
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
				spqrlog.Zero.Error().Err(err).Msg("failed to send msg to bd")
			}
			switch tm.Msg.(type) {
			case *pgproto3.Terminate:
				wg.Done()
				return
			default:
				err = receiveBackend(frontend)
				wg.Done()
				if err != nil {
					spqrlog.Zero.Error().Err(err).Msg("error while receiving reply")
				}
			}
		}
	}
}

// parseFile reads a file and parses the contents into a TimedMessage struct.
// It expects the file to have a specific format where each message is structured as follows:
// - 15 bytes: timestamp
// - 4 bytes: session number
// - 1 byte: message header
// - 4 bytes: message length (excluding header)
// - variable bytes: message content
//
// The function returns a TimedMessage struct and an error. If there is an error while parsing the file,
// the function returns an empty TimedMessage and the corresponding error.
//
// Parameters:
// - f: the file to parse
//
// Returns:
// - TimedMessage: the parsed message
// - error: an error if there is an issue with reading or parsing the file
//
// TODO : unit tests
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

// receiveBackend receives messages from the database frontend until a ReadyForQuery message is received.
// It returns an error if there is a failure in receiving the message.
//
// Parameters:
// - frontend: the frontend to receive messages from
//
// Returns:
// - error: an error if there is a failure in receiving the message
//
// TODO : unit tests
func receiveBackend(frontend *pgproto3.Frontend) error {
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
