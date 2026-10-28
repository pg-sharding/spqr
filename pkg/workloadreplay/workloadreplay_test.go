package workloadreplay

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/workloadlog"
)

const (
	timestampSize = 15
	sessionSize   = 4
	typeOffset    = timestampSize + sessionSize
	lengthOffset  = typeOffset + 1
	payloadOffset = lengthOffset + 4
)

var fixedTimestamp = time.Date(2026, 3, 14, 12, 30, 45, 123456789, time.UTC)

// Helpers for tests.
func encodeRecord(t *testing.T, session int, msg pgproto3.FrontendMessage) []byte {
	t.Helper()
	return encodeRecordAt(t, fixedTimestamp, session, msg)
}

func encodeRecordAt(t *testing.T, timestamp time.Time, session int, msg pgproto3.FrontendMessage) []byte {
	t.Helper()
	data, err := workloadlog.EncodeMessage(workloadlog.TimedMessage{
		Timestamp: timestamp,
		Session:   session,
		Msg:       msg,
	})
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	return data
}

func fileWithData(t *testing.T, data []byte) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workload.bin")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func assertMessage(t *testing.T, got workloadlog.TimedMessage, session int, msg pgproto3.FrontendMessage) {
	t.Helper()
	assertMessageAt(t, got, fixedTimestamp, session, msg)
}

func assertMessageAt(t *testing.T, got workloadlog.TimedMessage, timestamp time.Time, session int, msg pgproto3.FrontendMessage) {
	t.Helper()
	if !got.Timestamp.Equal(timestamp) {
		t.Errorf("timestamp = %v, want %v", got.Timestamp, timestamp)
	}
	if got.Session != session {
		t.Errorf("session = %d, want %d", got.Session, session)
	}
	if !reflect.DeepEqual(got.Msg, msg) {
		t.Errorf("message = %#v, want %#v", got.Msg, msg)
	}
}

func assertErrorAndZeroResult(t *testing.T, f *os.File, want error) {
	t.Helper()
	got, err := parseFile(f)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want errors.Is(..., %v)", err, want)
	}
	if !reflect.DeepEqual(got, workloadlog.TimedMessage{}) {
		t.Errorf("on error got %+v, want zero-value TimedMessage", got)
	}
}

func failOnPanic(t *testing.T) {
	t.Helper()
	if value := recover(); value != nil {
		t.Errorf("parseFile panicked instead of returning an error: %v", value)
	}
}

func TestParseFileValidRecords(t *testing.T) {
	tests := []struct {
		name    string
		session int
		msg     pgproto3.FrontendMessage
	}{
		{"query and session zero", 0, &pgproto3.Query{String: "SELECT 1"}},
		{"nonzero session", 5, &pgproto3.Query{String: "SELECT 2"}},
		{"empty query", 17, &pgproto3.Query{String: ""}},
		{"unicode query", 42, &pgproto3.Query{String: "SELECT 'Привет 🌍'"}},
		{"terminate", 3, &pgproto3.Terminate{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := fileWithData(t, encodeRecord(t, tt.session, tt.msg))
			got, err := parseFile(f)
			if err != nil {
				t.Fatalf("parseFile: %v", err)
			}
			assertMessage(t, got, tt.session, tt.msg)
		})
	}
}

func TestParseFileEmptyFile(t *testing.T) {
	assertErrorAndZeroResult(t, fileWithData(t, nil), io.EOF)
}

func TestParseFileWireFormat(t *testing.T) {
	// Construct the record independently of workloadlog.EncodeMessage:
	// binary timestamp version 1, year 1 UTC; session 0x01020304;
	// Query with length 6 (four length bytes plus "x\x00").
	data := []byte{
		1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff,
		1, 2, 3, 4,
		'Q', 0, 0, 0, 6, 'x', 0,
	}
	f := fileWithData(t, data)
	got, err := parseFile(f)
	if err != nil {
		t.Fatalf("parseFile: %v", err)
	}
	assertMessageAt(t, got, time.Time{}, 0x01020304, &pgproto3.Query{String: "x"})
	assertErrorAndZeroResult(t, f, io.EOF)
}

func TestParseFileConsecutiveRecords(t *testing.T) {
	records := []struct {
		timestamp time.Time
		session   int
		msg       pgproto3.FrontendMessage
	}{
		{fixedTimestamp, 0, &pgproto3.Query{String: "SELECT 1"}},
		{fixedTimestamp.Add(time.Second).In(time.FixedZone("UTC+05:30", 19800)), 7, &pgproto3.Query{String: "SELECT 2"}},
		{fixedTimestamp.Add(2 * time.Second), 0, &pgproto3.Terminate{}},
	}

	var data []byte
	for _, r := range records {
		data = append(data, encodeRecordAt(t, r.timestamp, r.session, r.msg)...)
	}
	f := fileWithData(t, data)

	for i, want := range records {
		got, err := parseFile(f)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		assertMessageAt(t, got, want.timestamp, want.session, want.msg)
	}
	assertErrorAndZeroResult(t, f, io.EOF)
}

func TestParseFileTruncatedSecondRecord(t *testing.T) {
	first := encodeRecord(t, 2, &pgproto3.Query{String: "SELECT 1"})
	second := encodeRecord(t, 2, &pgproto3.Query{String: "SELECT 2"})
	data := append(append([]byte(nil), first...), second[:len(second)-1]...)
	f := fileWithData(t, data)

	got, err := parseFile(f)
	if err != nil {
		t.Fatalf("first record: %v", err)
	}
	assertMessage(t, got, 2, &pgproto3.Query{String: "SELECT 1"})
	assertErrorAndZeroResult(t, f, io.ErrUnexpectedEOF)
}

func TestParseFileMalformedMessages(t *testing.T) {
	query := encodeRecord(t, 1, &pgproto3.Query{String: "SELECT 1"})
	terminate := encodeRecord(t, 1, &pgproto3.Terminate{})

	badTimestamp := append([]byte(nil), query...)
	badTimestamp[0] = 0xff // invalid time.MarshalBinary version

	missingNUL := append([]byte(nil), query...)
	missingNUL[len(missingNUL)-1] = '!'

	extraQueryBytes := append(append([]byte(nil), query...), '!')
	binary.BigEndian.PutUint32(extraQueryBytes[lengthOffset:payloadOffset],
		binary.BigEndian.Uint32(extraQueryBytes[lengthOffset:payloadOffset])+1)

	terminateWithPayload := append(append([]byte(nil), terminate...), '!')
	binary.BigEndian.PutUint32(terminateWithPayload[lengthOffset:payloadOffset], 5)

	unknownType := append([]byte(nil), query...)
	unknownType[typeOffset] = 'Z'

	emptyQueryPayload := append([]byte(nil), query[:payloadOffset]...)
	binary.BigEndian.PutUint32(emptyQueryPayload[lengthOffset:payloadOffset], 4)

	tests := []struct {
		name string
		data []byte
	}{
		{"corrupt timestamp", badTimestamp},
		{"query without NUL", missingNUL},
		{"query with trailing garbage", extraQueryBytes},
		{"terminate with payload", terminateWithPayload},
		{"unsupported message type", unknownType},
		{"query without payload", emptyQueryPayload},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer failOnPanic(t)
			got, err := parseFile(fileWithData(t, tt.data))
			if err == nil {
				t.Fatalf("parseFile succeeded: %+v, want an error", got)
			}
			if !reflect.DeepEqual(got, workloadlog.TimedMessage{}) {
				t.Errorf("on error got %+v, want zero-value TimedMessage", got)
			}
		})
	}
}

func TestParseFileInvalidLengths(t *testing.T) {
	tests := []struct {
		name   string
		length uint32
	}{
		{"zero", 0},
		{"one", 1},
		{"two", 2},
		{"three", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer failOnPanic(t)
			data := encodeRecord(t, 1, &pgproto3.Terminate{})
			binary.BigEndian.PutUint32(data[lengthOffset:payloadOffset], tt.length)
			got, err := parseFile(fileWithData(t, data))
			if err == nil {
				t.Fatalf("length %d accepted: %+v", tt.length, got)
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("length %d: got %v, want a length validation error before reading the payload", tt.length, err)
			}
			if !reflect.DeepEqual(got, workloadlog.TimedMessage{}) {
				t.Errorf("on error got %+v, want zero-value TimedMessage", got)
			}
		})
	}
}

func TestParseFileClosedFile(t *testing.T) {
	f := fileWithData(t, encodeRecord(t, 1, &pgproto3.Terminate{}))
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	assertErrorAndZeroResult(t, f, os.ErrClosed)
}
