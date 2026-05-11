package client_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/port"
	"go.uber.org/mock/gomock"

	"github.com/pg-sharding/spqr/pkg/conn"
	mock_conn "github.com/pg-sharding/spqr/pkg/mock/conn"
	"github.com/stretchr/testify/assert"
)

func TestCancel(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	rconn := mock_conn.NewMockRawConn(ctrl)
	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {
			binary.BigEndian.PutUint32(b, 16)

			return 4, nil
		}).Times(1)

	key := 12
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(key))
	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {

			canreq := pgproto3.CancelRequest{
				ProcessID: 7,
				SecretKey: buf,
			}
			binary.BigEndian.PutUint32(b, conn.CANCELREQ)

			binary.BigEndian.PutUint32(b[4:], canreq.ProcessID)
			copy(b[8:], canreq.SecretKey)
			return 12, nil
		}).Times(1)

	client := client.NewPsqlClient(rconn, port.DefaultRouterPortType, "BLOCK", false, "")

	err := client.Init(nil)
	assert.Equal(uint32(7), client.CancelMsg().ProcessID)
	assert.Equal(buf, client.CancelMsg().SecretKey)
	assert.NoError(err)
}

func TestPeek(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	rconn := mock_conn.NewMockRawConn(ctrl)

	req := pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersion30,
		Parameters: map[string]string{
			"user":     "u",
			"database": "d",
		},
	}

	bts, _ := req.Encode(nil)

	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {
			copy(b, bts[:4])
			return 4, nil
		}).Times(1)

	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {
			copy(b, bts[4:])
			return len(bts) - 4, nil
		}).Times(1)

	q1 := &pgproto3.Query{
		String: "s1",
	}

	btsQ1, _ := q1.Encode(nil)

	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {
			copy(b, btsQ1)
			return len(btsQ1), nil
		}).Times(1)

	q2 := &pgproto3.Query{
		String: "s2",
	}

	btsQ2, _ := q2.Encode(nil)

	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {
			copy(b, btsQ2)
			return len(btsQ2), nil
		}).Times(1)

	client := client.NewPsqlClient(rconn, port.DefaultRouterPortType, "BLOCK", false, "")

	err := client.Init(nil)
	assert.NoError(err)

	m1, err := client.Peek()
	assert.NoError(err)
	assert.Equal(q1, m1)
	m2, err := client.Peek()
	assert.NoError(err)
	assert.Equal(q1, m2)
	m3, err := client.Receive()
	assert.NoError(err)
	assert.Equal(q1, m3)

	m4, err := client.Peek()
	assert.NoError(err)
	assert.Equal(q2, m4)
	m5, err := client.Peek()
	assert.NoError(err)
	assert.Equal(q2, m5)
	m6, err := client.Receive()
	assert.NoError(err)
	assert.Equal(q2, m6)
}

func TestNoGSSAPI(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)

	rconn := mock_conn.NewMockRawConn(ctrl)

	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {
			binary.BigEndian.PutUint32(b, 8)

			return 4, nil
		}).Times(1)

	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {

			binary.BigEndian.PutUint32(b, conn.GSSREQ)
			return 4, nil
		}).Times(1)

	exprErr := fmt.Errorf("stop test")

	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(_ []byte) (int, error) {

			return 0, exprErr
		}).Times(1)

	rconn.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {

		assert.Equal(1, len(b))

		assert.Equal(uint8('N'), b[0])
		return 4, nil
	}).Times(1)

	client := client.NewPsqlClient(rconn, port.DefaultRouterPortType, "BLOCK", false, "")

	err := client.Init(nil)
	assert.Equal(exprErr, err)
}
