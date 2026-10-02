package client_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	mockpool "github.com/pg-sharding/spqr/pkg/mock/pool"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/router/client"
	"github.com/pg-sharding/spqr/router/port"
	"github.com/pg-sharding/spqr/router/server"
	"go.uber.org/mock/gomock"

	"github.com/pg-sharding/spqr/pkg/conn"
	mock_conn "github.com/pg-sharding/spqr/pkg/mock/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShutdownKeepsServerForCleanup(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)

	rconn := mock_conn.NewMockRawConn(ctrl)
	startup, err := (&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersion30,
		Parameters: map[string]string{
			"user":     "u",
			"database": "d",
		},
	}).Encode(nil)
	require.NoError(err)
	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(bytes.NewReader(startup).Read).Times(2)
	rconn.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
		return len(b), nil
	}).Times(2)
	closed := rconn.EXPECT().Close().Return(nil)

	cl := client.NewPsqlClient(rconn, port.DefaultRouterPortType, false, "")
	require.NoError(cl.Init(nil))

	p := mockpool.NewMockConnectionProvider(ctrl)
	sh := mockshard.NewMockShardHostInstance(ctrl)
	key := kr.ShardKey{Name: "sh1"}
	p.EXPECT().ConnectionWithTSA(pool.ConnAllocParams{}, key).Return(sh, nil)
	p.EXPECT().Put(sh).After(closed).Return(nil)

	srv := server.NewShardServer(p)
	require.NoError(srv.AllocateGangMember(pool.ConnAllocParams{}, key))
	require.NoError(cl.AssignServerConn(srv))

	require.NoError(cl.Shutdown())
	require.Same(srv, cl.Server())
	require.NoError(cl.Reset())
	require.Nil(cl.Server())
}

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

	client := client.NewPsqlClient(rconn, port.DefaultRouterPortType, false, "")

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

	bytesQ1, _ := q1.Encode(nil)

	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {
			copy(b, bytesQ1)
			return len(bytesQ1), nil
		}).Times(1)

	q2 := &pgproto3.Query{
		String: "s2",
	}

	bytesQ2, _ := q2.Encode(nil)

	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {
			copy(b, bytesQ2)
			return len(bytesQ2), nil
		}).Times(1)

	client := client.NewPsqlClient(rconn, port.DefaultRouterPortType, false, "")

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

	client := client.NewPsqlClient(rconn, port.DefaultRouterPortType, false, "")

	err := client.Init(nil)
	assert.Equal(exprErr, err)
}
