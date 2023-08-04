package client_test

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/router/client"

	"github.com/pg-sharding/spqr/pkg/conn"
	mock_conn "github.com/pg-sharding/spqr/pkg/mock/conn"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
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

	rconn.EXPECT().Read(gomock.Any()).DoAndReturn(
		func(b []byte) (int, error) {

			canreq := pgproto3.CancelRequest{
				ProcessID: 7,
				SecretKey: 12,
			}
			binary.BigEndian.PutUint32(b, conn.CANCELREQ)

			binary.BigEndian.PutUint32(b[4:], canreq.ProcessID)
			binary.BigEndian.PutUint32(b[8:], canreq.SecretKey)
			return 12, nil
		}).Times(1)

	client := client.NewPsqlClient(rconn)

	err := client.Init(nil)
	assert.Equal(uint32(7), client.CancelMsg().ProcessID)
	assert.Equal(uint32(12), client.CancelMsg().SecretKey)
	assert.NoError(err)
}
