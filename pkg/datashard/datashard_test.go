package datashard

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	mockconn "github.com/pg-sharding/spqr/pkg/mock/conn"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDataPendingAfterQueryThenParse(t *testing.T) {
	for name, finish := range map[string]pgproto3.FrontendMessage{
		"flush": &pgproto3.Flush{},
		"sync":  &pgproto3.Sync{},
	} {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			backend := mockconn.NewMockDBInstance(ctrl)
			sh := &Conn{dedicated: backend}

			for _, msg := range []pgproto3.FrontendMessage{
				&pgproto3.Query{},
				&pgproto3.Parse{Query: "SELECT 1"},
				finish,
			} {
				backend.EXPECT().Send(msg).Return(nil)
				require.NoError(t, sh.Send(msg))
			}

			for _, msg := range []pgproto3.BackendMessage{
				&pgproto3.EmptyQueryResponse{},
				&pgproto3.ReadyForQuery{TxStatus: 'I'},
				&pgproto3.ParseComplete{},
			} {
				backend.EXPECT().Receive().Return(msg, nil)
				_, err := sh.Receive()
				require.NoError(t, err)
				assert.True(t, sh.DataPending())
			}

			if _, flushed := finish.(*pgproto3.Flush); flushed {
				assert.Zero(t, sh.Sync())
				backend.EXPECT().Send(&pgproto3.Sync{}).Return(nil)
				require.NoError(t, sh.Send(&pgproto3.Sync{}))
			}
			backend.EXPECT().Receive().Return(&pgproto3.ReadyForQuery{TxStatus: 'I'}, nil)
			_, err := sh.Receive()
			require.NoError(t, err)
			assert.False(t, sh.DataPending())
			assert.Zero(t, sh.Sync())
		})
	}
}

func TestDataPendingAfterCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
	backend := mockconn.NewMockDBInstance(ctrl)
	sh := &Conn{dedicated: backend}

	for _, msg := range []pgproto3.FrontendMessage{
		&pgproto3.Query{String: "COPY t FROM STDIN"},
		&pgproto3.CopyData{Data: []byte("1\n")},
		&pgproto3.CopyDone{},
		&pgproto3.Flush{},
	} {
		backend.EXPECT().Send(msg).Return(nil)
		require.NoError(t, sh.Send(msg))
	}

	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.CopyInResponse{},
		&pgproto3.CommandComplete{CommandTag: []byte("COPY 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	} {
		backend.EXPECT().Receive().Return(msg, nil)
		_, err := sh.Receive()
		require.NoError(t, err)
	}
	assert.False(t, sh.DataPending())
	assert.Zero(t, sh.Sync())
}

func TestHasPrepareStatement(t *testing.T) {

	var (
		desc1 = &prepstatement.PreparedStatementDescriptor{
			NoData: false,
			ParamDesc: &pgproto3.ParameterDescription{
				ParameterOIDs: []uint32{1, 2, 3},
			},
		}
		desc2 = &prepstatement.PreparedStatementDescriptor{
			NoData: true,
			ParamDesc: &pgproto3.ParameterDescription{
				ParameterOIDs: []uint32{4, 5},
			},
		}
	)

	tests := []struct {
		name           string
		setupConn      func() *Conn
		hash           uint64
		useRealShardId bool
		testShardId    uint
		expectedExists bool
		expectedDesc   *prepstatement.PreparedStatementDescriptor
	}{

		{
			name: "statement exists and shard ID matches",
			setupConn: func() *Conn {
				conn := &Conn{
					stmtDesc: make(map[uint64]*prepstatement.PreparedStatementDescriptor),
				}
				conn.stmtDesc[12345] = desc1
				return conn
			},
			hash:           12345,
			useRealShardId: true,
			expectedExists: true,
			expectedDesc:   desc1,
		},

		{
			name: "statement does not exist (key absent)",
			setupConn: func() *Conn {
				conn := &Conn{
					stmtDesc: make(map[uint64]*prepstatement.PreparedStatementDescriptor),
				}
				conn.stmtDesc[1] = desc1
				return conn
			},
			hash:           99999,
			useRealShardId: true,
			expectedExists: false,
			expectedDesc:   nil,
		},
		{
			name: "statement does not exist (empty map)",
			setupConn: func() *Conn {
				conn := &Conn{
					stmtDesc: make(map[uint64]*prepstatement.PreparedStatementDescriptor),
				}
				return conn
			},
			hash:           12345,
			useRealShardId: true,
			expectedExists: false,
			expectedDesc:   nil,
		},
		{
			name: "statement does not exist (nil map)",
			setupConn: func() *Conn {
				conn := &Conn{
					stmtDesc: nil,
				}
				return conn
			},
			hash:           12345,
			useRealShardId: true,
			expectedExists: false,
			expectedDesc:   nil,
		},

		{
			name: "statement exists but shard ID mismatch (random ID)",
			setupConn: func() *Conn {
				conn := &Conn{
					stmtDesc: make(map[uint64]*prepstatement.PreparedStatementDescriptor),
				}
				conn.stmtDesc[12345] = desc2
				return conn
			},
			hash:           12345,
			useRealShardId: false,
			testShardId:    99999,
			expectedExists: false,
			expectedDesc:   nil,
		},
		{
			name: "statement exists but shard ID mismatch (zero ID)",
			setupConn: func() *Conn {
				conn := &Conn{
					stmtDesc: make(map[uint64]*prepstatement.PreparedStatementDescriptor),
				}
				conn.stmtDesc[12345] = desc1
				return conn
			},
			hash:           12345,
			useRealShardId: false,
			testShardId:    0,
			expectedExists: false,
			expectedDesc:   nil,
		},

		{
			name: "statement with hash 0 exists",
			setupConn: func() *Conn {
				conn := &Conn{
					stmtDesc: make(map[uint64]*prepstatement.PreparedStatementDescriptor),
				}
				conn.stmtDesc[0] = desc2
				return conn
			},
			hash:           0,
			useRealShardId: true,
			expectedExists: true,
			expectedDesc:   desc2,
		},
		{
			name: "statement with hash 0 does not exist",
			setupConn: func() *Conn {
				conn := &Conn{
					stmtDesc: make(map[uint64]*prepstatement.PreparedStatementDescriptor),
				}
				conn.stmtDesc[1] = desc1
				return conn
			},
			hash:           0,
			useRealShardId: true,
			expectedExists: false,
			expectedDesc:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := tt.setupConn()

			var actualShardId uint
			if tt.useRealShardId {
				actualShardId = conn.ID()
			} else {
				actualShardId = tt.testShardId
			}

			exists, desc := conn.HasPrepareStatement(tt.hash, actualShardId)

			assert.Equal(t, tt.expectedExists, exists)

			if tt.expectedDesc != nil {
				assert.Equal(t, tt.expectedDesc, desc)
			} else {
				assert.Nil(t, desc)
			}
		})
	}
}
