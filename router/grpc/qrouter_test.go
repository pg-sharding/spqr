package grpc

import (
	"context"
	"net"
	"testing"
	"time"

	metaMock "github.com/pg-sharding/spqr/pkg/mock/meta"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mustGetUnreachableAddr(t *testing.T) string {
	t.Helper()

	const maxAttempts = 10
	for i := 0; i < maxAttempts; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		assert.NoError(t, err)
		addr := listener.Addr().String()
		_ = listener.Close()

		conn, dialErr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if dialErr != nil {
			return addr
		}
		_ = conn.Close()
	}

	t.Fatalf("failed to get an unreachable tcp address")
	return ""
}

func TestAddDataShardRejectsUnreachableHosts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	addr := mustGetUnreachableAddr(t)

	server := &LocalQrouterServer{
		mgr: metaMock.NewMockEntityMgr(ctrl),
	}

	_, err := server.AddDataShard(context.Background(), &protos.AddShardRequest{
		Shard: &protos.Shard{
			Id:    "sh-bad",
			Hosts: []string{addr},
		},
	})

	assert.ErrorContains(t, err, "not reachable")
}

func TestAddDataShardRejectsNilShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	server := &LocalQrouterServer{
		mgr: metaMock.NewMockEntityMgr(ctrl),
	}

	_, err := server.AddDataShard(context.Background(), &protos.AddShardRequest{})
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
