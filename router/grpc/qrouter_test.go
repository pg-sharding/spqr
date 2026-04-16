package grpc

import (
	"context"
	"testing"

	metaMock "github.com/pg-sharding/spqr/pkg/mock/meta"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAddDataShardPassesRequestToManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := metaMock.NewMockEntityMgr(ctrl)
	mgr.EXPECT().AddDataShard(gomock.Any(), gomock.Any()).Times(1).Return(nil)

	server := &LocalQrouterServer{
		mgr: mgr,
	}

	_, err := server.AddDataShard(context.Background(), &protos.AddShardRequest{
		Shard: &protos.Shard{
			Id: "sh-bad",
			Options: []*protos.GenericOption{
				{Name: "host", Value: "127.0.0.1"},
			},
		},
	})

	assert.NoError(t, err)
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
