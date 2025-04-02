package provider_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/coordinator/mock"
	shardsProvider "github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/emptypb"
)

var someShards = []*topology.DataShard{
	{
		ID: "id-first",
		Cfg: &config.Shard{
			RawHosts: []string{
				"aboba:1337:kapusta",
				"eshkere:228",
			},
		},
	},
	{
		ID: "id-second",
		Cfg: &config.Shard{
			RawHosts: []string{
				"goooal:1488:laooog",
			},
		},
	},
}

var someProtoShards = []proto.ListShardsReply{
	{
		Shards: []*proto.Shard{
			{
				Id:    "id-first",
				Hosts: []string{"aboba:1337", "eshkere:228"},
			},
			{
				Id:    "id-second",
				Hosts: []string{"goooal:1488"},
			},
		},
	},
}

func TestListShards(t *testing.T) {
	assertions := assert.New(t)

	ctrl := gomock.NewController(t)
	coordinator := mock.NewMockCoordinator(ctrl)
	shardServer := shardsProvider.NewShardServer(coordinator)
	ctx := context.Background()
	emptypb := &emptypb.Empty{}

	coordinator.EXPECT().ListShards(ctx).Return(someShards, nil)

	res, err := shardServer.ListShards(ctx, emptypb)

	assertions.NoError(err)
	assertions.Equal(someProtoShards, res)
}

func TestListShards_ReturnsCoordinatorError(t *testing.T) {
	assertions := assert.New(t)

	ctrl := gomock.NewController(t)
	coordinator := mock.NewMockCoordinator(ctrl)
	shardServer := shardsProvider.NewShardServer(coordinator)
	ctx := context.Background()
	emptypb := &emptypb.Empty{}

	coordinator.EXPECT().ListShards(ctx).Return(someShards, assert.AnError)

	res, err := shardServer.ListShards(ctx, emptypb)

	assertions.ErrorIs(err, assert.AnError)
	assertions.Zero(res)
}
