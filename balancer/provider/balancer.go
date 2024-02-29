package provider

import (
	"context"
	"github.com/pg-sharding/spqr/balancer"
	"github.com/pg-sharding/spqr/pkg/config"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sort"
)

const metricsCount = 2

type BalancerImpl struct {
	coordinatorConn *grpc.ClientConn
	thresholds      []float64
}

type action struct {
}

func NewBalancer() (*BalancerImpl, error) {
	conn, err := grpc.Dial(config.BalancerConfig().CoordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &BalancerImpl{coordinatorConn: conn}, nil
}

var _ balancer.Balancer = &BalancerImpl{}

func (b *BalancerImpl) RunBalancer(ctx context.Context) {
	// TODO implement
	panic("not implemented")
}

func (b *BalancerImpl) runIteration(ctx context.Context) error {
	shardsServiceClient := protos.NewShardServiceClient(b.coordinatorConn)
	r, err := shardsServiceClient.ListShards(ctx, &protos.ListShardsRequest{})
	if err != nil {
		return err
	}
	shardToState := make(map[string]*shardState)
	shardStates := make([]*shardState, 0)
	for _, shard := range r.Shards {
		state, err := b.getShardCurrentState(shard)
		if err != nil {
			return err
		}
		shardToState[shard.Id] = state
		shardStates = append(shardStates, state)
	}

	maxMetric, criterion := b.getCriterion(shardStates)
	sort.Slice(shardStates, func(i, j int) bool {
		return shardStates[i].metrics[criterion] > shardStates[j].metrics[criterion]
	})

	spqrlog.Zero.Debug().Float64("metric", maxMetric).Int("criterion", criterion).Msg("Max metric")

	if maxMetric <= 1 {
		spqrlog.Zero.Debug().Msg("Metrics below the threshold, exiting")
		return nil
	}

	shardFrom := shardStates[0]
	shardFromTopKeys := b.getTopKeysByCriterion(shardFrom, criterion)

}

type shardState struct {
	shardId string
	metrics []float64
}

func (b *BalancerImpl) getShardCurrentState(shard *protos.Shard) (*shardState, error) {
	// TODO implement
	panic("not implemented")
}

func (b *BalancerImpl) getCriterion(shards []*shardState) (value float64, t int) {
	value = -1
	t = -1
	for _, state := range shards {
		for metricType, metric := range state.metrics {
			v := metric / b.thresholds[metricType%metricsCount]
			if v > value {
				value = v
				t = metricType
			}
		}
	}
	return value, t
}

func (b *BalancerImpl) getTopKeysByCriterion(shard *shardState, criterion int) map[string]float64 {
	// TODO implement
	panic("not implemented")
}
