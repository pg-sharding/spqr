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

type BalancerImpl struct {
	coordinatorConn *grpc.ClientConn
	threshold       []float64
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

func (b *BalancerImpl) generateTasks(ctx context.Context) error {
	shardsServiceClient := protos.NewShardServiceClient(b.coordinatorConn)
	r, err := shardsServiceClient.ListShards(ctx, &protos.ListShardsRequest{})
	if err != nil {
		return err
	}
	shardToState := make(map[string]*ShardMetrics)
	shardStates := make([]*ShardMetrics, 0)
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
		return shardStates[i].MetricsTotal.metrics[criterion] > shardStates[j].MetricsTotal.metrics[criterion]
	})

	spqrlog.Zero.Debug().Float64("metric", maxMetric).Int("criterion", criterion).Msg("Max metric")

	if maxMetric <= 1 {
		spqrlog.Zero.Debug().Msg("Metrics below the threshold, exiting")
		return nil
	}

	return nil
}

func (b *BalancerImpl) getShardCurrentState(shard *protos.Shard) (*ShardMetrics, error) {
	// TODO implement
	panic("not implemented")
}

// getStatsByKeyRange gets detailed statistics by key range & updates ShardMetrics
func (b *BalancerImpl) getStatsByKeyRange(shards []*ShardMetrics) error {
	// TODO implement
	panic("implement me")
}

func (b *BalancerImpl) getCriterion(shards []*ShardMetrics) (value float64, kind int) {
	value = -1
	kind = -1
	for _, state := range shards {
		for metricType, metric := range state.MetricsTotal.metrics {
			v := metric / b.threshold[metricType%metricsCount]
			if v > value {
				value = v
				kind = metricType
			}
		}
	}
	return
}
