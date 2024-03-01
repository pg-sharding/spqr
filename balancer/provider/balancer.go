package provider

import (
	"context"
	"fmt"
	"github.com/pg-sharding/spqr/balancer"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sort"
)

type BalancerImpl struct {
	coordinatorConn *grpc.ClientConn
	threshold       []float64

	keyRanges []*kr.KeyRange
	krIdx     map[string]int
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
	// TODO check for unfinished tasks before planning new
	if err := b.generateTasks(ctx); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error planning tasks")
	}
	if err := b.executeTasks(ctx); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error executing tasks")
	}
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

	// TODO update b.keyRanges & b.krIdx here

	if err := b.getStatsByKeyRange(shardStates); err != nil {
		return err
	}

	// determine most loaded key range
	shardFrom := shardStates[0]

	kRLoad, krId := b.getMostLoadedKR(shardFrom, criterion)

	keyCount := int(((shardFrom.MetricsTotal.metrics[criterion] - b.threshold[criterion]) * float64(shardFrom.KeyCountKR[krId])) / kRLoad)

	// determine where to move keys to
	shId, err := b.getShardToMoveTo(shardStates, shardToState, krId, shardFrom.ShardId, keyCount)

	if err != nil {
		shId, keyCount = b.moveMaxPossible(shardStates, shardToState, krId, shardFrom.ShardId)
	}

	tasks, err := b.getTasks(krId, shId, keyCount)
	if err != nil {
		return err
	}
	return b.planTasks(tasks)
}

func (b *BalancerImpl) getShardCurrentState(shard *protos.Shard) (*ShardMetrics, error) {
	// TODO implement
	panic("not implemented")
}

// getStatsByKeyRange gets statistics by key range & updates ShardMetrics
func (b *BalancerImpl) getStatsByKeyRange(shards []*ShardMetrics) error {
	// TODO implement
	panic("implement me")
}

// getShardToMoveTo determines where to send keys from specified key range
// TODO unit tests
func (b *BalancerImpl) getShardToMoveTo(shardMetrics []*ShardMetrics, shardIdToMetrics map[string]*ShardMetrics, krId string, krShardId string, keyCount int) (string, error) {
	// try fitting on shards with adjacent key ranges
	adjShards := b.getAdjacentShards(krId)
	for _, adjShard := range adjShards {
		if b.fitsOnShard(shardIdToMetrics[krShardId].MetricsKR[krId], keyCount, shardIdToMetrics[adjShard]) {
			return adjShard, nil
		}
	}
	// try fitting on other shards ordered by criterion load ascending
	for i := len(shardMetrics) - 1; i >= 0; i-- {
		if b.fitsOnShard(shardIdToMetrics[krShardId].MetricsKR[krId], keyCount, shardMetrics[i]) {
			return shardMetrics[i].ShardId, nil
		}
	}
	return "", fmt.Errorf("could not get shard to move keys to")
}

// moveMaxPossible determines where most keys can be sent
// TODO unit tests
func (b *BalancerImpl) moveMaxPossible(shardMetrics []*ShardMetrics, shardIdToMetrics map[string]*ShardMetrics, krId string, krShardId string) (shardId string, maxKeyCount int) {
	maxKeyCount = -1
	for i := len(shardMetrics) - 1; i >= 0; i-- {
		keyCount := b.maxFitOnShard(shardIdToMetrics[krShardId].MetricsKR[krId], shardMetrics[i])
		if keyCount > maxKeyCount {
			maxKeyCount = keyCount
			shardId = shardMetrics[i].ShardId
		}
	}
	return
}

// fitsOnShard
// TODO unit tests
func (b *BalancerImpl) fitsOnShard(krMetrics *Metrics, keyCount int, shard *ShardMetrics) bool {
	for kind, metric := range shard.MetricsTotal.metrics {
		loadExpectation := krMetrics.metrics[kind]*float64(keyCount) + metric
		if b.threshold[kind] > loadExpectation {
			return false
		}
	}
	return true
}

// fitsOnShard
// TODO unit tests
func (b *BalancerImpl) maxFitOnShard(krMetrics *Metrics, shard *ShardMetrics) (maxCount int) {
	maxCount = -1
	for kind, metric := range shard.MetricsTotal.metrics {
		// TODO move const to config
		count := int(0.8 * ((b.threshold[kind] - metric) / krMetrics.metrics[kind]))
		if count > maxCount {
			maxCount = count
		}
	}
	return
}

func (b *BalancerImpl) getAdjacentShards(krId string) []string {
	res := make([]string, 0)
	krIdx := b.krIdx[krId]
	if krIdx != 0 {
		res = append(res, b.keyRanges[krIdx-1].ShardID)
	}
	if krIdx < len(b.keyRanges)-1 && (len(res) == 0 || b.keyRanges[krIdx+1].ShardID != res[0]) {
		res = append(res, b.keyRanges[krIdx+1].ShardID)
	}
	return res
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

func (b *BalancerImpl) getMostLoadedKR(shard *ShardMetrics, kind int) (value float64, krId string) {
	value = -1
	for kr := range shard.MetricsKR {
		metric := shard.MetricsKR[kr].metrics[kind]
		count := shard.KeyCountKR[kr]
		totalKRMetric := metric * float64(count)
		if totalKRMetric > value {
			value = totalKRMetric
			krId = kr
		}
	}
	return
}

func (b *BalancerImpl) getTasks(krId string, shardToId string, keyCount int) ([]*Task, error) {
	// TODO implement
	panic("implement me")
}

// planTasks adds tasks to QDB
func (b *BalancerImpl) planTasks(tasks []*Task) error {
	// TODO implement
	panic("implement me")
}

func (b *BalancerImpl) executeTasks(ctx context.Context) error {
	// TODO implement
	panic("implement me")
}
