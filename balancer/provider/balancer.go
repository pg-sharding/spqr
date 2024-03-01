package provider

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
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
	tasks, err := b.generateTasks(ctx)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error planning tasks")
	}
	if len(tasks) == 0 {
		return
	}
	if err := b.executeTasks(ctx); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error executing tasks")
	}
}

func (b *BalancerImpl) generateTasks(ctx context.Context) ([]*Task, error) {
	shardsServiceClient := protos.NewShardServiceClient(b.coordinatorConn)
	r, err := shardsServiceClient.ListShards(ctx, &protos.ListShardsRequest{})
	if err != nil {
		return nil, err
	}
	shardToState := make(map[string]*ShardMetrics)
	shardStates := make([]*ShardMetrics, 0)
	for _, shard := range r.Shards {
		state, err := b.getShardCurrentState(ctx, shard)
		if err != nil {
			return nil, err
		}
		shardToState[shard.Id] = state
		shardStates = append(shardStates, state)
	}

	maxMetric, criterion := b.getCriterion(shardStates)
	sort.Slice(shardStates, func(i, j int) bool {
		return shardStates[i].MetricsTotal[criterion] > shardStates[j].MetricsTotal[criterion]
	})

	spqrlog.Zero.Debug().Float64("metric", maxMetric).Int("criterion", criterion).Msg("Max metric")

	if maxMetric <= 1 {
		spqrlog.Zero.Debug().Msg("Metrics below the threshold, exiting")
		return []*Task{}, nil
	}

	if err = b.updateKeyRanges(ctx); err != nil {
		return nil, err
	}

	if err = b.getStatsByKeyRange(ctx, shardStates); err != nil {
		return nil, err
	}

	// determine most loaded key range
	shardFrom := shardStates[0]

	kRLoad, krId := b.getMostLoadedKR(shardFrom, criterion)

	keyCount := int(((shardFrom.MetricsTotal[criterion] - b.threshold[criterion]) * float64(shardFrom.KeyCountKR[krId])) / kRLoad)

	// determine where to move keys to
	shId, err := b.getShardToMoveTo(shardStates, shardToState, krId, shardFrom.ShardId, keyCount)

	if err != nil {
		shId, keyCount = b.moveMaxPossible(shardStates, shardToState, krId, shardFrom.ShardId)
	}

	return b.getTasks(krId, shId, keyCount)
}

func (b *BalancerImpl) getShardCurrentState(ctx context.Context, shard *protos.Shard) (*ShardMetrics, error) {
	hosts := shard.Hosts
	res := NewShardMetrics()
	replicaMetrics := NewHostMetrics()
	for _, host := range hosts {
		hostsMetrics, isMaster, err := b.getHostStatus(ctx, host)
		if err != nil {
			return nil, err
		}
		if isMaster {
			res.SetMasterMetrics(hostsMetrics)
			res.Master = host
			continue
		}
		replicaThreshold := b.threshold[metricsCount:]
		if replicaMetrics.MaxRelative(replicaThreshold) < hostsMetrics.MaxRelative(replicaThreshold) {
			replicaMetrics = hostsMetrics
			res.TargetReplica = host
		}
	}
	res.SetReplicaMetrics(replicaMetrics)
	return res, nil
}

func (b *BalancerImpl) getHostStatus(ctx context.Context, dsn string) (metrics HostMetrics, isMaster bool, err error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, false, err
	}
	metrics = NewHostMetrics()

	row := conn.QueryRow(ctx, "SELECT pg_is_in_recovery() as is_replica;")
	res := ""
	if err = row.Scan(&res); err != nil {
		return nil, false, err
	}
	isMaster = res == "f"

	query := fmt.Sprintf(`
	SELECT coalesce(SUM((user_time + system_time)), 0) AS cpu_total
	FROM pgcs_get_stats_time_interval(now() - interval %ds, now())
`, config.BalancerConfig().StatIntervalSec)
	row = conn.QueryRow(ctx, query)
	if err = row.Scan(&metrics[cpuMetric]); err != nil {
		return nil, isMaster, err
	}

	query = `SELECT SUM(pg_database_size(datname)) as total_size 
			 FROM pg_database 
				WHERE datname != 'template0' 
				  AND datname != 'template1' 
				  AND datname != 'postgres';`
	row = conn.QueryRow(ctx, query)
	if err = row.Scan(&metrics[spaceMetric]); err != nil {
		return nil, isMaster, err
	}

	return
}

// getStatsByKeyRange gets statistics by key range & updates ShardMetrics
func (b *BalancerImpl) getStatsByKeyRange(ctx context.Context, shards []*ShardMetrics) error {
	for _, shard := range shards {
		conn, err := pgx.Connect(ctx, shard.Master)
		if err != nil {
			return err
		}
		query := `
		SELECT
		    comment_keys->>'key_range_id' AS key_range_id,
			SUM(user_time + system_time) AS cpu
		FROM pgcs_get_stats_time_interval(now() - interval %ds, now())
		GROUP BY key_range_id;
`
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return err
		}
		for rows.Next() {
			krId := ""
			cpu := 0.0
			if err = rows.Scan(&krId, &cpu); err != nil {
				return err
			}
			shard.MetricsKR[krId][cpuMetric] = cpu
		}
	}
}

func (b *BalancerImpl) getKeyRange(val []byte) string {
	l := 0
	r := len(b.keyRanges) + 1

	for r > l+1 {
		m := (l + r) / 2
		if kr.CmpRangesLess(b.keyRanges[m].LowerBound, val) {
			l = m
		} else {
			r = m
		}
	}

	return b.keyRanges[l].ID
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
func (b *BalancerImpl) fitsOnShard(krMetrics []float64, keyCount int, shard *ShardMetrics) bool {
	for kind, metric := range shard.MetricsTotal {
		loadExpectation := krMetrics[kind]*float64(keyCount) + metric
		if b.threshold[kind] > loadExpectation {
			return false
		}
	}
	return true
}

// fitsOnShard
// TODO unit tests
func (b *BalancerImpl) maxFitOnShard(krMetrics []float64, shard *ShardMetrics) (maxCount int) {
	maxCount = -1
	for kind, metric := range shard.MetricsTotal {
		// TODO move const to config
		count := int(0.8 * ((b.threshold[kind] - metric) / krMetrics[kind]))
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
		for metricType, metric := range state.MetricsTotal {
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
	for krg := range shard.MetricsKR {
		metric := shard.MetricsKR[krg][kind]
		count := shard.KeyCountKR[krg]
		totalKRMetric := metric * float64(count)
		if totalKRMetric > value {
			value = totalKRMetric
			krId = krg
		}
	}
	return
}

func (b *BalancerImpl) getTasks(krId string, shardToId string, keyCount int) ([]*Task, error) {
	// TODO implement
	panic("implement me")
}

func (b *BalancerImpl) insertTasksToQDB(tasks []*Task) error {
	// TODO implement
	panic("implement me")
}

func (b *BalancerImpl) executeTasks(ctx context.Context) error {
	// TODO implement
	panic("implement me")
}

func (b *BalancerImpl) updateKeyRanges(ctx context.Context) error {
	keyRangeService := protos.NewKeyRangeServiceClient(b.coordinatorConn)
	keyRangesProto, err := keyRangeService.ListAllKeyRanges(ctx, &protos.ListAllKeyRangesRequest{})
	if err != nil {
		return err
	}
	keyRanges := make([]*kr.KeyRange, len(keyRangesProto.KeyRangesInfo))
	for i, krg := range keyRangesProto.KeyRangesInfo {
		keyRanges[i] = kr.KeyRangeFromProto(krg)
	}
	sort.Slice(keyRanges, func(i, j int) bool {
		return kr.CmpRangesLess(keyRanges[i].LowerBound, keyRanges[j].LowerBound)
	})
	b.keyRanges = keyRanges
	b.krIdx = make(map[string]int)
	for i, krg := range b.keyRanges {
		b.krIdx[krg.ID] = i
	}

	return nil
}
