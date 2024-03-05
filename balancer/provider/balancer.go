package provider

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/pg-sharding/spqr/balancer"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sort"
	"strings"
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
	taskGroup, err := b.getCurrentTaskGroupFromQDB()
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error getting current tasks")
		return
	}
	if taskGroup == nil {
		taskGroup, err = b.generateTasks(ctx)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error planning tasks")
			return
		}
		if len(taskGroup.tasks) == 0 {
			return
		}
		if err := b.syncTaskGroupWithQDB(taskGroup); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error inserting tasks")
			return
		}
	}
	if err := b.executeTasks(ctx, taskGroup); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error executing tasks")
	}
}

func (b *BalancerImpl) generateTasks(ctx context.Context) (*TaskGroup, error) {
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
		return &TaskGroup{}, nil
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

	return b.getTasks(ctx, shardFrom, krId, shId, keyCount)
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
		for _, params := range []struct {
			Host            string
			MetricsStartInd int
		}{
			{Host: shard.Master, MetricsStartInd: 0},
			{Host: shard.TargetReplica, MetricsStartInd: metricsCount},
		} {
			conn, err := pgx.Connect(ctx, params.Host)
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
				if _, ok := shard.MetricsKR[krId]; !ok {
					shard.MetricsKR[krId] = make([]float64, 2*metricsCount)
				}
				shard.MetricsKR[krId][params.MetricsStartInd+cpuMetric] = cpu
			}

			for i, krg := range b.keyRanges {
				if krg.ShardID != shard.ShardId {
					continue
				}
				rels, err := b.getKRRelations(ctx, krg)
				if err != nil {
					return err
				}

				for _, rel := range rels {
					// TODO check units in other queries (mB/KB possible)
					queryRaw := `
				SELECT sum(pg_column_size(t.*)) as filesize, count(*) as filerow 
				FROM %s as t
				WHERE %s;
`
					condition, err := b.getKRCondition(rel, krg, b.keyRanges[i+1], "t")
					if err != nil {
						return err
					}
					query = fmt.Sprintf(queryRaw, rel.Name, condition)

					row := conn.QueryRow(ctx, query)
					var size, count int64
					if err := row.Scan(&size, &count); err != nil {
						return err
					}
					if _, ok := shard.MetricsKR[krg.ID]; !ok {
						shard.MetricsKR[krg.ID] = make([]float64, 2*metricsCount)
					}
					shard.MetricsKR[krg.ID][params.MetricsStartInd+spaceMetric] += float64(size)
					// TODO remove duplicate count on master & replica
					shard.KeyCountKR[krg.ID] += count
					if _, ok := shard.KeyCountRelKR[krg.ID]; !ok {
						shard.KeyCountRelKR[krg.ID] = make(map[string]int64)
					}
					shard.KeyCountRelKR[krg.ID][rel.Name] = count
				}
			}
		}
	}
	return nil
}

func (b *BalancerImpl) getKRRelations(ctx context.Context, kRange *kr.KeyRange) ([]*distributions.DistributedRelation, error) {
	distributionService := protos.NewDistributionServiceClient(b.coordinatorConn)
	res, err := distributionService.GetDistribution(ctx, &protos.GetDistributionRequest{Id: kRange.Distribution})
	if err != nil {
		return nil, err
	}
	rels := make([]*distributions.DistributedRelation, len(res.Distribution.Relations))
	for i, relProto := range res.Distribution.Relations {
		rels[i] = distributions.DistributedRelationFromProto(relProto)
	}
	return rels, nil
}

// getKRCondition returns SQL condition for elements of distributed relation between two key ranges
// TODO support multidimensional key ranges
func (b *BalancerImpl) getKRCondition(rel *distributions.DistributedRelation, kRange *kr.KeyRange, nextKR *kr.KeyRange, prefix string) (string, error) {
	buf := make([]string, len(rel.DistributionKey))
	for i, entry := range rel.DistributionKey {
		// TODO remove after multidimensional key range support
		if i > 0 {
			break
		}
		f, err := hashfunction.HashFunctionByName(entry.HashFunction)
		if err != nil {
			return "", err
		}
		hashedCol := ""
		if prefix != "" {
			hashedCol = fmt.Sprintf("%s.%s(%s)", prefix, hashfunction.ToString(f), entry.Column)
		} else {
			hashedCol = fmt.Sprintf("%s(%s)", hashfunction.ToString(f), entry.Column)
		}
		buf[i] = fmt.Sprintf("%s >= %s AND %s < %s", hashedCol, string(kRange.LowerBound), hashedCol, string(nextKR.LowerBound))
	}
	return strings.Join(buf, " AND "), nil
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

func (b *BalancerImpl) getTasks(ctx context.Context, shardFrom *ShardMetrics, krId string, shardToId string, keyCount int) (*TaskGroup, error) {
	// Move from beginning or the end of key range

	krInd := b.krIdx[krId]
	beginning := b.keyRanges[krInd+1].ShardID == shardToId
	krIdTo := ""
	var unification unificationType = unificationNone
	if b.keyRanges[krInd+1].ShardID == shardToId {
		krIdTo = b.keyRanges[krInd+1].ID
		unification = unificationRight
	} else if b.keyRanges[krInd-1].ShardID == shardToId {
		krIdTo = b.keyRanges[krInd-1].ID
		unification = unificationLeft
	}

	conn, err := pgx.Connect(ctx, shardFrom.TargetReplica)
	if err != nil {
		return nil, err
	}

	var maxCount int64 = -1
	relName := ""
	for r, count := range shardFrom.KeyCountRelKR[krId] {
		if count > maxCount {
			relName = r
			maxCount = count
		}
	}
	if _, ok := b.krIdx[krId]; !ok {
		return nil, fmt.Errorf("unknown key range id \"%s\"", krId)
	}
	var rel *distributions.DistributedRelation = nil
	allRels, err := b.getKRRelations(ctx, b.keyRanges[b.krIdx[krId]])
	for _, r := range allRels {
		if r.Name == relName {
			rel = r
			break
		}
	}
	if rel == nil {
		return nil, fmt.Errorf("relation \"%s\" not found", relName)
	}

	moveCount := min(keyCount/config.BalancerConfig().KeysPerMove, config.BalancerConfig().MaxMoveCount)
	counts := make([]int, moveCount)
	for i := 0; i < len(counts)-1; i++ {
		counts[i] = config.BalancerConfig().KeysPerMove
	}
	counts[len(counts)-1] = min(keyCount-(moveCount-1)*config.BalancerConfig().KeysPerMove, config.BalancerConfig().KeysPerMove)
	tasks := make([]*Task, moveCount)
	cumCount := 0
	// TODO multidimensional key ranges
	for i, count := range counts {
		query := fmt.Sprintf(`
		SELECT "%s" as idx
		FROM "%s"
		ORDER BY idx %s
		LIMIT 1
		OFFSET %d
		`, rel.DistributionKey[0].Column, rel.Name, func() string {
			if !beginning {
				return "DESC"
			}
			return ""
		}(), cumCount+count-1)
		row := conn.QueryRow(ctx, query)
		// TODO typed key ranges
		var idx string
		if err := row.Scan(&idx); err != nil {
			return nil, err
		}
		tasks[i] = &Task{
			shardFromId: shardFrom.ShardId,
			shardToId:   shardToId,
			krIdFrom:    krId,
			krIdTo:      krIdTo,
			bound:       []byte(idx),
		}
	}

	return &TaskGroup{tasks: tasks, unification: unification}, nil
}

func (b *BalancerImpl) getCurrentTaskGroupFromQDB() (group *TaskGroup, err error) {
	// TODO implement
	panic("implement me")
}

func (b *BalancerImpl) syncTaskGroupWithQDB(group *TaskGroup) error {
	// TODO implement
	panic("implement me")
}

func (b *BalancerImpl) removeTaskGroupFromQDB() error {
	// TODO implement
	panic("implement me")
}

func (b *BalancerImpl) executeTasks(ctx context.Context, group *TaskGroup) error {

	keyRangeService := protos.NewKeyRangeServiceClient(b.coordinatorConn)

	var buf bytes.Buffer
	e := gob.NewEncoder(&buf)
	if err := e.Encode(group); err != nil {
		return err
	}
	tasksHash := buf.String()

	for len(group.tasks) > 0 {
		task := group.tasks[0]
		switch task.state {
		case taskPlanned:
			newKeyRange := fmt.Sprintf("kr_%s", tasksHash)

			if _, err := keyRangeService.SplitKeyRange(ctx, &protos.SplitKeyRangeRequest{
				NewId:    newKeyRange,
				SourceId: task.krIdFrom,
				Bound:    task.bound,
			}); err != nil {
				return err
			}

			task.tempKRId = newKeyRange
			task.state = taskSplit
			if err := b.syncTaskGroupWithQDB(group); err != nil {
				// TODO mb retry?
				return err
			}
			continue
		case taskSplit:
			// TODO account for unification type
			if _, err := keyRangeService.MoveKeyRange(ctx, &protos.MoveKeyRangeRequest{
				Id:        task.tempKRId,
				ToShardId: task.shardToId,
			}); err != nil {
				return err
			}
			task.state = taskMoved
			if err := b.syncTaskGroupWithQDB(group); err != nil {
				// TODO mb retry?
				return err
			}
			continue
		case taskMoved:
			if group.unification != unificationNone {
				if _, err := keyRangeService.MergeKeyRange(ctx, &protos.MergeKeyRangeRequest{
					BaseId:      task.krIdTo,
					AppendageId: task.tempKRId,
				}); err != nil {
					return err
				}
			}
			group.tasks = group.tasks[1:]
			if err := b.syncTaskGroupWithQDB(group); err != nil {
				// TODO mb retry?
				return err
			}
			continue
		default:
			return fmt.Errorf("unknown task state %d", task.state)
		}
	}

	// TODO mb retry?
	return b.removeTaskGroupFromQDB()
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
