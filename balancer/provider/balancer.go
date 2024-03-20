package provider

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pg-sharding/spqr/balancer"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
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
	shardKr   map[string][]int
}

func NewBalancer() (*BalancerImpl, error) {
	threshold := make([]float64, 2*metricsCount)
	configThresholds := []float64{config.BalancerConfig().CpuThreshold, config.BalancerConfig().SpaceThreshold}
	for i := 0; i < metricsCount; i++ {
		threshold[i] = configThresholds[i]
		threshold[metricsCount+i] = configThresholds[i]
	}
	conn, err := grpc.Dial(config.BalancerConfig().CoordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &BalancerImpl{
		coordinatorConn: conn,
		threshold:       threshold,
		keyRanges:       []*kr.KeyRange{},
		krIdx:           map[string]int{},
		shardKr:         map[string][]int{},
	}, nil
}

var _ balancer.Balancer = &BalancerImpl{}

func (b *BalancerImpl) RunBalancer(ctx context.Context) {
	// TODO: add command to drop task group to coordinator
	taskGroup, err := b.getCurrentTaskGroupFromQDB(ctx)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error getting current tasks")
		return
	}
	if taskGroup == nil || len(taskGroup.Tasks) == 0 {
		taskGroup, err = b.generateTasks(ctx)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error planning tasks")
			return
		}
		if len(taskGroup.Tasks) == 0 {
			spqrlog.Zero.Debug().Msg("Nothing to execute")
			return
		}
		if err := b.syncTaskGroupWithQDB(ctx, taskGroup); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error inserting tasks")
			return
		}
	}
	if err := b.executeTasks(ctx, taskGroup); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error executing tasks")
	}
}

func (b *BalancerImpl) generateTasks(ctx context.Context) (*tasks.TaskGroup, error) {
	shardsServiceClient := protos.NewShardServiceClient(b.coordinatorConn)
	r, err := shardsServiceClient.ListShards(ctx, &protos.ListShardsRequest{})
	if err != nil {
		return nil, err
	}
	shardToState := make(map[string]*ShardMetrics)
	shardStates := make([]*ShardMetrics, 0)
	spqrlog.Zero.Debug().Int("shards count", len(r.Shards)).Msg("got shards from coordinator")
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
		return &tasks.TaskGroup{}, nil
	}

	if err = b.updateKeyRanges(ctx); err != nil {
		return nil, fmt.Errorf("error updating key range info: %s", err)
	}

	if err = b.getStatsByKeyRange(ctx, shardStates[0]); err != nil {
		return nil, fmt.Errorf("error getting detailed stats: %s", err)
	}

	// determine most loaded key range
	shardFrom := shardStates[0]

	kRLoad, krId := b.getMostLoadedKR(shardFrom, criterion)

	meanKeyLoad := kRLoad / float64(shardFrom.KeyCountKR[krId])
	keyCount := int((shardFrom.MetricsTotal[criterion] - b.threshold[criterion]) / meanKeyLoad)
	// do not move more keys than there are in the key range
	keyCount = min(keyCount, int(shardFrom.KeyCountKR[krId]))

	// determine where to move keys to
	shId, ok := b.getShardToMoveTo(shardStates, shardToState, krId, shardFrom.ShardId, keyCount)

	if !ok {
		shId, keyCount = b.moveMaxPossible(shardStates, shardToState, krId, shardFrom.ShardId)
		if keyCount < 0 {
			return nil, fmt.Errorf("could not find shard to move keys to")
		}
	}

	if keyCount == 0 {
		return &tasks.TaskGroup{Tasks: []*tasks.Task{}}, nil
	}
	return b.getTasks(ctx, shardFrom, krId, shId, keyCount)
}

func (b *BalancerImpl) getShardCurrentState(ctx context.Context, shard *protos.Shard) (*ShardMetrics, error) {
	spqrlog.Zero.Debug().Str("shard id", shard.Id).Msg("getting shard state")
	hosts := shard.Hosts
	res := NewShardMetrics()
	res.ShardId = shard.Id
	replicaMetrics := NewHostMetrics()
	for _, host := range hosts {
		hostsMetrics, isMaster, err := b.getHostStatus(ctx, host)
		if err != nil {
			return nil, err
		}
		if hostsMetrics == nil {
			continue
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
	spqrlog.Zero.Debug().Str("host", dsn).Msg("getting host state")
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, false, nil
	}
	metrics = NewHostMetrics()

	row := conn.QueryRow(ctx, "SELECT NOT pg_is_in_recovery() as is_master;")
	if err = row.Scan(&isMaster); err != nil {
		return nil, false, err
	}

	query := fmt.Sprintf(`
	SELECT coalesce(SUM((user_time + system_time)), 0) AS cpu_total
	FROM pgcs_get_stats_time_interval(now() - interval '%ds', now())
`, config.BalancerConfig().StatIntervalSec)
	spqrlog.Zero.Debug().Str("query", query).Msg("Getting cpu stats")
	row = conn.QueryRow(ctx, query)
	if err = row.Scan(&metrics[cpuMetric]); err != nil {
		return nil, isMaster, err
	}

	query = `SELECT SUM(pg_database_size(datname)) as total_size 
			 FROM pg_database 
				WHERE datname != 'template0' 
				  AND datname != 'template1' 
				  AND datname != 'postgres';`
	spqrlog.Zero.Debug().Str("query", query).Msg("Getting space stats")
	row = conn.QueryRow(ctx, query)
	if err = row.Scan(&metrics[spaceMetric]); err != nil {
		return nil, isMaster, err
	}

	spqrlog.Zero.Debug().
		Float64("cpu-metric", metrics[cpuMetric]).
		Float64("space-metric", metrics[spaceMetric]).
		Bool("is master", isMaster).
		Msg("got host state")
	return
}

// getStatsByKeyRange gets statistics by key range & updates ShardMetrics
func (b *BalancerImpl) getStatsByKeyRange(ctx context.Context, shard *ShardMetrics) error {
	spqrlog.Zero.Debug().Str("shard", shard.ShardId).Msg("getting shard detailed state")

	type paramsStruct struct {
		Host            string
		MetricsStartInd int
	}
	paramsList := []paramsStruct{
		{Host: shard.Master, MetricsStartInd: 0},
	}
	if shard.TargetReplica != "" {
		paramsList = append(paramsList, paramsStruct{Host: shard.TargetReplica, MetricsStartInd: metricsCount})
	}
	for _, params := range paramsList {
		spqrlog.Zero.Debug().Str("host", params.Host).Msg("getting host detailed state")
		conn, err := pgx.Connect(ctx, params.Host)
		if err != nil {
			return err
		}
		query := fmt.Sprintf(`
		SELECT
		    comment_keys->>'key_range_id' AS key_range_id,
			SUM(user_time + system_time) AS cpu
		FROM (
		    SELECT * 
		    FROM pgcs_get_stats_time_interval(now() - interval '%ds', now())
		    WHERE comment_keys->>'key_range_id' IS NOT NULL        
		) as pg_comment_stats
		GROUP BY key_range_id;
`, config.BalancerConfig().StatIntervalSec)
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
			if _, ok := b.krIdx[krId]; !ok {
				continue
			}
			if _, ok := shard.MetricsKR[krId]; !ok {
				shard.MetricsKR[krId] = make([]float64, 2*metricsCount)
			}
			shard.MetricsKR[krId][params.MetricsStartInd+cpuMetric] = cpu
		}
	}

	conn, err := pgx.Connect(ctx, shard.Master)
	if err != nil {
		return err
	}

	for _, i := range b.shardKr[shard.ShardId] {
		krg := b.keyRanges[i]
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
			var nextKR *kr.KeyRange
			if i < len(b.keyRanges)-1 {
				nextKR = b.keyRanges[i+1]
			}
			condition, err := b.getKRCondition(rel, krg, nextKR, "t")
			if err != nil {
				return err
			}
			query := fmt.Sprintf(queryRaw, rel.Name, condition)
			spqrlog.Zero.Debug().Str("query", query).Msg("getting space usage & key count")

			row := conn.QueryRow(ctx, query)
			var size, count int64
			if err := row.Scan(&size, &count); err != nil {
				return err
			}
			if _, ok := shard.MetricsKR[krg.ID]; !ok {
				shard.MetricsKR[krg.ID] = make([]float64, 2*metricsCount)
			}
			shard.MetricsKR[krg.ID][spaceMetric] += float64(size)
			shard.KeyCountKR[krg.ID] += count
			if _, ok := shard.KeyCountRelKR[krg.ID]; !ok {
				shard.KeyCountRelKR[krg.ID] = make(map[string]int64)
			}
			shard.KeyCountRelKR[krg.ID][rel.Name] = count
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
		// TODO add hash (depends on col type)
		hashedCol := ""
		if prefix != "" {
			hashedCol = fmt.Sprintf("%s.%s", prefix, entry.Column)
		} else {
			hashedCol = entry.Column
		}
		if nextKR != nil {
			buf[i] = fmt.Sprintf("%s >= %s AND %s < %s", hashedCol, string(kRange.LowerBound), hashedCol, string(nextKR.LowerBound))
		} else {
			buf[i] = fmt.Sprintf("%s >= %s", hashedCol, string(kRange.LowerBound))
		}
	}
	return strings.Join(buf, " AND "), nil
}

// getShardToMoveTo determines where to send keys from specified key range
// TODO unit tests
func (b *BalancerImpl) getShardToMoveTo(shardMetrics []*ShardMetrics, shardIdToMetrics map[string]*ShardMetrics, krId string, krShardId string, keyCountToMove int) (string, bool) {
	krKeyCount := int(shardIdToMetrics[krShardId].KeyCountKR[krId])
	shardToMetrics := shardIdToMetrics[krShardId].MetricsKR[krId]

	// try fitting on shards with adjacent key ranges
	adjShards := b.getAdjacentShards(krId)
	for adjShard := range adjShards {
		if b.fitsOnShard(shardToMetrics, keyCountToMove, krKeyCount, shardIdToMetrics[adjShard]) {
			return adjShard, true
		}
	}
	// try fitting on other shards ordered by criterion load ascending
	for i := len(shardMetrics) - 1; i >= 0; i-- {
		if b.fitsOnShard(shardToMetrics, keyCountToMove, krKeyCount, shardMetrics[i]) {
			return shardMetrics[i].ShardId, true
		}
	}
	return "", false
}

// moveMaxPossible determines where most keys can be sent
// TODO unit tests
func (b *BalancerImpl) moveMaxPossible(shardMetrics []*ShardMetrics, shardIdToMetrics map[string]*ShardMetrics, krId string, krShardId string) (shardId string, maxKeyCount int) {
	maxKeyCount = -1
	for i := len(shardMetrics) - 1; i >= 0; i-- {
		keyCount := b.maxFitOnShard(shardIdToMetrics[krShardId].MetricsKR[krId], shardIdToMetrics[krShardId].KeyCountKR[krId], shardMetrics[i])
		if keyCount > maxKeyCount {
			maxKeyCount = keyCount
			shardId = shardMetrics[i].ShardId
		}
	}
	return
}

// fitsOnShard
// TODO unit tests
func (b *BalancerImpl) fitsOnShard(krMetrics []float64, keyCountToMove int, krKeyCount int, shard *ShardMetrics) bool {
	for kind, metric := range shard.MetricsTotal {
		meanKeyMetric := krMetrics[kind] / float64(krKeyCount)
		loadExpectation := meanKeyMetric*float64(keyCountToMove) + metric
		if b.threshold[kind] < loadExpectation {
			return false
		}
	}
	return true
}

// maxFitOnShard determines how many keys we can fit on shard
// TODO unit tests
func (b *BalancerImpl) maxFitOnShard(krMetrics []float64, krKeyCount int64, shard *ShardMetrics) (maxCount int) {
	maxCount = -1
	for kind, metric := range shard.MetricsTotal {
		// TODO move const to config
		krMeanMetricKey := krMetrics[kind] / float64(krKeyCount)
		count := int(0.8 * ((b.threshold[kind] - metric) / krMeanMetricKey))
		if count > maxCount {
			maxCount = count
		}
	}
	return
}

func (b *BalancerImpl) getAdjacentShards(krId string) map[string]struct{} {
	res := make(map[string]struct{}, 0)
	krIdx := b.krIdx[krId]
	if krIdx != 0 {
		res[b.keyRanges[krIdx-1].ShardID] = struct{}{}
	}
	if krIdx < len(b.keyRanges)-1 {
		res[b.keyRanges[krIdx+1].ShardID] = struct{}{}
	}
	// do not include current shard
	delete(res, b.keyRanges[krIdx].ShardID)
	return res
}

func (b *BalancerImpl) getCriterion(shards []*ShardMetrics) (value float64, kind int) {
	value = -1
	kind = -1
	for _, state := range shards {
		v, k := MaxRelative(state.MetricsTotal, b.threshold)
		if v > value {
			value = v
			kind = k
		}
	}
	return
}

func (b *BalancerImpl) getMostLoadedKR(shard *ShardMetrics, kind int) (value float64, krId string) {
	value = -1
	for krg := range shard.MetricsKR {
		metric := shard.MetricsKR[krg][kind]
		totalKRMetric := metric
		if totalKRMetric > value {
			value = totalKRMetric
			krId = krg
		}
	}
	return
}

func (b *BalancerImpl) getTasks(ctx context.Context, shardFrom *ShardMetrics, krId string, shardToId string, keyCount int) (*tasks.TaskGroup, error) {
	spqrlog.Zero.Debug().
		Str("shard_from", shardFrom.ShardId).
		Str("shard_to", shardToId).
		Str("key_range", krId).
		Int("key_count", keyCount).
		Msg("generating move tasks")
	// Move from beginning or the end of key range
	krInd := b.krIdx[krId]
	krIdTo := ""
	var join tasks.JoinType = tasks.JoinNone
	if krInd < len(b.keyRanges)-1 && b.keyRanges[krInd+1].ShardID == shardToId {
		krIdTo = b.keyRanges[krInd+1].ID
		join = tasks.JoinRight
	} else if krInd > 0 && b.keyRanges[krInd-1].ShardID == shardToId {
		krIdTo = b.keyRanges[krInd-1].ID
		join = tasks.JoinLeft
	}

	host := shardFrom.TargetReplica
	if host == "" {
		host = shardFrom.Master
	}
	conn, err := pgx.Connect(ctx, host)
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
	if err != nil {
		return nil, err
	}
	for _, r := range allRels {
		if r.Name == relName {
			rel = r
			break
		}
	}
	if rel == nil {
		return nil, fmt.Errorf("relation \"%s\" not found", relName)
	}

	moveCount := min((keyCount+config.BalancerConfig().KeysPerMove-1)/config.BalancerConfig().KeysPerMove, config.BalancerConfig().MaxMoveCount)

	counts := make([]int, moveCount)
	for i := 0; i < len(counts)-1; i++ {
		counts[i] = config.BalancerConfig().KeysPerMove
	}
	counts[len(counts)-1] = min(keyCount-(moveCount-1)*config.BalancerConfig().KeysPerMove, config.BalancerConfig().KeysPerMove)
	groupTasks := make([]*tasks.Task, moveCount)
	totalCount := 0
	// TODO multidimensional key ranges
	for i, count := range counts {
		offset := totalCount + count
		if join != tasks.JoinLeft {
			offset--
		}
		query := fmt.Sprintf(`
		SELECT %s as idx
		FROM %s
		ORDER BY idx %s
		LIMIT 1
		OFFSET %d
		`, rel.DistributionKey[0].Column, rel.Name, func() string {
			if join != tasks.JoinLeft {
				return "DESC"
			}
			return ""
		}(), offset)
		spqrlog.Zero.Debug().
			Str("query", query).
			Msg("getting split bound")
		row := conn.QueryRow(ctx, query)
		// TODO typed key ranges
		var idx string
		if err := row.Scan(&idx); err != nil {
			return nil, err
		}
		groupTasks[i] = &tasks.Task{
			ShardFromId: shardFrom.ShardId,
			ShardToId:   shardToId,
			KrIdFrom:    krId,
			KrIdTo:      krIdTo,
			Bound:       []byte(idx),
		}
		totalCount += count
	}

	return &tasks.TaskGroup{Tasks: groupTasks, JoinType: join}, nil
}

func (b *BalancerImpl) getCurrentTaskGroupFromQDB(ctx context.Context) (group *tasks.TaskGroup, err error) {
	tasksService := protos.NewTasksServiceClient(b.coordinatorConn)
	resp, err := tasksService.GetTaskGroup(ctx, &protos.GetTaskGroupRequest{})
	if err != nil {
		return nil, err
	}
	return tasks.TaskGroupFromProto(resp.TaskGroup), nil
}

func (b *BalancerImpl) syncTaskGroupWithQDB(ctx context.Context, group *tasks.TaskGroup) error {
	tasksService := protos.NewTasksServiceClient(b.coordinatorConn)
	_, err := tasksService.WriteTaskGroup(ctx, &protos.WriteTaskGroupRequest{TaskGroup: tasks.TaskGroupToProto(group)})
	return err
}

func (b *BalancerImpl) removeTaskGroupFromQDB(ctx context.Context) error {
	tasksService := protos.NewTasksServiceClient(b.coordinatorConn)
	_, err := tasksService.RemoveTaskGroup(ctx, &protos.RemoveTaskGroupRequest{})
	return err
}

func (b *BalancerImpl) executeTasks(ctx context.Context, group *tasks.TaskGroup) error {

	keyRangeService := protos.NewKeyRangeServiceClient(b.coordinatorConn)

	id := uuid.New()

	for len(group.Tasks) > 0 {
		task := group.Tasks[0]
		spqrlog.Zero.Debug().
			Str("key_range_from", task.KrIdFrom).
			Str("key_range_to", task.KrIdTo).
			Str("bound", string(task.Bound)).
			Int("state", int(task.State)).
			Msg("processing task")
		switch task.State {
		case tasks.TaskPlanned:
			newKeyRange := fmt.Sprintf("kr_%s", id.String())

			if _, err := keyRangeService.SplitKeyRange(ctx, &protos.SplitKeyRangeRequest{
				NewId:     newKeyRange,
				SourceId:  task.KrIdFrom,
				Bound:     task.Bound,
				SplitLeft: group.JoinType == tasks.JoinLeft,
			}); err != nil {
				return err
			}

			task.KrIdTemp = newKeyRange
			task.State = tasks.TaskSplit
			if err := b.syncTaskGroupWithQDB(ctx, group); err != nil {
				// TODO mb retry?
				return err
			}
			continue
		case tasks.TaskSplit:
			// TODO account for join type
			if _, err := keyRangeService.MoveKeyRange(ctx, &protos.MoveKeyRangeRequest{
				Id:        task.KrIdTemp,
				ToShardId: task.ShardToId,
			}); err != nil {
				return err
			}
			task.State = tasks.TaskMoved
			if err := b.syncTaskGroupWithQDB(ctx, group); err != nil {
				// TODO mb retry?
				return err
			}
			continue
		case tasks.TaskMoved:
			if group.JoinType != tasks.JoinNone {
				if _, err := keyRangeService.MergeKeyRange(ctx, &protos.MergeKeyRangeRequest{
					BaseId:      task.KrIdTo,
					AppendageId: task.KrIdTemp,
				}); err != nil {
					return err
				}
			} else {
				for _, otherTask := range group.Tasks[1:] {
					otherTask.KrIdTo = task.KrIdTemp
				}
				group.JoinType = tasks.JoinRight
				id = uuid.New()
			}
			group.Tasks = group.Tasks[1:]
			if err := b.syncTaskGroupWithQDB(ctx, group); err != nil {
				// TODO mb retry?
				return err
			}
			continue
		default:
			return fmt.Errorf("unknown task state %d", task.State)
		}
	}

	// TODO mb retry?
	return b.removeTaskGroupFromQDB(ctx)
}

func (b *BalancerImpl) updateKeyRanges(ctx context.Context) error {
	keyRangeService := protos.NewKeyRangeServiceClient(b.coordinatorConn)
	keyRangesProto, err := keyRangeService.ListAllKeyRanges(ctx, &protos.ListAllKeyRangesRequest{})
	if err != nil {
		return err
	}
	keyRanges := make([]*kr.KeyRange, len(keyRangesProto.KeyRangesInfo))
	for i, krProto := range keyRangesProto.KeyRangesInfo {
		keyRanges[i] = kr.KeyRangeFromProto(krProto)
	}
	sort.Slice(keyRanges, func(i, j int) bool {
		return kr.CmpRangesLess(keyRanges[i].LowerBound, keyRanges[j].LowerBound)
	})
	b.keyRanges = keyRanges
	b.krIdx = make(map[string]int)
	b.shardKr = make(map[string][]int)
	for i, krg := range b.keyRanges {
		b.krIdx[krg.ID] = i
		if _, ok := b.shardKr[krg.ShardID]; !ok {
			b.shardKr[krg.ShardID] = make([]int, 0)
		}
		b.shardKr[krg.ShardID] = append(b.shardKr[krg.ShardID], i)
	}

	return nil
}
