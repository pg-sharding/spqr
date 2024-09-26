package provider

import (
	"context"
	"fmt"
	"sort"
	"strings"

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
)

type BalancerImpl struct {
	coordinatorConn *grpc.ClientConn
	threshold       []float64

	shardConns    *config.DatatransferConnections
	dsToKeyRanges map[string][]*kr.KeyRange
	dsToKrIdx     map[string]map[string]int
	shardKr       map[string][]string
	krToDs        map[string]string
}

func NewBalancer() (*BalancerImpl, error) {
	shards, err := loadShardsConfig(config.BalancerConfig().ShardsConfig)
	if err != nil {
		return nil, err
	}
	threshold := make([]float64, 2*metricsCount)
	configThresholds := []float64{config.BalancerConfig().CpuThreshold, config.BalancerConfig().SpaceThreshold}
	for i := 0; i < metricsCount; i++ {
		threshold[i] = configThresholds[i]
		threshold[metricsCount+i] = configThresholds[i]
	}
	conn, err := grpc.NewClient(config.BalancerConfig().CoordinatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &BalancerImpl{
		shardConns:      shards,
		coordinatorConn: conn,
		threshold:       threshold,
		dsToKeyRanges:   map[string][]*kr.KeyRange{},
		dsToKrIdx:       map[string]map[string]int{},
		shardKr:         map[string][]string{},
		krToDs:          map[string]string{},
	}, nil
}

var _ balancer.Balancer = &BalancerImpl{}

func (b *BalancerImpl) RunBalancer(ctx context.Context) {
	// TODO: add command to drop task group to coordinator
	task, err := b.getCurrentTaskFromQDB(ctx)
	if err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error getting current tasks")
		return
	}
	if task == nil || task.KeyCount == 0 {
		task, err = b.generateTasks(ctx)
		if err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error planning tasks")
			return
		}
		if task.KeyCount == 0 {
			spqrlog.Zero.Debug().Msg("Nothing to execute")
			return
		}
		if err := b.syncTaskWithQDB(ctx, task); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("error inserting tasks")
			return
		}
	}
	if err := b.executeTasks(ctx, task); err != nil {
		spqrlog.Zero.Error().Err(err).Msg("error executing tasks")
	}
}

func (b *BalancerImpl) generateTasks(ctx context.Context) (*tasks.BalancerTask, error) {
	shardToState := make(map[string]*ShardMetrics)
	shardStates := make([]*ShardMetrics, 0)
	for shardId, shard := range b.shardConns.ShardsData {
		state, err := b.getShardCurrentState(ctx, shardId, shard)
		if err != nil {
			return nil, err
		}
		shardToState[shardId] = state
		shardStates = append(shardStates, state)
	}

	maxMetric, criterion := b.getCriterion(shardStates)
	sort.Slice(shardStates, func(i, j int) bool {
		return shardStates[i].MetricsTotal[criterion] > shardStates[j].MetricsTotal[criterion]
	})

	spqrlog.Zero.Debug().Float64("metric", maxMetric).Int("criterion", criterion).Msg("Max metric")

	if maxMetric <= 1 {
		spqrlog.Zero.Debug().Msg("Metrics below the threshold, exiting")
		return &tasks.BalancerTask{}, nil
	}

	if err := b.updateKeyRanges(ctx); err != nil {
		return nil, fmt.Errorf("error updating key range info: %s", err)
	}

	if err := b.getStatsByKeyRange(ctx, shardStates[0]); err != nil {
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
		return &tasks.BalancerTask{}, nil
	}
	return b.getTasks(shardFrom, krId, shId, keyCount)
}

func (b *BalancerImpl) getShardCurrentState(ctx context.Context, shardId string, shard *config.ShardConnect) (*ShardMetrics, error) {
	spqrlog.Zero.Debug().Str("shard id", shardId).Msg("getting shard state")
	connStrings := shard.GetConnStrings()
	res := NewShardMetrics()
	res.ShardId = shardId
	replicaMetrics := NewHostMetrics()
	for _, connString := range connStrings {
		hostsMetrics, isMaster, err := b.getHostStatus(ctx, connString)
		if err != nil {
			return nil, err
		}
		if hostsMetrics == nil {
			continue
		}
		if isMaster {
			res.SetMasterMetrics(hostsMetrics)
			res.Master = connString
			continue
		}
		replicaThreshold := b.threshold[metricsCount:]
		if replicaMetrics.MaxRelative(replicaThreshold) < hostsMetrics.MaxRelative(replicaThreshold) {
			replicaMetrics = hostsMetrics
			res.TargetReplica = connString
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
			if _, ok := b.krToDs[krId]; !ok {
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

	for _, krId := range b.shardKr[shard.ShardId] {
		ds := b.krToDs[krId]
		i := b.dsToKrIdx[ds][krId]
		krg := b.dsToKeyRanges[ds][i]
		if krg.ShardID != shard.ShardId {
			continue
		}
		rels, err := b.getKRRelations(ctx, krg)
		if err != nil {
			return err
		}

		for _, rel := range rels {
			queryRaw := `
				SELECT sum(pg_column_size(t.*)) as filesize, count(*) as filerow 
				FROM %s as t
				WHERE %s;
`
			var nextBound kr.KeyRangeBound
			if i < len(b.dsToKeyRanges[ds])-1 {
				nextBound = b.dsToKeyRanges[ds][i+1].LowerBound
			}
			condition := kr.GetKRCondition(rel, krg, nextBound, "t")
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
		// TODO: fix multidim case
		if nextKR != nil {
			buf[i] = fmt.Sprintf("%s >= %s AND %s < %s", hashedCol, kRange.SendRaw()[0], hashedCol, nextKR.SendRaw()[0])
		} else {
			buf[i] = fmt.Sprintf("%s >= %s", hashedCol, kRange.SendRaw()[0])
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
	ds := b.krToDs[krId]
	krIdx := b.dsToKrIdx[ds][krId]
	if krIdx != 0 {
		res[b.dsToKeyRanges[ds][krIdx-1].ShardID] = struct{}{}
	}
	if krIdx < len(b.dsToKeyRanges)-1 {
		res[b.dsToKeyRanges[ds][krIdx+1].ShardID] = struct{}{}
	}
	// do not include current shard
	delete(res, b.dsToKeyRanges[ds][krIdx].ShardID)
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

func (b *BalancerImpl) getTasks(shardFrom *ShardMetrics, krId string, shardToId string, keyCount int) (*tasks.BalancerTask, error) {
	spqrlog.Zero.Debug().
		Str("shard_from", shardFrom.ShardId).
		Str("shard_to", shardToId).
		Str("key_range", krId).
		Int("key_count", keyCount).
		Msg("generating move tasks")
	// Move from beginning or the end of key range
	if _, ok := b.krToDs[krId]; !ok {
		return nil, fmt.Errorf("unknown key range id \"%s\"", krId)
	}
	ds := b.krToDs[krId]
	krInd := b.dsToKrIdx[ds][krId]
	krIdTo := ""
	var join tasks.JoinType = tasks.JoinNone
	if krInd < len(b.dsToKeyRanges[ds])-1 && b.dsToKeyRanges[ds][krInd+1].ShardID == shardToId {
		krIdTo = b.dsToKeyRanges[ds][krInd+1].ID
		join = tasks.JoinRight
	} else if krInd > 0 && b.dsToKeyRanges[ds][krInd-1].ShardID == shardToId {
		krIdTo = b.dsToKeyRanges[ds][krInd-1].ID
		join = tasks.JoinLeft
	}

	id := uuid.New()

	return &tasks.BalancerTask{
		KrIdFrom:  krId,
		KrIdTo:    krIdTo,
		KrIdTemp:  id.String(),
		ShardIdTo: shardToId,
		KeyCount:  int64(keyCount),
		Type:      join,
		State:     tasks.BalancerTaskPlanned,
	}, nil
}

//func (b *BalancerImpl) getMoveTasks(ctx context.Context, shardFrom *ShardMetrics, krId string, shardToId string, keyCount int) (*tasks.MoveTaskGroup, error) {
//	spqrlog.Zero.Debug().
//		Str("shard_from", shardFrom.ShardId).
//		Str("shard_to", shardToId).
//		Str("key_range", krId).
//		Int("key_count", keyCount).
//		Msg("generating move tasks")
//	// Move from beginning or the end of key range
//	if _, ok := b.krToDs[krId]; !ok {
//		return nil, fmt.Errorf("unknown key range id \"%s\"", krId)
//	}
//	ds := b.krToDs[krId]
//	krInd := b.dsToKrIdx[ds][krId]
//	krIdTo := ""
//	var join tasks.JoinType = tasks.JoinNone
//	if krInd < len(b.dsToKeyRanges[ds])-1 && b.dsToKeyRanges[ds][krInd+1].ShardID == shardToId {
//		krIdTo = b.dsToKeyRanges[ds][krInd+1].ID
//		join = tasks.JoinRight
//	} else if krInd > 0 && b.dsToKeyRanges[ds][krInd-1].ShardID == shardToId {
//		krIdTo = b.dsToKeyRanges[ds][krInd-1].ID
//		join = tasks.JoinLeft
//	}
//
//	host := shardFrom.TargetReplica
//	if host == "" {
//		host = shardFrom.Master
//	}
//	conn, err := pgx.Connect(ctx, host)
//	if err != nil {
//		return nil, err
//	}
//
//	var maxCount int64 = -1
//	relName := ""
//	for r, count := range shardFrom.KeyCountRelKR[krId] {
//		if count > maxCount {
//			relName = r
//			maxCount = count
//		}
//	}
//
//	var rel *distributions.DistributedRelation = nil
//	allRels, err := b.getKRRelations(ctx, b.dsToKeyRanges[ds][krInd])
//	if err != nil {
//		return nil, err
//	}
//	for _, r := range allRels {
//		if r.Name == relName {
//			rel = r
//			break
//		}
//	}
//	if rel == nil {
//		return nil, fmt.Errorf("relation \"%s\" not found", relName)
//	}
//
//	dsService := protos.NewDistributionServiceClient(b.coordinatorConn)
//
//	dsS, err := dsService.GetDistribution(ctx, &protos.GetDistributionRequest{
//		Id: ds,
//	})
//
//	if err != nil {
//		return nil, err
//	}
//
//	moveCount := min((keyCount+config.BalancerConfig().KeysPerMove-1)/config.BalancerConfig().KeysPerMove, config.BalancerConfig().MaxMoveCount)
//
//	counts := make([]int, moveCount)
//	for i := 0; i < len(counts)-1; i++ {
//		counts[i] = config.BalancerConfig().KeysPerMove
//	}
//	counts[len(counts)-1] = min(keyCount-(moveCount-1)*config.BalancerConfig().KeysPerMove, config.BalancerConfig().KeysPerMove)
//	groupTasks := make([]*tasks.MoveTask, moveCount)
//	totalCount := 0
//	// TODO multidimensional key ranges
//	for i, count := range counts {
//		offset := totalCount + count
//		if join != tasks.JoinLeft {
//			offset--
//		}
//		query := fmt.Sprintf(`
//		SELECT %s as idx
//		FROM %s
//		ORDER BY idx %s
//		LIMIT 1
//		OFFSET %d
//		`, rel.DistributionKey[0].Column, rel.Name, func() string {
//			if join != tasks.JoinLeft {
//				return "DESC"
//			}
//			return ""
//		}(), offset)
//		spqrlog.Zero.Debug().
//			Str("query", query).
//			Msg("getting split bound")
//		row := conn.QueryRow(ctx, query)
//		// TODO typed key ranges
//		var idx string
//		if err := row.Scan(&idx); err != nil {
//			return nil, err
//		}
//
//		var bound []byte
//
//		switch dsS.Distribution.ColumnTypes[0] {
//		case qdb.ColumnTypeVarchar:
//			fallthrough
//		case qdb.ColumnTypeVarcharDeprecated:
//			bound = []byte(idx)
//		case qdb.ColumnTypeVarcharHashed:
//			fallthrough
//		case qdb.ColumnTypeInteger:
//			i, err := strconv.ParseInt(idx, 10, 64)
//			if err != nil {
//				return nil, err
//			}
//			bound = make([]byte, 8)
//			binary.PutVarint(bound, i)
//		case qdb.ColumnTypeUinteger:
//			i, err := strconv.ParseUint(idx, 10, 64)
//			if err != nil {
//				return nil, err
//			}
//			bound = make([]byte, 8)
//			binary.PutUvarint(bound, i)
//		}
//
//		groupTasks[len(groupTasks)-1-i] = &tasks.MoveTask{
//			ShardFromId: shardFrom.ShardId,
//			ShardToId:   shardToId,
//			KrIdFrom:    krId,
//			KrIdTo:      krIdTo,
//			Bound:       bound,
//		}
//		totalCount += count
//	}
//
//	return &tasks.MoveTaskGroup{Tasks: groupTasks, JoinType: join}, nil
//}

func (b *BalancerImpl) getCurrentTaskFromQDB(ctx context.Context) (group *tasks.BalancerTask, err error) {
	tasksService := protos.NewBalancerTaskServiceClient(b.coordinatorConn)
	resp, err := tasksService.GetBalancerTask(ctx, nil)
	if err != nil {
		return nil, err
	}
	return tasks.BalancerTaskFromProto(resp.Task), nil
}

func (b *BalancerImpl) syncTaskWithQDB(ctx context.Context, group *tasks.BalancerTask) error {
	tasksService := protos.NewBalancerTaskServiceClient(b.coordinatorConn)
	_, err := tasksService.WriteBalancerTask(ctx, &protos.WriteBalancerTaskRequest{Task: tasks.BalancerTaskToProto(group)})
	return err
}

func (b *BalancerImpl) removeTaskFromQDB(ctx context.Context) error {
	tasksService := protos.NewBalancerTaskServiceClient(b.coordinatorConn)
	_, err := tasksService.RemoveBalancerTask(ctx, nil)
	return err
}

func (b *BalancerImpl) executeTasks(ctx context.Context, task *tasks.BalancerTask) error {

	keyRangeService := protos.NewKeyRangeServiceClient(b.coordinatorConn)

	for {
		switch task.State {
		case tasks.BalancerTaskPlanned:
			if _, err := keyRangeService.RedistributeKeyRange(ctx, &protos.RedistributeKeyRangeRequest{
				Id:        task.KrIdFrom,
				ToKrId:    task.KrIdTemp,
				ToShardId: task.ShardIdTo,
				BatchSize: int64(config.BalancerConfig().KeysPerMove),
				Limit:     task.KeyCount,
				LimitType: protos.RedistributeLimitType_RedistributeKeysLimit,
				SplitType: func() protos.SplitType {
					switch task.Type {
					case tasks.JoinLeft:
						return protos.SplitType_SplitLeft
					case tasks.JoinNone:
						fallthrough
					case tasks.JoinRight:
						return protos.SplitType_SplitRight
					default:
						panic("unknown split type")
					}
				}(),
			}); err != nil {
				return err
			}
			task.State = tasks.BalancerTaskMoved
			// TODO: save task state
		case tasks.BalancerTaskMoved:
			var err error
			switch task.Type {
			case tasks.JoinLeft:
				fallthrough
			case tasks.JoinRight:
				_, err = keyRangeService.MergeKeyRange(ctx, &protos.MergeKeyRangeRequest{
					BaseId:      task.KrIdTo,
					AppendageId: task.KrIdTemp,
				})
			case tasks.JoinNone:
				break
			default:
				panic("unknown join type")
			}
			if err != nil {
				return err
			}
			// TODO: drop task
		}
	}
	return nil
}

func (b *BalancerImpl) updateKeyRanges(ctx context.Context) error {
	keyRangeService := protos.NewKeyRangeServiceClient(b.coordinatorConn)
	distrService := protos.NewDistributionServiceClient(b.coordinatorConn)
	keyRangesProto, err := keyRangeService.ListAllKeyRanges(ctx, nil)
	if err != nil {
		return err
	}
	keyRanges := make(map[string][]*kr.KeyRange)
	for _, krProto := range keyRangesProto.KeyRangesInfo {
		if _, ok := keyRanges[krProto.DistributionId]; !ok {
			keyRanges[krProto.DistributionId] = make([]*kr.KeyRange, 0)
		}
		ds, err := distrService.GetDistribution(ctx, &protos.GetDistributionRequest{
			Id: krProto.DistributionId,
		})
		if err != nil {
			return err
		}
		keyRanges[krProto.DistributionId] = append(keyRanges[krProto.DistributionId], kr.KeyRangeFromProto(krProto, ds.Distribution.ColumnTypes))
	}
	for _, krs := range keyRanges {
		sort.Slice(krs, func(i, j int) bool {
			return kr.CmpRangesLess(krs[i].LowerBound, krs[j].LowerBound, krs[j].ColumnTypes)
		})
	}

	b.dsToKeyRanges = keyRanges
	b.dsToKrIdx = make(map[string]map[string]int)
	b.shardKr = make(map[string][]string)
	b.krToDs = make(map[string]string)

	for ds, krs := range b.dsToKeyRanges {
		for i, krg := range krs {
			b.krToDs[krg.ID] = ds
			if _, ok := b.dsToKrIdx[ds]; !ok {
				b.dsToKrIdx[ds] = make(map[string]int)
			}
			b.dsToKrIdx[ds][krg.ID] = i
			if _, ok := b.shardKr[krg.ShardID]; !ok {
				b.shardKr[krg.ShardID] = make([]string, 0)
			}
			b.shardKr[krg.ShardID] = append(b.shardKr[krg.ShardID], krg.ID)
		}
	}

	return nil
}

func loadShardsConfig(path string) (*config.DatatransferConnections, error) {
	var err error

	shards, err := config.LoadShardDataCfg(path)
	if err != nil {
		return nil, err
	}
	return shards, nil
}
