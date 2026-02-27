package engine

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/client"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/netutil"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/rps"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/statistics"
)

func GetVPHeader(stmts ...string) []pgproto3.FieldDescription {
	var desc []pgproto3.FieldDescription
	for _, stmt := range stmts {
		desc = append(desc, TextOidFD(stmt))
	}
	return desc
}

func KeyRangeVirtualRelationScan(
	krs []*kr.KeyRange,
	locks []string) *tupleslot.TupleTableSlot {
	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("key_range_id", "shard_id", "distribution_id", "lower_bound", "locked"),
	}

	lockMap := make(map[string]string, len(locks))
	for _, idKeyRange := range locks {
		lockMap[idKeyRange] = "true"
	}

	for _, keyRange := range krs {
		isLocked := "false"
		if lockState, ok := lockMap[keyRange.ID]; ok {
			isLocked = lockState
		}

		tts.Raw = append(tts.Raw, [][]byte{
			[]byte(keyRange.ID),
			[]byte(keyRange.ShardID),
			[]byte(keyRange.Distribution),
			[]byte(strings.Join(keyRange.SendRaw(), ",")),
			[]byte(isLocked),
		})
	}

	return tts
}

func KeyRangeVirtualRelationScanExtended(
	krs []*kr.KeyRange,
	locks []string,
	dists []*distributions.Distribution) (*tupleslot.TupleTableSlot, error) {
	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("key_range_id", "shard_id", "distribution_id", "lower_bound", "next_lower_bound", "coverage", "locked"),
	}

	lockMap := make(map[string]string, len(locks))
	for _, idKeyRange := range locks {
		lockMap[idKeyRange] = "true"
	}

	// Build distribution map for lookup
	distMap := make(map[string]*distributions.Distribution)
	for _, d := range dists {
		distMap[d.Id] = d
		if len(d.ColTypes) == 0 {
			return nil, fmt.Errorf("malformed distribution %v", d.Id)
		}
	}

	distToKrs := make(map[string][]*kr.KeyRange)
	for _, keyRange := range krs {
		distToKrs[keyRange.Distribution] = append(distToKrs[keyRange.Distribution], keyRange)
		if len(keyRange.LowerBound) == 0 {
			return nil, fmt.Errorf("malformed key range %v", keyRange.ID)
		}
	}

	for _, keyRange := range krs {
		isLocked := "false"
		if lockState, ok := lockMap[keyRange.ID]; ok {
			isLocked = lockState
		}

		dist, ok := distMap[keyRange.Distribution]
		if !ok {
			return nil,
				fmt.Errorf("could not find distribution %s for key range %v", keyRange.Distribution, keyRange.ID)
		}

		distKrs := distToKrs[keyRange.Distribution]

		/* TODO: Fix this AI mess  */
		var nextKr *kr.KeyRange
		for i, kr := range distKrs {
			if kr.ID == keyRange.ID && i < len(distKrs)-1 {
				nextKr = distKrs[i+1]
				break
			}
		}

		next_lower_bound := "+inf"

		var maxValue any
		if nextKr != nil {
			maxValue = nextKr.LowerBound[0]

			next_lower_bound = strings.Join(nextKr.SendRaw(), ",")
		} else {
			// Last key range - calculate coverage to max value

			switch dist.ColTypes[0] {
			case qdb.ColumnTypeInteger:
				maxValue = int64(math.MaxInt64)
			case qdb.ColumnTypeUinteger, qdb.ColumnTypeVarcharHashed:
				maxValue = uint64(math.MaxUint64)
			case qdb.ColumnTypeUUID:
				maxValue = "ffffffff-ffff-ffff-ffff-ffffffffffff"
			default:
				// For varchar, coverage is meaningless
				maxValue = nil
			}
		}

		var coverage string
		if maxValue != nil {
			/* TODO: multicolumn support? */
			coverage = calculateCoverage(
				keyRange.LowerBound[0],
				maxValue,
				dist.ColTypes[0],
			)
		} else {
			coverage = "N/A"
		}

		tts.WriteDataRow(
			keyRange.ID,
			keyRange.ShardID,
			keyRange.Distribution,
			strings.Join(keyRange.SendRaw(), ","),
			next_lower_bound,
			coverage,
			isLocked,
		)
	}

	return tts, nil
}

func calculateCoverage(lowerBound, upperBound any, colType string) string {
	switch colType {
	case qdb.ColumnTypeInteger:
		lower, ok := lowerBound.(int64)
		if !ok {
			return "N/A"
		}
		upper, ok := upperBound.(int64)
		if !ok {
			return "N/A"
		}
		if upper <= lower {
			return "0.00%"
		}
		totalRange := float64(math.MaxInt64) - float64(math.MinInt64)
		keyRangeSize := float64(upper - lower)
		percentage := (keyRangeSize / totalRange) * 100.0
		return fmt.Sprintf("%.2f%%", percentage)

	case qdb.ColumnTypeUinteger, qdb.ColumnTypeVarcharHashed:
		lower, ok := lowerBound.(uint64)
		if !ok {
			return "N/A"
		}
		upper, ok := upperBound.(uint64)
		if !ok {
			return "N/A"
		}
		if upper <= lower {
			return "0.00%"
		}
		totalRange := float64(math.MaxUint64)
		keyRangeSize := float64(upper - lower)
		percentage := (keyRangeSize / totalRange) * 100.0
		return fmt.Sprintf("%.2f%%", percentage)

	case qdb.ColumnTypeUUID:
		lowerStr, ok := lowerBound.(string)
		if !ok {
			return "N/A"
		}
		upperStr, ok := upperBound.(string)
		if !ok {
			return "N/A"
		}

		lowerUUID, err := uuid.Parse(lowerStr)
		if err != nil {
			return "N/A"
		}
		upperUUID, err := uuid.Parse(upperStr)
		if err != nil {
			return "N/A"
		}

		// Convert UUIDs to big.Int for comparison
		lowerBytes := lowerUUID[:]
		upperBytes := upperUUID[:]

		lowerBig := new(big.Int).SetBytes(lowerBytes)
		upperBig := new(big.Int).SetBytes(upperBytes)

		if upperBig.Cmp(lowerBig) <= 0 {
			return "0.00%"
		}

		// UUID space is 2^128
		uuidSpace := new(big.Int).Exp(big.NewInt(2), big.NewInt(128), nil)
		keyRangeSize := new(big.Int).Sub(upperBig, lowerBig)

		// Calculate coverage percentage
		coverage := new(big.Float).Quo(
			new(big.Float).SetInt(keyRangeSize),
			new(big.Float).SetInt(uuidSpace),
		)
		percentage := new(big.Float).Mul(coverage, big.NewFloat(100.0))

		percentageValue, _ := percentage.Float64()
		return fmt.Sprintf("%.2f%%", percentageValue)

	default:
		// For varchar coverage is meaningless
		return "N/A"
	}
}

func HostsVirtualRelationScan(shards []*topology.DataShard, ihc map[string]tsa.CachedCheckResult) *tupleslot.TupleTableSlot {

	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("shard", "host", "alive", "rw", "time"),
	}

	spqrlog.Zero.Debug().Msg("listing hosts and statuses")

	for _, shard := range shards {
		for _, h := range shard.Cfg.Hosts() {
			hc, ok := ihc[h]
			if !ok {

				tts.Raw = append(tts.Raw, [][]byte{
					[]byte(shard.ID),
					[]byte(h),
					[]byte("unknown"),
					[]byte("unknown"),
					[]byte("unknown"),
				})

			} else {

				tts.Raw = append(tts.Raw, [][]byte{

					[]byte(shard.ID),
					[]byte(h),
					fmt.Appendf(nil, "%v", hc.CR.Alive),
					fmt.Appendf(nil, "%v", hc.CR.RW),
					fmt.Appendf(nil, "%v", hc.LastCheckTime),
				})
			}
		}
	}

	return tts
}

func ReferenceRelationsScan(rrs []*rrelation.ReferenceRelation) *tupleslot.TupleTableSlot {

	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("table_name", "schema_name", "schema_version", "shards", "column_sequence_mapping"),
	}
	for _, r := range rrs {
		schema := r.RelationName.GetSchema()
		tts.Raw = append(tts.Raw, [][]byte{
			[]byte(r.RelationName.RelationName),
			[]byte(schema),
			fmt.Appendf(nil, "%d", r.SchemaVersion),
			fmt.Appendf(nil, "%+v", r.ShardIds),
			fmt.Appendf(nil, "%+v", r.ColumnSequenceMapping),
		})
	}

	return tts
}

func TSAVirtualRelationScan(cacheEntries map[pool.TsaKey]pool.CachedEntry) *tupleslot.TupleTableSlot {

	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("tsa", "host", "az", "alive", "match", "reason", "last_check_time"),
	}

	count := 0
	for key, entry := range cacheEntries {
		tts.Raw = append(tts.Raw, [][]byte{
			[]byte(key.Tsa),
			[]byte(key.Host),
			[]byte(key.AZ),
			fmt.Appendf(nil, "%v", entry.Result.Alive),
			fmt.Appendf(nil, "%v", entry.Result.Match),
			[]byte(entry.Result.Reason),
			[]byte(entry.LastCheckTime.Format(time.RFC3339)),
		})
		count++
	}

	return tts
}

// TODO refactor it to make more user-friendly
func InstanceVirtualRelationScan(ctx context.Context, ci connmgr.ConnectionMgr) *tupleslot.TupleTableSlot {

	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader(
			"total_tcp_connection_count",
			"total_cancel_requests",
			"active_tcp_connections",
			"total_requests",
			"current_rps",
			"avg_rps",
			"peak_rps")}

	stats := rps.GetRPSFullSnapshot()

	tts.WriteDataRow(
		fmt.Sprintf("%v", ci.TotalTcpCount()),
		fmt.Sprintf("%v", ci.TotalCancelCount()),
		fmt.Sprintf("%v", ci.ActiveTcpCount()),
		fmt.Sprintf("%d", stats.TotalRequests),
		fmt.Sprintf("%.2f", stats.CurrentRPS),
		fmt.Sprintf("%.2f", stats.AvgRPS),
		fmt.Sprintf("%.2f", stats.PeakRPS))

	return tts
}

func PreparedStatementsVirtualRelationScan(ctx context.Context, shs []shard.PreparedStatementsMgrDescriptor) *tupleslot.TupleTableSlot {
	tts := &tupleslot.TupleTableSlot{Desc: GetVPHeader("name", "backend_id", "hash", "query")}

	for _, sh := range shs {
		tts.WriteDataRow(sh.Name, fmt.Sprintf("%d", sh.ServerId), fmt.Sprintf("%d", sh.Hash), sh.Query)
	}

	return tts
}

// TODO unit tests

// Relations sends information about attached relations that satisfy conditions in WHERE-clause
// Relations writes relation information to the PSQL client based on the given distribution-to-relations map and condition.
//
// Parameters:
// - dsToRels (map[string][]*distributions.DistributedRelation): The map of distribution names to their corresponding distributed relations.
// - condition (spqrparser.WhereClauseNode): The condition for filtering the relations.
//
// Returns:
// - error: An error if any occurred during the operation.
func RelationsVirtualRelationScan(
	dsToRels map[string][]*distributions.DistributedRelation) (*tupleslot.TupleTableSlot, error) {

	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("relation_name", "distribution_id", "distribution_key", "schema_name"),
	}

	/* XXX: make sort support in outer abstraction layer */
	dss := make([]string, len(dsToRels))
	i := 0
	for ds := range dsToRels {
		dss[i] = ds
		i++
	}
	sort.Strings(dss)

	c := 0
	for _, ds := range dss {
		rels := dsToRels[ds]
		for _, rel := range rels {
			dsKey := make([]string, len(rel.DistributionKey))
			for i, e := range rel.DistributionKey {
				t, err := hashfunction.HashFunctionByName(e.HashFunction)
				if err != nil {
					return nil, err
				}
				dsKey[i] = fmt.Sprintf("(\"%s\", %s)", e.Column, hashfunction.ToString(t))
			}
			schema := rel.Relation.GetSchema()
			tts.WriteDataRow(rel.Relation.RelationName, ds, strings.Join(dsKey, ","), schema)
			c++
		}
	}
	return tts, nil
}

var BackendConnectionsHeaders = []string{
	"backend_connection_id",
	"router",
	"shard_key_name",
	"hostname",
	"pid",
	"user",
	"dbname",
	"sync",
	"tx_served",
	"tx_status",
	"is_stale",
	"created_at",
}

// BackendConnections writes backend connection information to the PSQL client.
//
// Parameters:
// - _ (context.Context): The context for the operation.
// - shs ([]shard.Shardinfo): The list of shard information.
// - stmt (*spqrparser.Show): The query itself.
//
// Returns:
// - error: An error if any occurred during the operation.
func BackendConnectionsVirtualRelationScan(shs []shard.ShardHostCtl) (*tupleslot.TupleTableSlot, error) {

	getRouter := func(sh shard.ShardHostCtl) string {
		router := "no data"
		s, ok := sh.(shard.CoordShardinfo)
		if ok {
			router = s.Router()
		}
		return router
	}

	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader(BackendConnectionsHeaders...),
	}

	var rows [][][]byte

	for _, sh := range shs {

		rows = append(rows, [][]byte{
			fmt.Appendf(nil, "%d", sh.ID()),
			[]byte(getRouter(sh)),
			[]byte(sh.ShardKeyName()),
			[]byte(sh.InstanceHostname()),
			fmt.Appendf(nil, "%d", sh.Pid()),
			[]byte(sh.Usr()),
			[]byte(sh.DB()),
			[]byte(strconv.FormatInt(sh.Sync(), 10)),
			[]byte(strconv.FormatInt(sh.TxServed(), 10)),
			[]byte(sh.TxStatus().String()),
			[]byte(strconv.FormatBool(sh.IsStale())),
			[]byte(sh.CreatedAt().UTC().Format(time.RFC3339)),
		})
	}

	tts.Raw = rows

	return tts, nil
}

// TODO : unit tests

// Clients retrieves client information based on provided client information, filtering conditions and writes the data to the PSQL client.
//
// Parameters:
// - ctx (context.Context): The context for the operation.
// - clients ([]client.ClientInfo): The list of client information to process.
// - condition (spqrparser.WhereClauseNode): The condition to filter the client information.
//
// Returns:
// - error: An error if any occurred during the operation.
func ClientsVirtualRelationScan(ctx context.Context, clients []client.ClientInfo) (*tupleslot.TupleTableSlot, error) {

	quantiles := statistics.GetQuantiles()
	headers := []string{
		"client_id", "user", "dbname", "server_id", "router_address", "is_alive",
	}
	for _, el := range *quantiles {
		headers = append(headers, fmt.Sprintf("router_time_%g", el))
		headers = append(headers, fmt.Sprintf("shard_time_%g", el))
	}

	header := GetVPHeader(headers...)

	tts := &tupleslot.TupleTableSlot{
		Desc: header,
	}

	getRow := func(cl client.Client, hostname string, rAddr string) [][]byte {
		quantiles := statistics.GetQuantiles()
		rowData := [][]byte{
			fmt.Appendf(nil, "%d", cl.ID()),
			[]byte(cl.Usr()),
			[]byte(cl.DB()),
			[]byte(hostname),
			[]byte(rAddr),
			fmt.Appendf(nil, "%v", netutil.TCP_CheckAliveness(cl.Conn()))}

		for _, el := range *quantiles {
			rowData = append(rowData, fmt.Appendf(nil, "%.2fms",
				statistics.GetTimeQuantile(statistics.StatisticsTypeRouter, el, cl)))
			rowData = append(rowData, fmt.Appendf(nil, "%.2fms",
				statistics.GetTimeQuantile(statistics.StatisticsTypeShard, el, cl)))
		}
		return rowData
	}

	var data [][][]byte
	for _, cl := range clients {
		if len(cl.Shards()) > 0 {
			for _, sh := range cl.Shards() {
				if sh == nil {
					continue
				}
				row := getRow(cl, sh.Instance().Hostname(), cl.RAddr())

				data = append(data, row)
			}
		} else {
			row := getRow(cl, "no backend connection", cl.RAddr())

			data = append(data, row)
		}
	}

	tts.Raw = data

	return tts, nil
}

func UniqueIndexesVirtualRelationScan(idToidxs map[string]*distributions.UniqueIndex) *tupleslot.TupleTableSlot {

	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("id", "relation_name", "columns", "column_types"),
	}

	/* XXX: make sort support in outer abstraction layer */
	ids := make([]string, len(idToidxs))
	i := 0
	for id := range idToidxs {
		ids[i] = id
		i++
	}
	sort.Strings(ids)

	for _, id := range ids {
		idx := idToidxs[id]
		tts.WriteDataRow(idx.ID, idx.RelationName.RelationName, strings.Join(idx.Columns, ","), strings.Join(idx.ColTypes, ","))
	}
	return tts
}

func TaskGroupsVirtualRelationScan(groups map[string]*tasks.MoveTaskGroup, statuses map[string]*tasks.MoveTaskGroupStatus) *tupleslot.TupleTableSlot {
	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("task_group_id", "destination_shard_id", "source_key_range_id", "destination_key_range_id", "batch_size", "move_task_id", "state", "message", "created_at", "updated_at"),
	}
	for id, group := range groups {
		status, ok := statuses[id]
		if !ok {
			status = &tasks.MoveTaskGroupStatus{State: tasks.TaskGroupPlanned}
		}
		currTaskId := ""
		if group.CurrentTask != nil {
			currTaskId = group.CurrentTask.ID
		}
		tts.WriteDataRow(group.ID, group.ShardToId, group.KrIdFrom, group.KrIdTo, strconv.FormatInt(group.BatchSize, 10), currTaskId, string(status.State), status.Message, group.CreatedAt.Format("02-01-2006 15:04:05"), status.UpdatedAt.Format("02-01-2006 15:04:05"))
	}
	return tts
}

func MoveTasksVirtualRelationScan(ts map[string]*tasks.MoveTask, dsIDColTypes map[string][]string, moveTaskDsID map[string]string) (*tupleslot.TupleTableSlot, error) {
	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("task_group_id", "move_task_id", "temporary_key_range_id", "bound", "state"),
	}

	for _, task := range ts {
		krData := []string{""}
		if task.Bound != nil {
			dsID, ok := moveTaskDsID[task.ID]
			if !ok {
				return nil, fmt.Errorf("failed to reply move task data: distribution for task \"%s\" not found", task.ID)
			}
			moveTaskColTypes, ok := dsIDColTypes[dsID]
			if !ok {
				return nil, fmt.Errorf("failed to reply move task data: column types for distribution \"%s\" not found", dsID)
			}
			if len(task.Bound) != len(moveTaskColTypes) {
				err := fmt.Errorf("something wrong in task: %s, columns: %#v", task.ID, moveTaskColTypes)
				return nil, err
			}
			kRange, err := kr.KeyRangeFromBytes(task.Bound, moveTaskColTypes)
			if err != nil {
				return nil, err
			}
			krData = kRange.SendRaw()
		}
		tts.WriteDataRow(
			task.TaskGroupID,
			task.ID,
			task.KrIdTemp,
			strings.Join(krData, ";"),
			tasks.TaskStateToStr(task.State),
		)
	}
	return tts, nil
}

func TaskGroupBoundsCacheVirtualRelationScan(boundsMap map[string][][][]byte, indexMap map[string]int, colTypesMap map[string][]string) (*tupleslot.TupleTableSlot, error) {
	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("task_group_id", "bound", "status"),
	}
	for id, bounds := range boundsMap {
		index, ok := indexMap[id]
		if !ok {
			return nil, fmt.Errorf("index for task group \"%s\" not present", id)
		}
		colTypes, ok := colTypesMap[id]
		if !ok {
			return nil, fmt.Errorf("column types for task group \"%s\" not present", id)
		}
		for i, bound := range bounds {
			krData := []string{""}
			if bound != nil {
				kRange, err := kr.KeyRangeFromBytes(bound, colTypes)
				if err != nil {
					return nil, err
				}
				krData = kRange.SendRaw()
			}
			tts.WriteDataRow(
				id,
				strings.Join(krData, ";"),
				func() string {
					if i < index {
						return "USED"
					}
					return ""
				}(),
			)
		}
	}
	return tts, nil
}

func RedistributeTasksVirtualRelationScan(tasks []*tasks.RedistributeTask) (*tupleslot.TupleTableSlot, error) {
	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("redistribute_task_id", "key_range_id", "destination_shard_id", "batch_size"),
	}
	for _, task := range tasks {
		tts.WriteDataRow(
			task.ID,
			task.KeyRangeId,
			task.ShardId,
			strconv.FormatInt(int64(task.BatchSize), 10),
		)
	}

	return tts, nil
}
