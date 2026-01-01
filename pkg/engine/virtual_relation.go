package engine

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

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
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/router/statistics"
)

func GetVPHeader(stmts ...string) []pgproto3.FieldDescription {
	var desc []pgproto3.FieldDescription
	for _, stmt := range stmts {
		desc = append(desc, TextOidFD(stmt))
	}
	return desc
}

func KeyRangeVirtualRelationScan(krs []*kr.KeyRange, locks []string) *tupleslot.TupleTableSlot {

	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("Key range ID", "Shard ID", "Distribution ID", "Lower bound", "Locked"),
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
		Desc: GetVPHeader("table name", "schema name", "schema version", "shards", "column sequence mapping"),
	}
	for _, r := range rrs {
		schema := r.SchemaName
		if schema == "" {
			schema = "$search_path"
		}
		tts.Raw = append(tts.Raw, [][]byte{
			[]byte(r.TableName),
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
			"total tcp connection count",
			"total cancel requests",
			"active tcp connections")}

	tts.WriteDataRow(
		fmt.Sprintf("%v", ci.TotalTcpCount()),
		fmt.Sprintf("%v", ci.TotalCancelCount()),
		fmt.Sprintf("%v", ci.ActiveTcpCount()))

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
		Desc: GetVPHeader("Relation name", "Distribution ID", "Distribution key", "Schema name"),
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
			schema := rel.SchemaName
			if schema == "" {
				schema = "$search_path"
			}
			tts.WriteDataRow(rel.Name, ds, strings.Join(dsKey, ","), schema)
			c++
		}
	}
	return tts, nil
}

var BackendConnectionsHeaders = []string{
	"backend connection id",
	"router",
	"shard key name",
	"hostname",
	"pid",
	"user",
	"dbname",
	"sync",
	"tx_served",
	"tx status",
	"is stale",
	"created at",
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
		Desc: GetVPHeader("ID", "Relation name", "Column", "Column type"),
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
		tts.WriteDataRow(idx.ID, idx.RelationName.RelationName, idx.ColumnName, idx.ColType)
	}
	return tts
}

func TaskGroupsVirtualRelationScan(groups map[string]*tasks.MoveTaskGroup) *tupleslot.TupleTableSlot {
	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("Task group ID", "Destination shard ID", "Source key range ID", "Destination key range ID", "Move task ID"),
	}
	for _, group := range groups {
		currTaskId := ""
		if group.CurrentTask != nil {
			currTaskId = group.CurrentTask.ID
		}
		tts.WriteDataRow(group.ID, group.ShardToId, group.KrIdFrom, group.KrIdTo, currTaskId)
	}
	return tts
}

func MoveTasksVirtualRelationScan(ts map[string]*tasks.MoveTask, dsIDColTypes map[string][]string, moveTaskDsID map[string]string) (*tupleslot.TupleTableSlot, error) {
	tts := &tupleslot.TupleTableSlot{
		Desc: GetVPHeader("Move task ID", "Temporary key range ID", "Bound", "State", "Task group ID"),
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
			task.ID,
			task.KrIdTemp,
			strings.Join(krData, ";"),
			tasks.TaskStateToStr(task.State),
			task.TaskGroupID,
		)
	}
	return tts, nil
}
