package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/hashfunction"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
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
		Desc: GetVPHeader("table name", "schema version", "shards", "column sequence mapping"),
	}
	for _, r := range rrs {

		tts.Raw = append(tts.Raw, [][]byte{
			[]byte(r.TableName),
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
	dsToRels map[string][]*distributions.DistributedRelation,
	condition spqrparser.WhereClauseNode) (*tupleslot.TupleTableSlot, error) {

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
	index := map[string]int{"distribution_id": 0}
	for _, ds := range dss {
		rels := dsToRels[ds]
		sort.Slice(rels, func(i, j int) bool {
			return rels[i].Name < rels[j].Name
		})
		if ok, err := MatchRow([][]byte{[]byte(ds)}, index, condition); err != nil {
			return nil, err
		} else if !ok {
			continue
		}
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
