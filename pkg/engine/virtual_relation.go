package engine

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/rrelation"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
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
