package plan

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/engine"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/pkg/tsa"
)

func GetVPHeader(stmts ...string) []pgproto3.FieldDescription {
	var desc []pgproto3.FieldDescription
	for _, stmt := range stmts {
		desc = append(desc, engine.TextOidFD(stmt))
	}
	return desc
}

func KeyRangeVirtualPlan(krs []*kr.KeyRange, locks []string) *VirtualPlan {
	vp := &VirtualPlan{}

	vp.VirtualRowCols = GetVPHeader("Key range ID", "Shard ID", "Distribution ID", "Lower bound", "Locked")

	lockMap := make(map[string]string, len(locks))
	for _, idKeyRange := range locks {
		lockMap[idKeyRange] = "true"
	}

	for _, keyRange := range krs {
		isLocked := "false"
		if lockState, ok := lockMap[keyRange.ID]; ok {
			isLocked = lockState
		}

		vp.VirtualRowVals = append(vp.VirtualRowVals, [][]byte{
			[]byte(keyRange.ID),
			[]byte(keyRange.ShardID),
			[]byte(keyRange.Distribution),
			[]byte(strings.Join(keyRange.SendRaw(), ",")),
			[]byte(isLocked),
		})
	}

	return vp
}

func HostsVirtualPlan(shards []*topology.DataShard, ihc map[string]tsa.CachedCheckResult) *VirtualPlan {

	vp := &VirtualPlan{}

	vp.VirtualRowCols = GetVPHeader("shard", "host", "alive", "rw", "time")

	spqrlog.Zero.Debug().Msg("listing hosts and statuses")

	for _, shard := range shards {
		for _, h := range shard.Cfg.Hosts() {
			hc, ok := ihc[h]
			if !ok {

				vp.VirtualRowVals = append(vp.VirtualRowVals, [][]byte{
					[]byte(shard.ID),
					[]byte(h),
					[]byte("unknown"),
					[]byte("unknown"),
					[]byte("unknown"),
				})

			} else {

				vp.VirtualRowVals = append(vp.VirtualRowVals, [][]byte{

					[]byte(shard.ID),
					[]byte(h),
					fmt.Appendf(nil, "%v", hc.CR.Alive),
					fmt.Appendf(nil, "%v", hc.CR.RW),
					fmt.Appendf(nil, "%v", hc.LastCheckTime),
				})
			}
		}
	}

	return vp
}
