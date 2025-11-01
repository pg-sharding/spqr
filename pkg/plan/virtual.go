package plan

import (
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/engine"
	"github.com/pg-sharding/spqr/pkg/models/kr"
)

func KeyRangeVirtualPlan(krs []*kr.KeyRange, locks []string) *VirtualPlan {
	vp := &VirtualPlan{}

	vp.VirtualRowCols = []pgproto3.FieldDescription{
		engine.TextOidFD("Key range ID"),
		engine.TextOidFD("Shard ID"),
		engine.TextOidFD("Distribution ID"),
		engine.TextOidFD("Lower bound"),
		engine.TextOidFD("Locked"),
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
