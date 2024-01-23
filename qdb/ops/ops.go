package ops

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/qdb"
)

// TODO : unit tests
func AddKeyRangeWithChecks(ctx context.Context, qdb qdb.QDB, keyRange *kr.KeyRange) error {
	if _, err := qdb.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	if _, err := qdb.GetKeyRange(ctx, keyRange.ID); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v already present in qdb", keyRange.ID)
	}

	_, err := qdb.GetKeyspace(ctx, keyRange.Keyspace)
	if err != nil {
		return fmt.Errorf("try to add key range link to a non-existent dataspace")
	}

	existsKrids, err := qdb.ListKeyRanges(ctx, keyRange.Keyspace)
	if err != nil {
		return err
	}

	for _, v := range existsKrids {
		raw := keyRange.Raw()
		eq := len(raw) == len(v.LowerBound)
		if eq {
			for i := 0; i < len(raw); i++ {
				eq = eq && bytes.Equal(raw[i], v.LowerBound[i])
			}
		}

		if eq {
			return fmt.Errorf("key range %v intersects with key range %v in QDB", keyRange.ID, v.KeyRangeID)
		}
	}

	return qdb.AddKeyRange(ctx, keyRange.ToDB())
}

// TODO : unit tests
func ModifyKeyRangeWithChecks(ctx context.Context, qdb qdb.QDB, keyRange *kr.KeyRange) error {
	_, err := qdb.CheckLockedKeyRange(ctx, keyRange.ID)
	if err != nil {
		return err
	}

	if _, err := qdb.GetShard(ctx, keyRange.ShardID); err != nil {
		return err
	}

	krids, err := qdb.ListKeyRanges(ctx, keyRange.Keyspace)
	if err != nil {
		return err
	}

	for _, v := range krids {
		if v.KeyRangeID == keyRange.ID {
			// update req
			continue
		}
		raw := keyRange.Raw()
		eq := len(raw) == len(v.LowerBound)
		if eq {
			for i := 0; i < len(raw); i++ {
				eq = eq && bytes.Equal(raw[i], v.LowerBound[i])
			}
		}
		if eq {
			return fmt.Errorf("key range %v intersects with key range %v in QDB", keyRange.ID, v.KeyRangeID)
		}
	}

	return qdb.UpdateKeyRange(ctx, keyRange.ToDB())
}
