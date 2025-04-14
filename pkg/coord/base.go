package coord

import (
	"context"
	"fmt"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/qdb/ops"
)

type BaseCoordinator struct {
	qdb qdb.QDB
}

func NewBaseCoordinator(qdb qdb.QDB) BaseCoordinator {
	return BaseCoordinator{
		qdb: qdb,
	}
}

// GetMoveTaskGroup retrieves the MoveTask group from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - *tasks.MoveTaskGroup: the retrieved task group, or nil if an error occurred.
// - error: an error if the retrieval process fails.
func (lc *BaseCoordinator) GetMoveTaskGroup(ctx context.Context) (*tasks.MoveTaskGroup, error) {
	group, err := lc.qdb.GetMoveTaskGroup(ctx)
	if err != nil {
		return nil, err
	}
	return tasks.TaskGroupFromDb(group), nil
}

// GetDistribution retrieves info about distribution from QDB
// TODO: unit tests
func (lc *BaseCoordinator) GetDistribution(ctx context.Context, id string) (*distributions.Distribution, error) {
	ret, err := lc.qdb.GetDistribution(ctx, id)
	if err != nil {
		return nil, err
	}
	ds := distributions.DistributionFromDB(ret)
	for relName := range ds.Relations {
		mapping, err := lc.qdb.GetRelationSequence(ctx, relName)
		if err != nil {
			return nil, err
		}
		ds.Relations[relName].ColumnSequenceMapping = mapping
	}
	return ds, nil
}

// GetRelationDistribution retrieves info about distribution attached to relation from QDB
// TODO: unit tests
func (lc *BaseCoordinator) GetRelationDistribution(ctx context.Context, relation string) (*distributions.Distribution, error) {
	ret, err := lc.qdb.GetRelationDistribution(ctx, relation)
	if err != nil {
		return nil, err
	}
	ds := distributions.DistributionFromDB(ret)
	for relName := range ds.Relations {
		mapping, err := lc.qdb.GetRelationSequence(ctx, relName)
		if err != nil {
			return nil, err
		}
		ds.Relations[relName].ColumnSequenceMapping = mapping
	}
	return ds, nil
}

func (lc *BaseCoordinator) ListShards(ctx context.Context) ([]*topology.DataShard, error) {
	resp, err := lc.qdb.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	var retShards []*topology.DataShard

	for _, sh := range resp {
		retShards = append(retShards, &topology.DataShard{
			ID: sh.ID,
			Cfg: &config.Shard{
				RawHosts: sh.RawHosts,
			},
		})
	}
	return retShards, nil
}

// TODO unit tests

// GetKeyRange gets key range by id
// GetKeyRange retrieves a key range identified by krId from the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): the context of the operation.
// - krId (string): the ID of the key range to retrieve.
//
// Returns:
// - *kr.KeyRange: the KeyRange object retrieved.
// - error: an error if the retrieval encounters any issues.
func (qc *BaseCoordinator) GetKeyRange(ctx context.Context, krId string) (*kr.KeyRange, error) {
	krDb, err := qc.qdb.GetKeyRange(ctx, krId)
	if err != nil {
		return nil, err
	}
	ds, err := qc.qdb.GetDistribution(ctx, krDb.DistributionId)
	if err != nil {
		return nil, err
	}
	return kr.KeyRangeFromDB(krDb, ds.ColTypes), nil
}

// TODO : unit tests

// ListKeyRanges retrieves a list of key ranges associated with the specified distribution from the LocalCoordinator.
//
// Parameters:
// - ctx: the context of the operation.
// - distribution: the distribution to filter the key ranges by.
//
// Returns:
// - []*kr.KeyRange: a slice of KeyRange objects retrieved.
// - error: an error if the retrieval encounters any issues.
func (qc *BaseCoordinator) ListKeyRanges(ctx context.Context, distribution string) ([]*kr.KeyRange, error) {
	keyRanges, err := qc.qdb.ListKeyRanges(ctx, distribution)
	if err != nil {
		return nil, err
	}

	keyr := make([]*kr.KeyRange, 0, len(keyRanges))
	for _, keyRange := range keyRanges {
		ds, err := qc.qdb.GetDistribution(ctx, keyRange.DistributionId)
		if err != nil {
			return nil, err
		}
		keyr = append(keyr, kr.KeyRangeFromDB(keyRange, ds.ColTypes))
	}

	return keyr, nil
}

// WriteMoveTaskGroup writes the given task group to the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - taskGroup (*tasks.MoveTaskGroup): the task group to be written to the QDB.
//
// Returns:
// - error: an error if the write operation fails.
func (qc *BaseCoordinator) WriteMoveTaskGroup(ctx context.Context, taskGroup *tasks.MoveTaskGroup) error {
	return qc.qdb.WriteMoveTaskGroup(ctx, tasks.TaskGroupToDb(taskGroup))
}

// RemoveMoveTaskGroup removes the task group from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - error: an error if the removal operation fails.
func (qc *BaseCoordinator) RemoveMoveTaskGroup(ctx context.Context) error {
	return qc.qdb.RemoveMoveTaskGroup(ctx)
}

// TODO : unit tests

// ListDistributions retrieves a list of distributions from the local coordinator's QDB.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
//
// Returns:
// - []*distributions.Distribution: a slice of distributions.Distribution objects representing the retrieved distributions.
// - error: an error if the retrieval operation fails.
func (qc *BaseCoordinator) ListDistributions(ctx context.Context) ([]*distributions.Distribution, error) {
	distrs, err := qc.qdb.ListDistributions(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]*distributions.Distribution, 0)
	for _, ds := range distrs {
		ret := distributions.DistributionFromDB(ds)
		for relName := range ds.Relations {
			mapping, err := qc.qdb.GetRelationSequence(ctx, relName)
			if err != nil {
				return nil, err
			}
			ret.Relations[relName].ColumnSequenceMapping = mapping
		}
		res = append(res, ret)
	}
	return res, nil
}

func (qc *BaseCoordinator) CreateDistribution(ctx context.Context, ds *distributions.Distribution) error {
	if len(ds.ColTypes) == 0 && ds.Id != distributions.REPLICATED {
		return fmt.Errorf("empty distributions are disallowed")
	}
	for _, rel := range ds.Relations {
		for colName, seqName := range rel.ColumnSequenceMapping {
			err := qc.qdb.AlterSequenceAttach(ctx, seqName, rel.Name, colName)
			if err != nil {
				return err
			}
		}
	}
	return qc.qdb.CreateDistribution(ctx, distributions.DistributionToDB(ds))
}

// TODO : unit tests

// AlterDistributionDetach alters the distribution by detaching a specific distributed relation.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - id (string): the ID of the distribution to be altered.
// - relName (string): the name of the distributed relation to be detached.
//
// Returns:
// - error: an error if the alteration operation fails.
func (qc *BaseCoordinator) AlterDistributionDetach(ctx context.Context, id string, relName string) error {
	return qc.qdb.AlterDistributionDetach(ctx, id, relName)
}

// ShareKeyRange shares a key range with the LocalCoordinator.
//
// Parameters:
// - id (string): The ID of the key range to be shared.
//
// Returns:
// - error: An error indicating the sharing status.
func (qc *BaseCoordinator) ShareKeyRange(id string) error {
	return qc.qdb.ShareKeyRange(id)
}

// CreateKeyRange creates a new key range in the LocalCoordinator.
//
// Parameters:
// - ctx (context.Context): The context of the operation.
// - kr (*kr.KeyRange): The key range object to be created.
//
// Returns:
// - error: An error if the creation encounters any issues.
func (lc *BaseCoordinator) CreateKeyRange(ctx context.Context, kr *kr.KeyRange) error {
	return ops.CreateKeyRangeWithChecks(ctx, lc.qdb, kr)
}

// TODO : unit tests

// LockKeyRange locks a key range identified by krid and returns the corresponding KeyRange object.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - krid (string): the ID of the key range to lock.
//
// Returns:
// - *kr.KeyRange: the locked KeyRange object.
// - error: an error if the lock operation encounters any issues.
func (qc *BaseCoordinator) LockKeyRange(ctx context.Context, keyRangeID string) (*kr.KeyRange, error) {
	keyRangeDB, err := qc.qdb.LockKeyRange(ctx, keyRangeID)
	if err != nil {
		return nil, err
	}
	ds, err := qc.qdb.GetDistribution(ctx, keyRangeDB.DistributionId)
	if err != nil {
		_ = qc.qdb.UnlockKeyRange(ctx, keyRangeID)
		return nil, err
	}

	return kr.KeyRangeFromDB(keyRangeDB, ds.ColTypes), nil
}

// TODO : unit tests

// UnlockKeyRange unlocks a key range identified by krid.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - krid (string): the ID of the key range to lock.
//
// Returns:
// - error: an error if the unlock operation encounters any issues.
func (qc *BaseCoordinator) UnlockKeyRange(ctx context.Context, keyRangeID string) error {
	return qc.qdb.UnlockKeyRange(ctx, keyRangeID)
}

// TODO : unit tests

// AlterDistributionAttach alters the distribution by attaching additional distributed relations.
//
// Parameters:
// - ctx (context.Context): the context.Context object for managing the request's lifetime.
// - id (string): the ID of the distribution to be altered.
// - rels ([]*distributions.DistributedRelation): the slice of distributions.DistributedRelation objects representing the relations to be attached.
//
// Returns:
// - error: an error if the alteration operation fails.
func (lc *BaseCoordinator) AlterDistributionAttach(ctx context.Context, id string, rels []*distributions.DistributedRelation) error {
	ds, err := lc.qdb.GetDistribution(ctx, id)
	if err != nil {
		return err
	}

	dRels := []*qdb.DistributedRelation{}
	for _, r := range rels {
		if len(r.DistributionKey) != len(ds.ColTypes) {
			return fmt.Errorf("cannot attach relation %v to this dataspace: number of column mismatch", r.Name)
		}
		if !r.ReplicatedRelation && len(r.ColumnSequenceMapping) > 0 {
			return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "sequence are supported for replicated relations only")
		}
		dRels = append(dRels, distributions.DistributedRelationToDB(r))
	}

	err = lc.qdb.AlterDistributionAttach(ctx, id, dRels)
	if err != nil {
		return err
	}

	for _, r := range rels {
		for colName, seqName := range r.ColumnSequenceMapping {
			if err := lc.qdb.AlterSequenceAttach(ctx, seqName, r.Name, colName); err != nil {
				return err
			}
		}
	}
	return nil
}

// TODO : unit tests

// Unite merges two key ranges identified by req.BaseKeyRangeId and req.AppendageKeyRangeId into a single key range.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - req (*kr.UniteKeyRange): a pointer to a UniteKeyRange object containing the necessary information for the unite operation.
//
// Returns:
// - error: an error if the unite operation encounters any issues.
func (qc *BaseCoordinator) Unite(ctx context.Context, uniteKeyRange *kr.UniteKeyRange) error {
	krBaseDb, err := qc.qdb.LockKeyRange(ctx, uniteKeyRange.BaseKeyRangeId)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.qdb.UnlockKeyRange(ctx, uniteKeyRange.BaseKeyRangeId); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	/*
		krAppendageDb, err := qc.qdb.LockKeyRange(ctx, uniteKeyRange.AppendageKeyRangeId)
		if err != nil {
			return err
		}

		defer func() {
			if err := qc.qdb.UnlockKeyRange(ctx, uniteKeyRange.AppendageKeyRangeId); err != nil {
				spqrlog.Zero.Error().Err(err).Msg("")
			}
		}()
	*/

	ds, err := qc.qdb.GetDistribution(ctx, krBaseDb.DistributionId)
	if err != nil {
		return err
	}

	krAppendageDb, err := qc.qdb.GetKeyRange(ctx, uniteKeyRange.AppendageKeyRangeId)
	if err != nil {
		return err
	}

	krBase := kr.KeyRangeFromDB(krBaseDb, ds.ColTypes)
	krAppendage := kr.KeyRangeFromDB(krAppendageDb, ds.ColTypes)

	if krBase.ShardID != krAppendage.ShardID {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges routing different shards")
	}
	if krBase.Distribution != krAppendage.Distribution {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite key ranges of different distributions")
	}
	// TODO: check all types when composite keys are supported
	krLeft, krRight := krBase, krAppendage
	if kr.CmpRangesLess(krRight.LowerBound, krLeft.LowerBound, ds.ColTypes) {
		krLeft, krRight = krRight, krLeft
	}

	krs, err := qc.ListKeyRanges(ctx, ds.ID)
	if err != nil {
		return err
	}
	for _, kRange := range krs {
		if kRange.ID != krLeft.ID &&
			kRange.ID != krRight.ID &&
			kr.CmpRangesLessEqual(krLeft.LowerBound, kRange.LowerBound, ds.ColTypes) &&
			kr.CmpRangesLessEqual(kRange.LowerBound, krRight.LowerBound, ds.ColTypes) {
			return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to unite non-adjacent key ranges")
		}
	}

	if err := qc.qdb.DropKeyRange(ctx, krAppendage.ID); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to drop an old key range: %s", err.Error())
	}

	if krLeft.ID != krBase.ID {
		krBase.LowerBound = krAppendage.LowerBound
	}

	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.qdb, krBase); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to update a new key range: %s", err.Error())
	}
	return nil
}

// Caller should lock key range
// TODO : unit tests

// Split splits an existing key range identified by req.SourceID into two new key ranges.
//
// Parameters:
// - ctx (context.Context): The context.Context object for managing the request's lifetime.
// - req (*kr.SplitKeyRange): a pointer to a SplitKeyRange object containing the necessary information for the split operation.
//
// Returns:
// - error: an error if the split operation encounters any issues.
func (qc *BaseCoordinator) Split(ctx context.Context, req *kr.SplitKeyRange) error {
	spqrlog.Zero.Debug().
		Str("krid", req.Krid).
		Interface("bound", req.Bound).
		Str("source-id", req.SourceID).
		Msg("split request is")

	if _, err := qc.qdb.GetKeyRange(ctx, req.Krid); err == nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v already present in qdb", req.Krid)
	}

	krOldDB, err := qc.qdb.LockKeyRange(ctx, req.SourceID)
	if err != nil {
		return err
	}

	defer func() {
		if err := qc.qdb.UnlockKeyRange(ctx, req.SourceID); err != nil {
			spqrlog.Zero.Error().Err(err).Msg("")
		}
	}()

	ds, err := qc.qdb.GetDistribution(ctx, krOldDB.DistributionId)

	if err != nil {
		return err
	}

	krOld := kr.KeyRangeFromDB(krOldDB, ds.ColTypes)

	eph := kr.KeyRangeFromBytes(req.Bound, ds.ColTypes)

	if kr.CmpRangesEqual(krOld.LowerBound, eph.LowerBound, ds.ColTypes) {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound equals lower of the key range")
	}

	if kr.CmpRangesLess(eph.LowerBound, krOld.LowerBound, ds.ColTypes) {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound is out of key range")
	}

	krs, err := qc.ListKeyRanges(ctx, ds.ID)
	if err != nil {
		return err
	}
	for _, kRange := range krs {
		if kr.CmpRangesLess(krOld.LowerBound, kRange.LowerBound, ds.ColTypes) && kr.CmpRangesLessEqual(kRange.LowerBound, eph.LowerBound, ds.ColTypes) {
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to split because bound intersects with \"%s\" key range", kRange.ID)
		}
	}

	krNew := kr.KeyRangeFromDB(
		&qdb.KeyRange{
			// fix multidim case
			LowerBound: func() [][]byte {
				if req.SplitLeft {
					return krOld.Raw()
				}
				return req.Bound
			}(),
			KeyRangeID:     req.Krid,
			ShardID:        krOld.ShardID,
			DistributionId: krOld.Distribution,
		},
		ds.ColTypes,
	)

	spqrlog.Zero.Debug().
		Bytes("lower-bound", krNew.Raw()[0]).
		Str("shard-id", krNew.ShardID).
		Str("id", krNew.ID).
		Msg("new key range")

	if req.SplitLeft {
		krOld.LowerBound = kr.KeyRangeFromBytes(req.Bound, ds.ColTypes).LowerBound
	}

	if err := ops.ModifyKeyRangeWithChecks(ctx, qc.qdb, krOld); err != nil {
		return err
	}

	if err := ops.CreateKeyRangeWithChecks(ctx, qc.qdb, krNew); err != nil {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "failed to add a new key range: %s", err.Error())
	}
	return nil
}

func (bc *BaseCoordinator) ListSequences(ctx context.Context) ([]string, error) {
	return bc.qdb.ListSequences(ctx)
}
