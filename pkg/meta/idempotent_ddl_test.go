package meta_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/rfqn"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/stretchr/testify/assert"
)

// runMigration parses a (possibly multi-statement) SQL string and executes every
// statement through the metadata command processor, returning the first error.
// It mirrors how spqrmigrate applies a .sql migration file linearly.
func runMigration(ctx context.Context, mngr meta.EntityMgr, sql string) error {
	stmts, err := spqrparser.Parse(sql)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		if stmt == nil {
			continue
		}
		if _, err := meta.ProcMetadataCommand(ctx, stmt, mngr, nil, nil, nil, false, nil); err != nil {
			return err
		}
	}
	return nil
}

// The up- and down-migrations mirror the example from the task description:
// every command is self-contained idempotent via IF [NOT] EXISTS, so applying a
// file twice must yield the same result with a successful (nil) error.
const idempotentUpMigration = `
CREATE DISTRIBUTION IF NOT EXISTS ds_notify COLUMN TYPES varchar;
ALTER DISTRIBUTION ds_notify ATTACH RELATION IF NOT EXISTS notifications_i18n DISTRIBUTION KEY id;
CREATE KEY RANGE IF NOT EXISTS krid_notify_0 FROM 'a' ROUTE TO sh1 FOR DISTRIBUTION ds_notify;
CREATE REFERENCE TABLE IF NOT EXISTS ref_notify;
`

const idempotentDownMigration = `
DROP KEY RANGE IF EXISTS krid_notify_0;
ALTER DISTRIBUTION ds_notify DETACH RELATION IF EXISTS notifications_i18n;
DROP REFERENCE TABLE IF EXISTS ref_notify;
DROP DISTRIBUTION IF EXISTS ds_notify;
`

// TestIdempotentDDLReRunnable verifies the core acceptance criterion: applying
// the same migration file more than once succeeds (exit code 0) and the
// previously-applied commands are silently skipped instead of failing with
// "already exists" / "does not exist".
func TestIdempotentDDLReRunnable(t *testing.T) {
	ctx := context.Background()
	memqdb, err := prepareDB(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	// Apply up-migration twice: both runs must succeed.
	assert.NoError(t, runMigration(ctx, mngr, idempotentUpMigration), "first up-migration must succeed")
	assert.NoError(t, runMigration(ctx, mngr, idempotentUpMigration), "re-running up-migration must be a no-op success")

	// The objects must exist exactly once after the (repeated) up-migration.
	ds, err := mngr.GetDistribution(ctx, "ds_notify")
	assert.NoError(t, err)
	assert.NotNil(t, ds.GetRelation(&rfqn.RelationFQN{RelationName: "notifications_i18n"}), "relation must stay attached")

	_, err = mngr.GetKeyRange(ctx, "krid_notify_0")
	assert.NoError(t, err, "key range must exist")

	_, err = mngr.GetReferenceRelation(ctx, &rfqn.RelationFQN{RelationName: "ref_notify"})
	assert.NoError(t, err, "reference relation must exist")

	// Apply down-migration twice: both runs must succeed.
	assert.NoError(t, runMigration(ctx, mngr, idempotentDownMigration), "first down-migration must succeed")
	assert.NoError(t, runMigration(ctx, mngr, idempotentDownMigration), "re-running down-migration must be a no-op success")

	// Everything must be gone after the down-migration.
	_, err = mngr.GetDistribution(ctx, "ds_notify")
	assert.Error(t, err, "distribution must be dropped")
	_, err = mngr.GetKeyRange(ctx, "krid_notify_0")
	assert.Error(t, err, "key range must be dropped")
	_, err = mngr.GetReferenceRelation(ctx, &rfqn.RelationFQN{RelationName: "ref_notify"})
	assert.Error(t, err, "reference relation must be dropped")
}

// TestAttachRelationIfNotExistsMixed verifies that ATTACH RELATION IF NOT EXISTS
// with several relations attaches only the ones that are not yet attached, while
// silently skipping those that already are.
func TestAttachRelationIfNotExistsMixed(t *testing.T) {
	ctx := context.Background()
	memqdb, err := prepareDB(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	assert.NoError(t, runMigration(ctx, mngr, "CREATE DISTRIBUTION ds_mix COLUMN TYPES varchar;"))
	assert.NoError(t, runMigration(ctx, mngr, "ALTER DISTRIBUTION ds_mix ATTACH RELATION t1 DISTRIBUTION KEY id;"))

	// t1 is already attached, t2 is new: the statement must succeed and attach t2.
	assert.NoError(t, runMigration(ctx, mngr,
		"ALTER DISTRIBUTION ds_mix ATTACH RELATION IF NOT EXISTS t1 DISTRIBUTION KEY id RELATION IF NOT EXISTS t2 DISTRIBUTION KEY id;"))

	ds, err := mngr.GetDistribution(ctx, "ds_mix")
	assert.NoError(t, err)
	assert.NotNil(t, ds.GetRelation(&rfqn.RelationFQN{RelationName: "t1"}), "t1 must stay attached")
	assert.NotNil(t, ds.GetRelation(&rfqn.RelationFQN{RelationName: "t2"}), "t2 must be attached")
}

// TestNonIdempotentDDLStillFails verifies that without the IF [NOT] EXISTS flags
// the historical behaviour is preserved: re-creating or dropping-missing fails.
func TestNonIdempotentDDLStillFails(t *testing.T) {
	ctx := context.Background()
	memqdb, err := prepareDB(ctx)
	assert.NoError(t, err)
	mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil, topology.TopMgrFromMap(map[string]*topology.DataShard{}), false, nil, qdb.DefaultMaxTxnSize)

	// CREATE DISTRIBUTION without IF NOT EXISTS: second attempt fails.
	assert.NoError(t, runMigration(ctx, mngr, "CREATE DISTRIBUTION ds_plain COLUMN TYPES varchar;"))
	assert.Error(t, runMigration(ctx, mngr, "CREATE DISTRIBUTION ds_plain COLUMN TYPES varchar;"),
		"re-creating a distribution without IF NOT EXISTS must fail")

	// DROP DISTRIBUTION without IF EXISTS on a missing id must fail.
	assert.Error(t, runMigration(ctx, mngr, "DROP DISTRIBUTION ds_missing;"),
		"dropping a missing distribution without IF EXISTS must fail")
}
