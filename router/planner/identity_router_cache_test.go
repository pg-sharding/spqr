package planner_test

import (
	"context"
	"sync"
	"testing"

	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/models/sequences"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/stretchr/testify/assert"
)

const MemQDBPath = "memqdb.json"

func TestStepOne(t *testing.T) {
	assert := assert.New(t)
	db, _ := qdb.NewMemQDB(MemQDBPath)
	ctx := context.TODO()
	err := db.CreateSequence(ctx, "testSeq", 0)
	assert.NoError(err)

	_ = db.CreateReferenceRelation(context.TODO(), &qdb.ReferenceRelation{
		TableName: "test_ref_rel",
	})

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)
	var seqMngr sequences.SequenceMgr = lc
	identityMgr := planner.NewIdentityRouterCache(1, &seqMngr)
	actualNext, err := identityMgr.NextVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(1), actualNext)
	actualNext, err = identityMgr.NextVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(2), actualNext)
	actualNext, err = identityMgr.NextVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(3), actualNext)
	actualCurrent, err := lc.CurrVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(3), actualCurrent)
}

func TestStepFive(t *testing.T) {
	assert := assert.New(t)
	db, _ := qdb.NewMemQDB(MemQDBPath)
	ctx := context.TODO()
	err := db.CreateSequence(ctx, "testSeq", 0)
	assert.NoError(err)

	_ = db.CreateReferenceRelation(context.TODO(), &qdb.ReferenceRelation{
		TableName: "test_ref_rel",
	})

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)
	var seqMngr sequences.SequenceMgr = lc
	identityMgr := planner.NewIdentityRouterCache(5, &seqMngr)
	actualNext, err := identityMgr.NextVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(1), actualNext)
	actualNext, err = identityMgr.NextVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(2), actualNext)
	actualNext, err = identityMgr.NextVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(3), actualNext)
	actualCurrent, err := lc.CurrVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(5), actualCurrent)

	actualNext, err = identityMgr.NextVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(4), actualNext)
	actualNext, err = identityMgr.NextVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(5), actualNext)
	actualCurrent, err = lc.CurrVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(5), actualCurrent)

	actualNext, err = identityMgr.NextVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(6), actualNext)
	actualCurrent, err = lc.CurrVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(10), actualCurrent)

}

func TestStepOne_concurrent(t *testing.T) {
	assert := assert.New(t)
	ctx := context.TODO()

	db, _ := qdb.NewMemQDB(MemQDBPath)
	err := db.CreateSequence(ctx, "testSeq", 0)
	assert.NoError(err)

	_ = db.CreateReferenceRelation(context.TODO(), &qdb.ReferenceRelation{
		TableName: "test_ref_rel",
	})

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)
	var seqMngr sequences.SequenceMgr = lc
	identityMgr := planner.NewIdentityRouterCache(1, &seqMngr)

	var wg sync.WaitGroup
	const goroutines = 10
	const increments = 1000
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < increments; j++ {
				_, err := identityMgr.NextVal(ctx, "testSeq")
				assert.NoError(err)
			}
		}()
	}

	wg.Wait()

	// Verify final value
	expectedValue := int64(goroutines*increments + 1)
	actual, err := identityMgr.NextVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(expectedValue, actual)
}

func TestStepFive_concurrent(t *testing.T) {
	assert := assert.New(t)
	ctx := context.TODO()

	db, _ := qdb.NewMemQDB(MemQDBPath)
	err := db.CreateSequence(ctx, "testSeq", 0)
	assert.NoError(err)

	_ = db.CreateReferenceRelation(context.TODO(), &qdb.ReferenceRelation{
		TableName: "test_ref_rel",
	})

	lc := coord.NewLocalInstanceMetadataMgr(db, nil)
	var seqMngr sequences.SequenceMgr = lc
	identityMgr := planner.NewIdentityRouterCache(5, &seqMngr)

	var wg sync.WaitGroup
	const goroutines = 10
	const increments = 1000
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < increments; j++ {
				_, err := identityMgr.NextVal(ctx, "testSeq")
				assert.NoError(err)
			}
		}()
	}

	wg.Wait()

	// Verify final value
	expectedValue := int64(goroutines*increments + 1)
	actual, err := identityMgr.NextVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(expectedValue, actual)
	actualCurrent, err := lc.CurrVal(ctx, "testSeq")
	assert.NoError(err)
	assert.Equal(int64(10005), actualCurrent)
}
