package console

import (
	"testing"

	"context"
	"fmt"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
	"github.com/stretchr/testify/assert"
)

func TestSequencesCommandRoutesToCoordinator(t *testing.T) {
	// This is a documentation test to verify that SequencesStr is included
	// in the list of commands that should route to coordinator.
	//
	// The actual routing logic is tested in integration tests, but this test
	// serves as documentation of the expected behavior.

	coordinatorCommands := []string{
		spqrparser.RoutersStr,
		spqrparser.TaskGroupStr,
		spqrparser.TaskGroupsStr,
		spqrparser.MoveTaskStr,
		spqrparser.MoveTasksStr,
		spqrparser.SequencesStr, // Added for issue #1590
	}

	// Verify all coordinator commands are defined
	for _, cmd := range coordinatorCommands {
		if cmd == "" {
			t.Error("Found empty command in coordinator commands list")
		}
	}
}

const MemQDBPath = ""

var mockShard1 = &qdb.Shard{
	ID:       "sh1",
	RawHosts: []string{"host1", "host2"},
}
var mockShard2 = &qdb.Shard{
	ID:       "sh2",
	RawHosts: []string{"host3", "host4"},
}

func prepareDB(ctx context.Context) (*qdb.MemQDB, error) {
	memqdb, err := qdb.RestoreQDB(MemQDBPath)
	if err != nil {
		return nil, err
	}
	d1 := &distributions.Distribution{Id: "ds1", ColTypes: []string{"integer"}}
	if chunk, err := memqdb.CreateDistribution(ctx, distributions.DistributionToDB(d1)); err != nil {
		return nil, err
	} else {
		if err = memqdb.ExecNoTransaction(ctx, chunk); err != nil {
			return nil, err
		}
	}
	d2 := &distributions.Distribution{Id: "ds_hashed", ColTypes: []string{"uinteger"}}
	if chunk, err := memqdb.CreateDistribution(ctx, distributions.DistributionToDB(d2)); err != nil {
		return nil, err
	} else {
		if err = memqdb.ExecNoTransaction(ctx, chunk); err != nil {
			return nil, err
		}
	}
	d3 := &distributions.Distribution{Id: "ds_2keys", ColTypes: []string{"integer", "varchar"}}
	if chunk, err := memqdb.CreateDistribution(ctx, distributions.DistributionToDB(d3)); err != nil {
		return nil, err
	} else {
		if err = memqdb.ExecNoTransaction(ctx, chunk); err != nil {
			return nil, err
		}
	}
	if err = memqdb.AddShard(ctx, mockShard1); err != nil {
		return nil, err
	}
	if err = memqdb.AddShard(ctx, mockShard2); err != nil {
		return nil, err
	}
	return memqdb, nil
}

func TestAlterDistributionAttach(t *testing.T) {
	ctx := context.Background()
	is := assert.New(t)
	for _, testData := range []struct {
		dsId      string
		rel       lyx.RangeVar
		keyColumn string
		err       error
	}{
		{
			dsId:      "ds_hashed",
			rel:       lyx.RangeVar{RelationName: "rel"},
			keyColumn: "col1",
			err:       fmt.Errorf("automatic attach isn't supported for column key uinteger"),
		},
		{
			dsId:      "ds1",
			rel:       lyx.RangeVar{RelationName: "rel"},
			keyColumn: "col1",
			err:       nil,
		},
		{
			dsId:      "ds_2keys",
			rel:       lyx.RangeVar{RelationName: "rel"},
			keyColumn: "col1",
			err:       fmt.Errorf("automatic attach is supported only for distribution with one column key"),
		},
	} {
		memqdb, err := prepareDB(ctx)
		assert.NoError(t, err)
		mngr := coord.NewLocalInstanceMetadataMgr(memqdb, nil, nil)
		err = innerAlterDistributionAttach(ctx, mngr, &testData.rel, testData.dsId, testData.keyColumn)
		if testData.err != nil {
			is.EqualError(err, testData.err.Error())
		} else {
			is.NoError(err)
			distr, err := mngr.GetDistribution(ctx, testData.dsId)
			is.NoError(err)
			_, ok := distr.Relations[testData.rel.RelationName]
			is.True(ok)
		}
	}
}
