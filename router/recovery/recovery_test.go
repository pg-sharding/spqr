package recovery_test

import (
	"context"
	"testing"
	"time"

	"go.uber.org/mock/gomock"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/qdb"
	mockqdb "github.com/pg-sharding/spqr/qdb/mock"
	"github.com/pg-sharding/spqr/router/recovery"
	"github.com/stretchr/testify/assert"
)

func TestCleanUpOldTXs(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ttl := time.Hour
	config.RouterConfig().TxDataTTL = ttl

	now := time.Now()
	stale := now.Add(-2 * ttl) 
	fresh := now


	txs := map[string]*qdb.TwoPCInfo{
		// remove
		"gid-committed-stale": {Gid: "gid-committed-stale", State: qdb.TwoPhaseP2, UpdatedAt: stale},
		"gid-rejected-stale":  {Gid: "gid-rejected-stale", State: qdb.TwoPhaseP2Rejected, UpdatedAt: stale},

		// keep
		"gid-committed-fresh": {Gid: "gid-committed-fresh", State: qdb.TwoPhaseP2, UpdatedAt: fresh},

		// keep
		"gid-init-stale": {Gid: "gid-init-stale", State: qdb.TwoPhaseInitState, UpdatedAt: stale},
		"gid-p1-stale":   {Gid: "gid-p1-stale", State: qdb.TwoPhaseP1, UpdatedAt: stale},
	}

	d := mockqdb.NewMockXDCStateKeeper(ctrl)
	d.EXPECT().GetTXs(gomock.Any()).Return(txs, nil)

	// Only the two finalized+stale GIDs must be removed.
	removed := map[string]bool{}
	d.EXPECT().RemoveTXData(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, gid string) error {
			removed[gid] = true
			return nil
		}).AnyTimes()

	wd := recovery.NewTwoPCWatchDogForTest(d)

	res, err := wd.CleanUpOldTXs(context.Background())
	assert.NoError(err)

	assert.ElementsMatch([]string{"gid-committed-stale", "gid-rejected-stale"}, res)
	assert.Equal(map[string]bool{
		"gid-committed-stale": true,
		"gid-rejected-stale":  true,
	}, removed)
}
