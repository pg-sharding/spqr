package recovery_test

import (
	"context"
	"testing"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/pg-sharding/spqr/qdb/mock"
	"github.com/pg-sharding/spqr/router/recovery"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
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

	d := mock.NewMockXDCStateKeeper(ctrl)
	d.EXPECT().TXInfos(gomock.Any()).DoAndReturn(func(_ context.Context) ([]qdb.TwoPCInfo, error) {
		res := make([]qdb.TwoPCInfo, 0, len(txs))
		for _, tx := range txs {
			txCopy := *tx
			res = append(res, txCopy)
		}
		return res, nil
	}).AnyTimes()

	candidates := map[string]bool{}
	d.EXPECT().ClearTxStatuses(gomock.Any(), false, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ bool, gids []string) ([]string, error) {
			for _, gid := range gids {
				candidates[gid] = true
			}
			// Another cleanup can delete a GID before this call reaches storage.
			return []string{"gid-committed-stale"}, nil
		}).Times(1)

	wd := recovery.NewTwoPCWatchDogForTest(d)

	res, err := wd.CleanUpOldTXs(context.Background())
	assert.NoError(err)

	wantCandidates := map[string]bool{
		"gid-committed-stale": true,
		"gid-rejected-stale":  true,
	}

	assert.Equal([]string{"gid-committed-stale"}, res)
	assert.Equal(wantCandidates, candidates)

	for k := range wantCandidates {
		delete(txs, k)
	}

	res, err = wd.CleanUpOldTXs(context.Background())
	assert.NoError(err)
	assert.Empty(res)
}
