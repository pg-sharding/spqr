package recovery_test

import (
	"context"
	"fmt"
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

		// update, then remove
		"gid-init-stale": {Gid: "gid-init-stale", State: qdb.TwoPhaseInitState, UpdatedAt: stale},
		"gid-p1-stale":   {Gid: "gid-p1-stale", State: qdb.TwoPhaseP1, UpdatedAt: stale},

		// keep
		"gid-committed-fresh": {Gid: "gid-committed-fresh", State: qdb.TwoPhaseP2, UpdatedAt: fresh},
		"gid-rejected-fresh":  {Gid: "gid-rejected-fresh", State: qdb.TwoPhaseP2Rejected, UpdatedAt: fresh},
		"gid-init-fresh":      {Gid: "gid-init-fresh", State: qdb.TwoPhaseInitState, UpdatedAt: fresh},
		"gid-p1-fresh":        {Gid: "gid-p1-fresh", State: qdb.TwoPhaseP1, UpdatedAt: fresh},
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
			return gids, nil
		}).AnyTimes()
	d.EXPECT().AcquireTxOwnership(gomock.Any(), "gid-init-stale", false).Return(true, nil).AnyTimes()
	d.EXPECT().AcquireTxOwnership(gomock.Any(), "gid-p1-stale", false).Return(true, nil).AnyTimes()
	d.EXPECT().ReleaseTxOwnership(gomock.Any(), "gid-init-stale").Return(nil).AnyTimes()
	d.EXPECT().ReleaseTxOwnership(gomock.Any(), "gid-p1-stale").Return(nil).AnyTimes()
	d.EXPECT().TXStatus(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, gid string) (qdb.TwoPhaseTxState, error) {
		tx, ok := txs[gid]
		if !ok {
			return qdb.TwoPhaseTxState(""), fmt.Errorf("tx not found")
		}
		return tx.State, nil
	}).AnyTimes()
	d.EXPECT().TXCohortShards(gomock.Any(), gomock.AnyOf("gid-p1-stale", "gid-init-stale")).Return([]string{"sh1", "sh2"}, nil).AnyTimes()

	wd := recovery.NewTwoPCWatchDogForTest(d)

	res, err := wd.CleanUpOldTXs(context.Background())
	assert.NoError(err)

	wantCandidates := map[string]bool{
		"gid-committed-stale": true,
		"gid-rejected-stale":  true,
		/* won't finalize bc of empty backend rules
		"gid-init-stale":      true,
		"gid-p1-stale":        true,
		*/
	}

	assert.ElementsMatch([]string{"gid-committed-stale", "gid-rejected-stale" /* "gid-init-stale", "gid-p1-stale" */}, res)
	assert.Equal(wantCandidates, candidates)

	for k := range wantCandidates {
		delete(txs, k)
	}

	res, err = wd.CleanUpOldTXs(context.Background())
	assert.NoError(err)
	assert.Empty(res)
}
