package meta_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/meta"
	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	mtran "github.com/pg-sharding/spqr/pkg/models/transaction"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestSessionCommit(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := mockmgr.NewMockEntityMgr(ctrl)
	snapMgr := mockmgr.NewMockEntityMgr(ctrl)
	sess := meta.NewConsoleSession(mgr)

	expectedXRecords := make([]*mtran.XRecord, 0)

	mgr.EXPECT().Snapshot().Return(snapMgr)
	snapMgr.EXPECT().Begin(gomock.Any()).Return(nil)
	snapMgr.EXPECT().Commit(gomock.Any()).Return(nil)
	snapMgr.EXPECT().XRecords().Return(expectedXRecords)
	mgr.EXPECT().ApplyXRecords(gomock.Any(), expectedXRecords).Return(nil)

	err := sess.Begin(context.TODO())
	assert.NoError(err)

	err = sess.Commit(context.TODO())
	assert.NoError(err)

	assert.False(sess.IsInTx())
}

func TestSessionRollback(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := mockmgr.NewMockEntityMgr(ctrl)
	snapMgr := mockmgr.NewMockEntityMgr(ctrl)
	sess := meta.NewConsoleSession(mgr)

	mgr.EXPECT().Snapshot().Return(snapMgr)
	snapMgr.EXPECT().Begin(gomock.Any()).Return(nil)
	snapMgr.EXPECT().Rollback(gomock.Any()).Return(nil)

	err := sess.Begin(context.TODO())
	assert.NoError(err)

	err = sess.Rollback(context.TODO())
	assert.NoError(err)

	assert.False(sess.IsInTx())
}
