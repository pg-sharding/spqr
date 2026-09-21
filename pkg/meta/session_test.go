package meta_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/pkg/meta"
	mockmgr "github.com/pg-sharding/spqr/pkg/mock/meta"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestSessionCommit(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := mockmgr.NewMockEntityMgr(ctrl)
	sess := meta.NewConsoleSession(mgr)

	mgr.EXPECT().Snapshot().Return(mgr)
	mgr.EXPECT().Commit(gomock.Any()).Return(nil)

	err := sess.Begin(context.TODO())
	assert.NoError(err)

	err = sess.Commit(context.TODO())
	assert.NoError(err)
}

func TestSessionRollback(t *testing.T) {
	assert := assert.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := mockmgr.NewMockEntityMgr(ctrl)
	sess := meta.NewConsoleSession(mgr)

	mgr.EXPECT().Snapshot().Return(mgr)
	mgr.EXPECT().Rollback(gomock.Any()).Return(nil)

	err := sess.Begin(context.TODO())
	assert.NoError(err)

	err = sess.Rollback(context.TODO())
	assert.NoError(err)
}
