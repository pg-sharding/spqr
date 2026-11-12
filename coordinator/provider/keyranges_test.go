package provider_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/coordinator/mock"
	keyrangesProvider "github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	qdbmock "github.com/pg-sharding/spqr/qdb/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestUnlockKeyRange_Success_UnlocksAllIds(t *testing.T) {
	assertions := assert.New(t)

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	coordinator := mock.NewMockCoordinator(ctrl)
	keyRangeService := keyrangesProvider.NewKeyRangeService(coordinator)

	ctx := context.Background()
	req := &proto.UnlockKeyRangeRequest{Id: []string{"kr-1", "kr-2", "kr-3"}}

	qdbMock := qdbmock.NewMockXQDB(ctrl)
	coordinator.EXPECT().QDB().Return(qdbMock).AnyTimes()
	for _, id := range req.Id {
		coordinator.EXPECT().GetKeyRange(ctx, id).Return(&kr.KeyRange{ID: id, IsLocked: true}, nil)
		qdbMock.EXPECT().LockKeyRangeOps(ctx, id).Return(&qdb.KeyRange{KeyRangeID: id}, nil)
		qdbMock.EXPECT().ListKeyRangeMoves(ctx).Return(nil, nil)
		coordinator.EXPECT().UnlockKeyRange(ctx, id).Return(nil)
		qdbMock.EXPECT().UnlockKeyRangeOps(ctx, id).Return(nil)
	}

	res, err := keyRangeService.UnlockKeyRange(ctx, req)

	assertions.NoError(err)
	assertions.NotNil(res)
	assertions.IsType(&proto.ModifyReply{}, res)
}

func TestUnlockKeyRange_ReturnsError_StopsOnFirstFailure(t *testing.T) {
	assertions := assert.New(t)

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	coordinator := mock.NewMockCoordinator(ctrl)
	keyRangeService := keyrangesProvider.NewKeyRangeService(coordinator)

	ctx := context.Background()
	req := &proto.UnlockKeyRangeRequest{Id: []string{"kr-1", "kr-2", "kr-3"}}

	qdbMock := qdbmock.NewMockXQDB(ctrl)
	coordinator.EXPECT().QDB().Return(qdbMock).AnyTimes()
	for _, id := range req.Id[:2] {
		coordinator.EXPECT().GetKeyRange(ctx, id).Return(&kr.KeyRange{ID: id, IsLocked: true}, nil)
		qdbMock.EXPECT().LockKeyRangeOps(ctx, id).Return(&qdb.KeyRange{KeyRangeID: id}, nil)
		qdbMock.EXPECT().ListKeyRangeMoves(ctx).Return(nil, nil)
		qdbMock.EXPECT().UnlockKeyRangeOps(ctx, id).Return(nil)
	}
	coordinator.EXPECT().UnlockKeyRange(ctx, "kr-1").Return(nil)
	coordinator.EXPECT().UnlockKeyRange(ctx, "kr-2").Return(assert.AnError)

	res, err := keyRangeService.UnlockKeyRange(ctx, req)

	assertions.ErrorIs(err, assert.AnError)
	assertions.Zero(res)
}

func TestUnlockKeyRange_EmptyIdList(t *testing.T) {
	assertions := assert.New(t)

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	coordinator := mock.NewMockCoordinator(ctrl)
	keyRangeService := keyrangesProvider.NewKeyRangeService(coordinator)

	ctx := context.Background()
	req := &proto.UnlockKeyRangeRequest{Id: []string{}}

	res, err := keyRangeService.UnlockKeyRange(ctx, req)

	assertions.NoError(err)
	assertions.NotNil(res)
	assertions.IsType(&proto.ModifyReply{}, res)
}
