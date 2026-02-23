package provider_test

import (
	"context"
	"testing"

	"github.com/pg-sharding/spqr/coordinator/mock"
	keyrangesProvider "github.com/pg-sharding/spqr/coordinator/provider"
	proto "github.com/pg-sharding/spqr/pkg/protos"
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

	coordinator.EXPECT().UnlockKeyRange(ctx, "kr-1").Return(nil)
	coordinator.EXPECT().UnlockKeyRange(ctx, "kr-2").Return(nil)
	coordinator.EXPECT().UnlockKeyRange(ctx, "kr-3").Return(nil)

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
