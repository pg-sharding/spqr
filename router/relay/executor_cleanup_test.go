package relay

import (
	"errors"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	mockpool "github.com/pg-sharding/spqr/pkg/mock/pool"
	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/pool"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/session/sessiontest"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestCleanupConnectionFailureDiscardsConn(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)

	prev := config.RouterConfig().ForceConnectionCleanup
	config.RouterConfig().ForceConnectionCleanup = false
	t.Cleanup(func() {
		config.RouterConfig().ForceConnectionCleanup = prev
	})

	pinned := true
	cl := mockcl.NewMockRouterClient(ctrl)
	cl.EXPECT().FindBoolGUC(session.SPQR_SESSION_CONNECTIONS_PIN).AnyTimes().
		Return(sessiontest.MustFindBoolGUC(session.SPQR_SESSION_CONNECTIONS_PIN), nil)
	cl.EXPECT().ResolveVirtualBoolParam(session.SPQR_SESSION_CONNECTIONS_PIN, false).AnyTimes().
		DoAndReturn(func(string, bool) bool {
			return pinned
		})
	cl.EXPECT().Rule().AnyTimes().Return(&config.FrontendRule{})

	s := NewQueryStateExecutor(nil, nil, nil, cl).(*QueryStateExecutorImpl)

	p := mockpool.NewMockMultiShardTSAPool(ctrl)
	cached := mockshard.NewMockShardHostInstance(ctrl)
	cached.EXPECT().ShardKeyName().Return("sh1").AnyTimes()
	require.NoError(s.CleanupConnection(p, cached))

	pinned = false

	cleanupErr := errors.New("cleanup failed")
	other := mockshard.NewMockShardHostInstance(ctrl)
	other.EXPECT().ShardKeyName().Return("sh1").AnyTimes()
	other.EXPECT().Sync().Return(int64(0))
	other.EXPECT().Cleanup(gomock.Any()).Return(cleanupErr)
	p.EXPECT().Discard(other).Return(nil)

	require.ErrorIs(s.CleanupConnection(p, other), cleanupErr)

	/* Discarding a failed connection must not drop a different pinned one. */
	pinned = true
	got, err := s.ConnectionWithTSA(pool.ConnAllocParams{}, kr.ShardKey{Name: "sh1"})
	require.NoError(err)
	require.Same(cached, got)
}
