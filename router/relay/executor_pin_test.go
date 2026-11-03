package relay

import (
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

func TestPinnedConnCloseAfterUnpinDoesNotPutTwice(t *testing.T) {
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
	sh := mockshard.NewMockShardHostInstance(ctrl)
	sh.EXPECT().ShardKeyName().Return("sh1").AnyTimes()
	sh.EXPECT().Sync().Return(int64(0))
	sh.EXPECT().Cleanup(gomock.Any()).Return(nil)
	p.EXPECT().Put(sh).Return(nil).Times(1)

	require.NoError(s.CleanupConnection(p, sh))

	got, err := s.ConnectionWithTSA(pool.ConnAllocParams{}, kr.ShardKey{Name: "sh1"})
	require.NoError(err)
	require.Same(sh, got)

	pinned = false
	require.NoError(s.CleanupConnection(p, got))
	s.Close()
}
