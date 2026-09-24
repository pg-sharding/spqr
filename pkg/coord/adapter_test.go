package coord_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/pg-sharding/spqr/coordinator/mock"
	"github.com/pg-sharding/spqr/coordinator/provider"
	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestAdapterPreservesMetadataError(t *testing.T) {
	ctx := context.Background()
	original := spqrerror.DistributionNotFound("ds").Detail("distribution lookup failed")
	wrapped := fmt.Errorf("coordinator: %w", original)
	mgr := mock.NewMockCoordinator(gomock.NewController(t))
	mgr.EXPECT().GetDistribution(gomock.Any(), "ds").Return(nil, wrapped)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(spqrerror.UnaryServerInterceptor))
	proto.RegisterDistributionServiceServer(server, provider.NewDistributionServer(mgr))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///coordinator", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	_, err = coord.NewAdapter(conn, qdb.DefaultMaxTxnSize).GetDistribution(ctx, "ds")
	var spErr *spqrerror.SpqrError
	require.ErrorAs(t, err, &spErr)
	assert.Equal(t, original.ErrorCode, spErr.ErrorCode)
	assert.Equal(t, wrapped.Error(), spErr.Error())
	assert.Equal(t, original.ErrHint, spErr.ErrHint)
	assert.Equal(t, original.ErrDetail, spErr.ErrDetail)
}
