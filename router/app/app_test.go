package app

import (
	"context"
	"net"
	"testing"

	"github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	protos "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	rgrpc "github.com/pg-sharding/spqr/router/grpc"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCPreservesMetadataError(t *testing.T) {
	db, err := qdb.NewMemQDB("")
	require.NoError(t, err)
	mgr := coord.NewCoordinator(db, nil, qdb.DefaultMaxTxnSize)
	app, err := NewApp(nil)
	require.NoError(t, err)
	server := grpc.NewServer(app.grpcServerOptions...)
	rgrpc.Register(server, nil, &mgr, nil)
	listener := bufconn.Listen(1024 * 1024)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///router", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	name := &rfqn.RelationFQN{SchemaName: "sales", RelationName: "countries"}
	_, directErr := mgr.GetReferenceRelation(t.Context(), name)
	var original *spqrerror.SpqrError
	require.ErrorAs(t, directErr, &original)
	_, err = protos.NewReferenceRelationsServiceClient(conn).GetReferenceRelation(t.Context(), rfqn.RelationFQNToProto(name))
	var spErr *spqrerror.SpqrError
	require.ErrorAs(t, spqrerror.CleanGrpcError(err), &spErr)
	assert.Equal(t, original.ErrorCode, spErr.ErrorCode)
	assert.Equal(t, original.Error(), spErr.Error())
	assert.Equal(t, original.ErrHint, spErr.ErrHint)
}
