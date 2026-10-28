package coord

import (
	"context"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (lc *Coordinator) SetMigration(ctx context.Context, name, value string) error {
	return lc.qdb.SetMigration(ctx, name, value)
}

func (lc *Coordinator) ResetMigration(ctx context.Context, name string) error {
	return lc.qdb.ResetMigration(ctx, name)
}

func (lc *Coordinator) ListMigrations(ctx context.Context) (map[string]string, error) {
	return lc.qdb.ListMigrations(ctx)
}

func (a *Adapter) SetMigration(ctx context.Context, name, value string) error {
	c := proto.NewMigrationServiceClient(a.conn)
	_, err := c.SetMigration(ctx, &proto.SetMigrationRequest{Name: name, Value: value})
	return spqrerror.CleanGrpcError(err)
}

func (a *Adapter) ResetMigration(ctx context.Context, name string) error {
	c := proto.NewMigrationServiceClient(a.conn)
	_, err := c.ResetMigration(ctx, &proto.ResetMigrationRequest{Name: name})
	return spqrerror.CleanGrpcError(err)
}

func (a *Adapter) ListMigrations(ctx context.Context) (map[string]string, error) {
	c := proto.NewMigrationServiceClient(a.conn)
	resp, err := c.ListMigrations(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, spqrerror.CleanGrpcError(err)
	}
	return resp.Migrations, nil
}
