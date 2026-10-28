package provider

import (
	"context"

	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MigrationServer struct {
	proto.UnimplementedMigrationServiceServer
	impl qdb.MigrationJournal
}

func NewMigrationServer(impl qdb.MigrationJournal) *MigrationServer {
	return &MigrationServer{impl: impl}
}

func (s *MigrationServer) SetMigration(ctx context.Context, req *proto.SetMigrationRequest) (*emptypb.Empty, error) {
	if err := s.impl.SetMigration(ctx, req.Name, req.Value); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *MigrationServer) ResetMigration(ctx context.Context, req *proto.ResetMigrationRequest) (*emptypb.Empty, error) {
	if err := s.impl.ResetMigration(ctx, req.Name); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *MigrationServer) ListMigrations(ctx context.Context, _ *emptypb.Empty) (*proto.ListMigrationsReply, error) {
	migrations, err := s.impl.ListMigrations(ctx)
	if err != nil {
		return nil, err
	}
	return &proto.ListMigrationsReply{Migrations: migrations}, nil
}
