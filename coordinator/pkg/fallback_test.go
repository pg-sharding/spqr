package coord

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// mockShardClient is a lightweight manual mock for proto.ShardServiceClient
// used exclusively in fallback tests.
type mockShardClient struct {
	getShardFn   func(ctx context.Context, in *proto.ShardRequest, opts ...grpc.CallOption) (*proto.ShardReply, error)
	dropShardFn  func(ctx context.Context, in *proto.DropShardRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	addDataShardFn func(ctx context.Context, in *proto.AddShardRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

func (m *mockShardClient) ListShards(_ context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*proto.ListShardsReply, error) {
	panic("ListShards not expected in fallback tests")
}

func (m *mockShardClient) AddDataShard(ctx context.Context, in *proto.AddShardRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return m.addDataShardFn(ctx, in, opts...)
}

func (m *mockShardClient) UpdateShard(_ context.Context, _ *proto.UpdateShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	panic("UpdateShard not expected in fallback tests")
}

func (m *mockShardClient) DropShard(ctx context.Context, in *proto.DropShardRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return m.dropShardFn(ctx, in, opts...)
}

func (m *mockShardClient) AddWorldShard(_ context.Context, _ *proto.AddWorldShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	panic("AddWorldShard not expected in fallback tests")
}

func (m *mockShardClient) GetShard(ctx context.Context, in *proto.ShardRequest, opts ...grpc.CallOption) (*proto.ShardReply, error) {
	return m.getShardFn(ctx, in, opts...)
}

// helper: a shard for testing
func testShard() *topology.DataShard {
	return &topology.DataShard{
		ID: "sh1",
		Cfg: &config.Shard{
			RawHosts: []string{"host1:5432"},
			Type:     config.DataShard,
		},
	}
}

func TestFallbackDropAndAdd_HappyPath(t *testing.T) {
	// Drop succeeds, re-add succeeds on first try.
	mock := &mockShardClient{
		getShardFn: func(_ context.Context, in *proto.ShardRequest, _ ...grpc.CallOption) (*proto.ShardReply, error) {
			return &proto.ShardReply{Shard: &proto.Shard{Id: in.Id, Hosts: []string{"old-host:5432"}}}, nil
		},
		dropShardFn: func(_ context.Context, _ *proto.DropShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
		addDataShardFn: func(_ context.Context, _ *proto.AddShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
	}

	err := fallbackDropAndAdd(context.Background(), mock, testShard())
	assert.NoError(t, err)
}

func TestFallbackDropAndAdd_ReAddSucceedsAfterRetry(t *testing.T) {
	// Re-add fails twice then succeeds on the 3rd attempt.
	var addCalls atomic.Int32
	mock := &mockShardClient{
		getShardFn: func(_ context.Context, in *proto.ShardRequest, _ ...grpc.CallOption) (*proto.ShardReply, error) {
			return &proto.ShardReply{Shard: &proto.Shard{Id: in.Id, Hosts: []string{"old-host:5432"}}}, nil
		},
		dropShardFn: func(_ context.Context, _ *proto.DropShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
		addDataShardFn: func(_ context.Context, _ *proto.AddShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			n := addCalls.Add(1)
			if n < 3 {
				return nil, fmt.Errorf("transient network error")
			}
			return &emptypb.Empty{}, nil
		},
	}

	err := fallbackDropAndAdd(context.Background(), mock, testShard())
	assert.NoError(t, err)
	assert.Equal(t, int32(3), addCalls.Load(), "should have taken 3 add attempts")
}

func TestFallbackDropAndAdd_ReAddExhausted_RollbackSucceeds(t *testing.T) {
	// Re-add always fails; rollback with old config should be attempted and succeed.
	var addCalls atomic.Int32
	var lastAddedShard *proto.Shard
	mock := &mockShardClient{
		getShardFn: func(_ context.Context, in *proto.ShardRequest, _ ...grpc.CallOption) (*proto.ShardReply, error) {
			return &proto.ShardReply{Shard: &proto.Shard{Id: in.Id, Hosts: []string{"old-host:5432"}}}, nil
		},
		dropShardFn: func(_ context.Context, _ *proto.DropShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
		addDataShardFn: func(_ context.Context, in *proto.AddShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			n := addCalls.Add(1)
			if n <= 3 {
				// First 3 calls (the retry attempts) fail.
				return nil, fmt.Errorf("transient error")
			}
			// 4th call is the rollback — succeeds.
			lastAddedShard = in.Shard
			return &emptypb.Empty{}, nil
		},
	}

	err := fallbackDropAndAdd(context.Background(), mock, testShard())
	require.Error(t, err, "should return the add error even though rollback succeeded")
	assert.Equal(t, int32(4), addCalls.Load(), "3 retries + 1 rollback = 4 add calls")
	require.NotNil(t, lastAddedShard, "rollback should have called AddDataShard with old config")
	assert.Equal(t, []string{"old-host:5432"}, lastAddedShard.Hosts, "rollback should use old host config")
}

func TestFallbackDropAndAdd_ReAddExhausted_RollbackAlsoFails(t *testing.T) {
	// Re-add always fails, rollback also fails — shard is lost until reconciliation.
	var addCalls atomic.Int32
	mock := &mockShardClient{
		getShardFn: func(_ context.Context, in *proto.ShardRequest, _ ...grpc.CallOption) (*proto.ShardReply, error) {
			return &proto.ShardReply{Shard: &proto.Shard{Id: in.Id, Hosts: []string{"old-host:5432"}}}, nil
		},
		dropShardFn: func(_ context.Context, _ *proto.DropShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
		addDataShardFn: func(_ context.Context, _ *proto.AddShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			addCalls.Add(1)
			return nil, fmt.Errorf("persistent failure")
		},
	}

	err := fallbackDropAndAdd(context.Background(), mock, testShard())
	require.Error(t, err)
	assert.Equal(t, int32(4), addCalls.Load(), "3 retries + 1 rollback attempt = 4 add calls")
}

func TestFallbackDropAndAdd_GetShardUnimplemented_StillWorks(t *testing.T) {
	// GetShard returns Unimplemented — no snapshot available, but drop+add still works.
	mock := &mockShardClient{
		getShardFn: func(_ context.Context, _ *proto.ShardRequest, _ ...grpc.CallOption) (*proto.ShardReply, error) {
			return nil, status.Error(codes.Unimplemented, "method not implemented")
		},
		dropShardFn: func(_ context.Context, _ *proto.DropShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
		addDataShardFn: func(_ context.Context, _ *proto.AddShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
	}

	err := fallbackDropAndAdd(context.Background(), mock, testShard())
	assert.NoError(t, err)
}

func TestFallbackDropAndAdd_DropFails(t *testing.T) {
	// Drop fails — should return error immediately without attempting re-add.
	var addCalls atomic.Int32
	mock := &mockShardClient{
		getShardFn: func(_ context.Context, in *proto.ShardRequest, _ ...grpc.CallOption) (*proto.ShardReply, error) {
			return &proto.ShardReply{Shard: &proto.Shard{Id: in.Id}}, nil
		},
		dropShardFn: func(_ context.Context, _ *proto.DropShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return nil, fmt.Errorf("drop failed")
		},
		addDataShardFn: func(_ context.Context, _ *proto.AddShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			addCalls.Add(1)
			return &emptypb.Empty{}, nil
		},
	}

	err := fallbackDropAndAdd(context.Background(), mock, testShard())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "drop failed")
	assert.Equal(t, int32(0), addCalls.Load(), "should not attempt re-add if drop fails")
}

func TestFallbackDropAndAdd_ContextCancelled(t *testing.T) {
	// Context cancelled during retry backoff — should return context error promptly.
	ctx, cancel := context.WithCancel(context.Background())
	mock := &mockShardClient{
		getShardFn: func(_ context.Context, in *proto.ShardRequest, _ ...grpc.CallOption) (*proto.ShardReply, error) {
			return &proto.ShardReply{Shard: &proto.Shard{Id: in.Id}}, nil
		},
		dropShardFn: func(_ context.Context, _ *proto.DropShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
		addDataShardFn: func(_ context.Context, _ *proto.AddShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			cancel() // cancel on first add failure, should exit during backoff
			return nil, fmt.Errorf("transient error")
		},
	}

	err := fallbackDropAndAdd(ctx, mock, testShard())
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestFallbackDropAndAdd_GetShardUnexpectedError_AbortsBeforeDrop(t *testing.T) {
	// GetShard returns an unexpected error (Internal) — fallback should abort
	// before the destructive drop to avoid losing the shard without rollback.
	var dropCalls atomic.Int32
	var addCalls atomic.Int32
	mock := &mockShardClient{
		getShardFn: func(_ context.Context, _ *proto.ShardRequest, _ ...grpc.CallOption) (*proto.ShardReply, error) {
			return nil, status.Error(codes.Internal, "storage backend unavailable")
		},
		dropShardFn: func(_ context.Context, _ *proto.DropShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			dropCalls.Add(1)
			return &emptypb.Empty{}, nil
		},
		addDataShardFn: func(_ context.Context, _ *proto.AddShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			addCalls.Add(1)
			return &emptypb.Empty{}, nil
		},
	}

	err := fallbackDropAndAdd(context.Background(), mock, testShard())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fallback aborted")
	assert.Contains(t, err.Error(), "snapshot failed")
	assert.Equal(t, int32(0), dropCalls.Load(), "should NOT attempt drop when snapshot fails unexpectedly")
	assert.Equal(t, int32(0), addCalls.Load(), "should NOT attempt re-add when snapshot fails unexpectedly")
}

func TestFallbackDropAndAdd_GetShardPermissionDenied_AbortsBeforeDrop(t *testing.T) {
	// GetShard returns PermissionDenied — another unexpected error class that
	// should trigger abort before drop.
	var dropCalls atomic.Int32
	mock := &mockShardClient{
		getShardFn: func(_ context.Context, _ *proto.ShardRequest, _ ...grpc.CallOption) (*proto.ShardReply, error) {
			return nil, status.Error(codes.PermissionDenied, "access denied")
		},
		dropShardFn: func(_ context.Context, _ *proto.DropShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			dropCalls.Add(1)
			return &emptypb.Empty{}, nil
		},
		addDataShardFn: func(_ context.Context, _ *proto.AddShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
	}

	err := fallbackDropAndAdd(context.Background(), mock, testShard())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fallback aborted")
	assert.Equal(t, int32(0), dropCalls.Load(), "should NOT attempt drop on PermissionDenied")
}

func TestFallbackDropAndAdd_GetShardNotFound_StillWorks(t *testing.T) {
	// GetShard returns NotFound — shard doesn't exist on router yet.
	// This is a safe condition: proceed with drop+add without snapshot.
	mock := &mockShardClient{
		getShardFn: func(_ context.Context, _ *proto.ShardRequest, _ ...grpc.CallOption) (*proto.ShardReply, error) {
			return nil, status.Error(codes.NotFound, "shard not found")
		},
		dropShardFn: func(_ context.Context, _ *proto.DropShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
		addDataShardFn: func(_ context.Context, _ *proto.AddShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
	}

	err := fallbackDropAndAdd(context.Background(), mock, testShard())
	assert.NoError(t, err, "should proceed normally when GetShard returns NotFound")
}

func TestFallbackDropAndAdd_GetShardPlainError_AbortsBeforeDrop(t *testing.T) {
	// GetShard returns a plain (non-gRPC-status) error — treated as unexpected,
	// should abort before destructive drop.
	var dropCalls atomic.Int32
	mock := &mockShardClient{
		getShardFn: func(_ context.Context, _ *proto.ShardRequest, _ ...grpc.CallOption) (*proto.ShardReply, error) {
			return nil, fmt.Errorf("connection reset by peer")
		},
		dropShardFn: func(_ context.Context, _ *proto.DropShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			dropCalls.Add(1)
			return &emptypb.Empty{}, nil
		},
		addDataShardFn: func(_ context.Context, _ *proto.AddShardRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
			return &emptypb.Empty{}, nil
		},
	}

	err := fallbackDropAndAdd(context.Background(), mock, testShard())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fallback aborted")
	assert.Equal(t, int32(0), dropCalls.Load(), "should NOT attempt drop on plain network error")
}
