package qdb

import "context"

type PgQDB struct {
}

// AcquireTxOwnership implements [DCStateKeeper].
func (p *PgQDB) AcquireTxOwnership(gid string) bool {
	panic("unimplemented")
}

// AddRouter implements [DCStateKeeper].
func (p *PgQDB) AddRouter(ctx context.Context, r *Router) error {
	panic("unimplemented")
}

// AddShard implements [DCStateKeeper].
func (p *PgQDB) AddShard(ctx context.Context, shard *Shard) error {
	panic("unimplemented")
}

// ChangeTxStatus implements [DCStateKeeper].
func (p *PgQDB) ChangeTxStatus(gid string, state string) error {
	panic("unimplemented")
}

// CloseRouter implements [DCStateKeeper].
func (p *PgQDB) CloseRouter(ctx context.Context, rID string) error {
	panic("unimplemented")
}

// DeleteRouter implements [DCStateKeeper].
func (p *PgQDB) DeleteRouter(ctx context.Context, rID string) error {
	panic("unimplemented")
}

// DeleteRouterAll implements [DCStateKeeper].
func (p *PgQDB) DeleteRouterAll(ctx context.Context) error {
	panic("unimplemented")
}

// DropShard implements [DCStateKeeper].
func (p *PgQDB) DropShard(ctx context.Context, shardID string) error {
	panic("unimplemented")
}

// GetShard implements [DCStateKeeper].
func (p *PgQDB) GetShard(ctx context.Context, shardID string) (*Shard, error) {
	panic("unimplemented")
}

// ListRouters implements [DCStateKeeper].
func (p *PgQDB) ListRouters(ctx context.Context) ([]*Router, error) {
	panic("unimplemented")
}

// ListShards implements [DCStateKeeper].
func (p *PgQDB) ListShards(ctx context.Context) ([]*Shard, error) {
	panic("unimplemented")
}

// OpenRouter implements [DCStateKeeper].
func (p *PgQDB) OpenRouter(ctx context.Context, rID string) error {
	panic("unimplemented")
}

// RecordTwoPhaseMembers implements [DCStateKeeper].
func (p *PgQDB) RecordTwoPhaseMembers(gid string, shards []string) error {
	panic("unimplemented")
}

// ReleaseTxOwnership implements [DCStateKeeper].
func (p *PgQDB) ReleaseTxOwnership(gid string) {
	panic("unimplemented")
}

// TXCohortShards implements [DCStateKeeper].
func (p *PgQDB) TXCohortShards(gid string) []string {
	panic("unimplemented")
}

// TXStatus implements [DCStateKeeper].
func (p *PgQDB) TXStatus(gid string) string {
	panic("unimplemented")
}

var _ DCStateKeeper = &PgQDB{}
