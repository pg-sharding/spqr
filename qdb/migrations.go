package qdb

import (
	"context"
	"maps"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const migrationsNamespace = "/migrations/"

func (q *MemQDB) SetMigration(_ context.Context, name, value string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.State.Migrations == nil {
		q.State.Migrations = make(map[string]string)
	}
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Migrations, name, value))
}

func (q *MemQDB) ResetMigration(_ context.Context, name string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.State.Migrations, name))
}

func (q *MemQDB) ListMigrations(_ context.Context) (map[string]string, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return maps.Clone(q.State.Migrations), nil
}

func (q *EtcdQDB) SetMigration(ctx context.Context, name, value string) error {
	// Preserve names verbatim, including slashes and dot segments in filenames.
	_, err := q.cli.Put(ctx, migrationsNamespace+name, value)
	return err
}

func (q *EtcdQDB) ResetMigration(ctx context.Context, name string) error {
	_, err := q.cli.Delete(ctx, migrationsNamespace+name)
	return err
}

func (q *EtcdQDB) ListMigrations(ctx context.Context) (map[string]string, error) {
	resp, err := q.cli.Get(ctx, migrationsNamespace, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	migrations := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		migrations[strings.TrimPrefix(string(kv.Key), migrationsNamespace)] = string(kv.Value)
	}
	return migrations, nil
}
