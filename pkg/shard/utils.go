package shard

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ShardIDs returns a slice of shard IDs extracted from the given list of shards.
// It takes a list of shards as input and returns a slice of shard IDs.
//
// Parameters:
//   - shards: A list of shards to extract the IDs from.
//
// Returns:
//   - []uint: A slice of shard IDs.
func ShardIDs(shards []ShardHostInstance) []uint {
	ret := []uint{}
	for _, shard := range shards {
		ret = append(ret, shard.ID())
	}
	return ret
}

func CheckExtension(ctx context.Context, conn *pgx.Conn, extname string, extversion string) (bool, error) {
	res := conn.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM pg_extension WHERE extname = '%s' and extversion = '%s'", extname, extversion))
	count := 0
	if err := res.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
