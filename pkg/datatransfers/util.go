package datatransfers

import (
	"context"
	"fmt"
	"strings"

	pgx "github.com/jackc/pgx/v5"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

// GetConnStrings generates connection strings based on the ShardConnect fields.
//
// Parameters:
// - None.
//
// Returns:
// - []string: a slice of strings containing connection strings.
func GetConnStrings(s *config.ShardConnect) []string {
	res := make([]string, len(s.Hosts))
	for i, host := range s.Hosts {
		address := strings.Split(host, ":")[0]
		port := strings.Split(host, ":")[1]
		res[i] = fmt.Sprintf("user=%s host=%s port=%s dbname=%s password=%s application_name=%s", s.User, address, port, s.DB, s.Password, spqrTransferApplicationName)
	}
	return res
}

// GetMasterConnection gets a connection to the master host in a shard
//
// Parameters:
//   - ctx: context for connections
//
// Returns:
//   - *pgx.Conn: the connection to master host
//   - error: error if any occurred
//
// TODO: unit tests
func GetMasterConnection(ctx context.Context, s *config.ShardConnect) (*pgx.Conn, error) {
	for _, dsn := range GetConnStrings(s) {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, err
		}
		var isMaster bool
		row := conn.QueryRow(ctx, "SELECT NOT pg_is_in_recovery() as is_master;")
		if err = row.Scan(&isMaster); err != nil {
			return nil, err
		}
		if isMaster {
			return conn, nil
		}
		_ = conn.Close(ctx)
	}
	return nil, spqrerror.New(spqrerror.SPQR_TRANSFER_ERROR, "unable to find master")
}
