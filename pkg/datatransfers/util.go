package datatransfers

import (
	"context"
	"fmt"
	"strings"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/sethvargo/go-retry"
)

// GetConnStrings generates connection strings based on the ShardConnect fields.
//
// Parameters:
// - None.
//
// Returns:
// - []string: a slice of strings containing connection strings.
func GetConnStrings(s *config.ShardConnect, applicationName string) map[string]string {
	if applicationName == "" {
		applicationName = spqrTransferApplicationName
	}
	res := make(map[string]string)
	for _, host := range s.Hosts {
		address := strings.Split(host, ":")[0]
		port := strings.Split(host, ":")[1]
		res[host] = fmt.Sprintf("user=%s host=%s port=%s dbname=%s password=%s application_name=%s", s.User, address, port, s.DB, s.Password, applicationName)
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
func GetMasterConnection(ctx context.Context, s *config.ShardConnect, taskGroupID string) (*pgx.Conn, error) {
	applicationName := ""
	if taskGroupID != "" {
		applicationName = spqrTransferApplicationName + "_" + taskGroupID
	}
	for _, dsn := range GetConnStrings(s, applicationName) {
		conn, err := connectDsn(ctx, dsn)
		if err != nil {
			return nil, retry.RetryableError(err)
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
	return nil, retry.RetryableError(spqrerror.New(spqrerror.SpqrTransferError, "unable to find master"))
}

func GetMasterHost(ctx context.Context, s *config.ShardConnect) (string, error) {
	for host, dsn := range GetConnStrings(s, "") {
		conn, err := connectDsn(ctx, dsn)
		if err != nil {
			return "", retry.RetryableError(err)
		}
		defer func() {
			_ = conn.Close(ctx)
		}()
		var isMaster bool
		row := conn.QueryRow(ctx, "SELECT NOT pg_is_in_recovery() as is_master;")
		if err = row.Scan(&isMaster); err != nil {
			return "", err
		}
		if isMaster {
			parts := strings.Split(host, ":")
			if len(parts) == 0 {
				return "", fmt.Errorf("malformed host address: missing port")
			}
			return parts[0], nil
		}
	}
	return "", spqrerror.New(spqrerror.SpqrTransferError, "unable to find master")
}

func connectDsn(ctx context.Context, dsn string) (*pgx.Conn, error) {
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	level, err := tracelog.LogLevelFromString(config.CoordinatorConfig().DataMoveQueryLogLevel)
	if err != nil {
		return nil, err
	}
	connConfig.Tracer = &tracelog.TraceLog{
		Logger:   &spqrlog.ZeroTraceLogger{},
		LogLevel: level,
	}
	connConfig.RuntimeParams["spqrguard.prevent_distributed_table_modify"] = "off"
	return pgx.ConnectConfig(ctx, connConfig)
}
