package topology

import (
	"context"
	"slices"

	"github.com/pg-sharding/spqr/pkg/config"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
)

type DataShard struct {
	ID  string
	Cfg *config.Shard
}

type ShardsMgr interface {
	AddDataShard(ctx context.Context, shard *DataShard) error
	AddWorldShard(ctx context.Context, shard *DataShard) error
	ListShards(ctx context.Context) ([]*DataShard, error)
	GetShard(ctx context.Context, shardID string) (*DataShard, error)
	UpdateShard(ctx context.Context, shard *DataShard) error
	DropShard(ctx context.Context, id string) error
}

// NewDataShard creates a new DataShard instance with the given name and configuration.
//
// Parameters:
//   - name: The name of the shard.
//   - cfg: The configuration of the shard.
//
// Returns:
//   - *DataShard: The created DataShard instance.
func NewDataShard(name string, cfg *config.Shard) *DataShard {
	return &DataShard{
		ID:  name,
		Cfg: cfg,
	}
}

func tlsConfigToProto(cfg *config.TLSConfig) *proto.TLSConfig {
	if cfg == nil {
		return nil
	}
	return &proto.TLSConfig{
		Sslmode:      cfg.SslMode,
		CertFile:     cfg.CertFile,
		KeyFile:      cfg.KeyFile,
		RootCertFile: cfg.RootCertFile,
	}
}

func tlsConfigFromProto(cfg *proto.TLSConfig) *config.TLSConfig {
	if cfg == nil {
		return nil
	}
	return &config.TLSConfig{
		SslMode:      cfg.Sslmode,
		CertFile:     cfg.CertFile,
		KeyFile:      cfg.KeyFile,
		RootCertFile: cfg.RootCertFile,
	}
}

func TLSConfigToDB(cfg *config.TLSConfig) *qdb.TLSConfig {
	if cfg == nil {
		return nil
	}
	return &qdb.TLSConfig{
		SslMode:      cfg.SslMode,
		CertFile:     cfg.CertFile,
		KeyFile:      cfg.KeyFile,
		RootCertFile: cfg.RootCertFile,
	}
}

func tlsConfigFromDB(cfg *qdb.TLSConfig) *config.TLSConfig {
	if cfg == nil {
		return nil
	}
	return &config.TLSConfig{
		SslMode:      cfg.SslMode,
		CertFile:     cfg.CertFile,
		KeyFile:      cfg.KeyFile,
		RootCertFile: cfg.RootCertFile,
	}
}

// DataShardToProto converts a DataShard object to a proto.Shard object.
// It takes a pointer to a DataShard as input and returns a pointer to a proto.Shard.
//
// Parameters:
//   - shard: The DataShard object to convert.
//
// Returns:
//   - *proto.Shard: The converted proto.Shard object.
func DataShardToProto(shard *DataShard) *proto.Shard {
	return &proto.Shard{
		Hosts: shard.Cfg.Hosts(),
		Id:    shard.ID,
		Tls:   tlsConfigToProto(shard.Cfg.TLS),
	}
}

// DataShardFromProto creates a new DataShard instance from the given proto.Shard.
// It initializes the DataShard with the shard ID and hosts from the proto.Shard,
// and sets the shard type to config.DataShard.
//
// Parameters:
//   - shard: The proto.Shard object to convert.
//
// Returns:
//   - *DataShard: The created DataShard instance.
func DataShardFromProto(shard *proto.Shard) *DataShard {
	return NewDataShard(shard.Id, &config.Shard{
		RawHosts: shard.Hosts,
		Type:     config.DataShard,
		TLS:      tlsConfigFromProto(shard.Tls),
	})
}

// DataShardFromDB creates a new DataShard instance from the given qdb.Shard.
// It initializes the DataShard with the shard ID and hosts from the qdb.Shard,
// and sets the shard type to config.DataShard.
//
// Parameters:
//   - shard: The qdb.Shard object to convert.
//
// Returns:
//   - *DataShard: The created DataShard instance.
func DataShardFromDB(shard *qdb.Shard) *DataShard {
	return NewDataShard(shard.ID, &config.Shard{
		RawHosts: shard.RawHosts,
		Type:     config.DataShard,
		TLS:      tlsConfigFromDB(shard.TLS),
	})
}

func DataShardToDB(shard *DataShard) *qdb.Shard {
	return &qdb.Shard{
		ID:       shard.ID,
		RawHosts: shard.Cfg.RawHosts,
		TLS:      TLSConfigToDB(shard.Cfg.TLS),
	}
}

// tlsConfigEqual reports whether two TLS configs are semantically equal.
func tlsConfigEqual(a, b *config.TLSConfig) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.SslMode == b.SslMode &&
		a.CertFile == b.CertFile &&
		a.KeyFile == b.KeyFile &&
		a.RootCertFile == b.RootCertFile
}

// ShardConfigEqual reports whether two DataShards have identical configuration
// (hosts and TLS). This is used by SyncRouterMetadata to detect shards
// that exist on both the coordinator and the router but have drifted.
func ShardConfigEqual(a, b *DataShard) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.ID != b.ID {
		return false
	}
	if a.Cfg == nil && b.Cfg == nil {
		return true
	}
	if a.Cfg == nil || b.Cfg == nil {
		return false
	}
	if !slices.Equal(a.Cfg.RawHosts, b.Cfg.RawHosts) {
		return false
	}
	return tlsConfigEqual(a.Cfg.TLS, b.Cfg.TLS)
}

// validSslModes is the set of SSL modes recognised by SPQR.
var validSslModes = map[string]bool{
	"disable":     true,
	"allow":       true,
	"prefer":      true,
	"require":     true,
	"verify-ca":   true,
	"verify-full": true,
}

// ValidSslMode reports whether mode is a recognised SSL mode.
func ValidSslMode(mode string) bool {
	return validSslModes[mode]
}
