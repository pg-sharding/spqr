package topology

import (
	"context"
	"reflect"
	"slices"

	"github.com/pg-sharding/spqr/pkg/config"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
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
	AlterShardHosts(ctx context.Context, shardID string, hosts []string) error
	AlterShardOptions(ctx context.Context, shardID string, options map[string]GenericOption) error
	SetShardOptions(ctx context.Context, shardID string, options map[string]string) error
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
		Hosts:   shard.Cfg.Hosts(),
		Id:      shard.ID,
		Options: shard.Cfg.Options,
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
		TLS:      TLSConfigFromOptions(shard.Options),
		RawHosts: shard.Hosts,
		Type:     config.DataShard,
		Options:  shard.Options,
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
		TLS:      TLSConfigFromOptions(shard.Options),
		Type:     config.DataShard,
		Options:  shard.Options,
	})
}

func DataShardToDB(shard *DataShard) *qdb.Shard {
	return &qdb.Shard{
		ID:       shard.ID,
		RawHosts: shard.Cfg.RawHosts,
		Options:  shard.Cfg.Options,
	}
}

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
	return slices.Equal(a.Cfg.Hosts(), b.Cfg.Hosts()) &&
		reflect.DeepEqual(a.Cfg.Options, b.Cfg.Options)
}

type GenericOptionAction int

const (
	GenericOptionActionUnspecified = iota
	GenericOptionActionAdd
	GenericOptionActionSet
	GenericOptionActionDrop
)

type GenericOption struct {
	Name   string
	Arg    string
	Action GenericOptionAction
}

func OptionsFromSQL(options []spqrparser.GenericOption) map[string]GenericOption {
	m := make(map[string]GenericOption)

	for _, opt := range options {
		m[opt.Name] = GenericOption{
			Name:   opt.Name,
			Arg:    opt.Arg,
			Action: GenericOptionAction(opt.Action),
		}
	}

	return m
}

func TLSConfigFromOptions(options map[string]string) *config.TLSConfig {
	tls := &config.TLSConfig{}
	hasTlsOption := false
	if v, ok := options["sslmode"]; ok {
		tls.SslMode = v
		hasTlsOption = true
	}
	if v, ok := options["key_file"]; ok {
		tls.KeyFile = v
		hasTlsOption = true
	}
	if v, ok := options["root_cert_file"]; ok {
		tls.RootCertFile = v
		hasTlsOption = true
	}
	if v, ok := options["cert_file"]; ok {
		tls.CertFile = v
		hasTlsOption = true
	}
	if !hasTlsOption {
		return nil
	}
	return tls
}
