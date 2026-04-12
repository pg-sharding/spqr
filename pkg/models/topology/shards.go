package topology

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/pg-sharding/spqr/pkg/config"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

var ShardMapping map[string]*DataShard

func InitShardMapping(shardMapping map[string]*DataShard) {
	ShardMapping = shardMapping
}

type DataShard struct {
	ID      string
	Type    config.ShardType
	options []GenericOption

	parsedHosts     []config.Host
	parsedAddresses []string
	tls             *config.TLSConfig
}

type ShardsMgr interface {
	AddDataShard(ctx context.Context, shard *DataShard) error
	AddWorldShard(ctx context.Context, shard *DataShard) error
	ListShards(ctx context.Context) ([]*DataShard, error)
	GetShard(ctx context.Context, shardID string) (*DataShard, error)
	AlterShardOptions(ctx context.Context, shardID string, options []GenericOption) error
	SetShardOptions(ctx context.Context, shardID string, options []GenericOption) error
	DropShard(ctx context.Context, id string) error
}

func (ds *DataShard) Options() []GenericOption {
	return ds.options
}

func (ds *DataShard) SetOptions(options []GenericOption) {
	ds.options = options
	ds.parsedHosts, ds.parsedAddresses = retrieveHostsFromOptions(options)
	ds.tls = TLSConfigFromOptions(ds.options)
}

// parseHosts parses the raw hosts into a slice of Hosts.
// The format of the RawHost is host:port:availability_zone.
// If the availability_zone is not provided, it is empty.
// If the port is not provided, it does not matter
func parseHosts(rawHosts []string) (parsedHosts []config.Host, parsedAddresses []string) {
	for _, rawHost := range rawHosts {
		host := config.Host{}
		parts := strings.Split(rawHost, ":")
		if len(parts) > 3 {
			log.Printf("invalid host format: expected 'host:port:availability_zone', got '%s'", rawHost)
			continue
		} else if len(parts) == 3 {
			host.AZ = parts[2]
			host.Address = fmt.Sprintf("%s:%s", parts[0], parts[1])
		} else {
			host.Address = rawHost
		}

		parsedHosts = append(parsedHosts, host)
		parsedAddresses = append(parsedAddresses, host.Address)
	}
	return
}

func (ds *DataShard) Hosts() []string {
	return ds.parsedAddresses
}

func (ds *DataShard) HostsAZ() []config.Host {
	return ds.parsedHosts
}

func retrieveHostsFromOptions(options []GenericOption) ([]config.Host, []string) {
	hosts := make([]string, 0)
	for _, opt := range options {
		if opt.Name == "host" {
			hosts = append(hosts, opt.Arg)
		}
	}
	return parseHosts(hosts)
}

func (ds *DataShard) TLS() *config.TLSConfig {
	return ds.tls
}

// NewDataShard creates a new DataShard instance with the given name and configuration.
//
// Parameters:
//   - name: The name of the shard.
//   - cfg: The configuration of the shard.
//
// Returns:
//   - *DataShard: The created DataShard instance.
func NewDataShard(name string, t config.ShardType, options []GenericOption) *DataShard {
	ds := &DataShard{
		ID:   name,
		Type: t,
	}
	ds.SetOptions(options)
	return ds
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
		Id:      shard.ID,
		Options: GenericOptionsToProto(shard.options),
	}
}

func GenericOptionsToProto(options []GenericOption) []*proto.GenericOption {
	protoOptions := make([]*proto.GenericOption, 0, len(options))
	for _, opt := range options {
		if opt.Name == "host" {
			continue
		}

		protoOptions = append(protoOptions, &proto.GenericOption{
			Name:  opt.Name,
			Value: opt.Arg,
		})
	}

	_, addresses := retrieveHostsFromOptions(options)
	for _, addr := range addresses {
		protoOptions = append(protoOptions, &proto.GenericOption{
			Name:  "host",
			Value: addr,
		})
	}

	return protoOptions
}

func GenericOptionsFromProto(protoOptions []*proto.GenericOption) []GenericOption {
	options := make([]GenericOption, 0, len(protoOptions))
	for _, opt := range protoOptions {
		options = append(options, GenericOption{
			Name: strings.ToLower(opt.Name),
			Arg:  opt.Value,
		})
	}
	return options
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
	return NewDataShard(shard.Id, config.DataShard, GenericOptionsFromProto(shard.Options))
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
	options := make([]GenericOption, 0, len(shard.Options))
	for _, opt := range shard.Options {
		options = append(options, GenericOption{
			Name: strings.ToLower(opt.Name),
			Arg:  opt.Value,
		})
	}
	return NewDataShard(shard.ID, config.DataShard, options)
}

func DataShardToDB(shard *DataShard) *qdb.Shard {
	return &qdb.Shard{
		ID:      shard.ID,
		Options: GenericOptionsToDB(shard.options),
	}
}

func DataShardMapFromConfig(shards map[string]*config.Shard) map[string]*DataShard {
	mds := make(map[string]*DataShard)
	for id, sh := range shards {
		mds[id] = DataShardFromConfig(id, sh)
	}
	return mds
}

func DataShardFromConfig(id string, cfg *config.Shard) *DataShard {
	options := make([]GenericOption, 0)
	options = append(options, hostsToOptions(cfg.RawHosts)...)
	options = append(options, TLSConfigToOptions(cfg.TLS)...)

	return NewDataShard(
		id,
		cfg.Type,
		options,
	)
}

func DataShardMapFromShardConnectConfig(shards map[string]*config.ShardConnect) map[string]*DataShard {
	mds := make(map[string]*DataShard)
	for id, sh := range shards {
		mds[id] = DataShardFromShardConnectConfig(id, sh)
	}
	return mds
}

func DataShardFromShardConnectConfig(id string, cfg *config.ShardConnect) *DataShard {
	options := make([]GenericOption, 0)
	options = append(options, hostsToOptions(cfg.Hosts)...)
	options = append(options, TLSConfigToOptions(cfg.TLS)...)
	options = append(options, []GenericOption{
		{Name: "db", Arg: cfg.DB},
		{Name: "user", Arg: cfg.User},
		{Name: "password", Arg: cfg.Password},
	}...)
	return NewDataShard(id, config.DataShard, options)
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
	return reflect.DeepEqual(a.options, b.options)
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

func OptionsFromSQL(options []spqrparser.GenericOption) []GenericOption {
	m := make([]GenericOption, 0, len(options))

	for _, opt := range options {
		m = append(m, GenericOption{
			Name:   opt.Name,
			Arg:    opt.Arg,
			Action: GenericOptionAction(opt.Action),
		})
	}

	return m
}

func TLSConfigFromOptions(options []GenericOption) *config.TLSConfig {
	tls := &config.TLSConfig{}
	hasTlsOption := false

	for _, opt := range options {
		switch opt.Name {
		case "sslmode":
			tls.SslMode = opt.Arg
			hasTlsOption = true
		case "key_file":
			tls.KeyFile = opt.Arg
			hasTlsOption = true
		case "root_cert_file":
			tls.RootCertFile = opt.Arg
			hasTlsOption = true
		case "cert_file":
			tls.CertFile = opt.Arg
			hasTlsOption = true
		}
	}

	if !hasTlsOption {
		return nil
	}
	return tls
}

func TLSConfigToOptions(tls *config.TLSConfig) []GenericOption {
	if tls == nil {
		return nil
	}

	options := make([]GenericOption, 0)
	if tls.SslMode != "" {
		options = append(options, GenericOption{Name: "sslmode", Arg: tls.SslMode})
	}
	if tls.CertFile != "" {
		options = append(options, GenericOption{Name: "cert_file", Arg: tls.CertFile})
	}
	if tls.RootCertFile != "" {
		options = append(options, GenericOption{Name: "root_cert_file", Arg: tls.RootCertFile})
	}
	if tls.KeyFile != "" {
		options = append(options, GenericOption{Name: "key_file", Arg: tls.KeyFile})
	}

	return options
}

func hostsToOptions(hosts []string) []GenericOption {
	options := make([]GenericOption, 0, len(hosts))
	for _, host := range hosts {
		options = append(options, GenericOption{
			Name: "host",
			Arg:  host,
		})
	}
	return options
}

func GenericOptionsToDB(options []GenericOption) []qdb.GenericOption {
	dboptions := make([]qdb.GenericOption, 0, len(options))
	for _, opt := range options {
		dboptions = append(dboptions, qdb.GenericOption{
			Name:  opt.Name,
			Value: opt.Arg,
		})
	}
	return dboptions
}
