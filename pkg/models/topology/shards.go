package topology

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

type TopologyMgr interface {
	ShardById(string) (*DataShard, error)

	AddShard(*DataShard)

	DropShard(string)

	SetOptions(string, []GenericOption)

	Snap() map[string]*DataShard
}

type TopologyMgrImpl struct {
	shardMapping sync.Map
}

func (t *TopologyMgrImpl) Snap() map[string]*DataShard {

	/* NB: this is not exactly `snapshot` */
	rt := map[string]*DataShard{}

	t.shardMapping.Range(func(k, v any) bool {
		id, ok := k.(string)
		if !ok {
			return true
		}

		cnf, ok := v.(*DataShard)
		if !ok {
			return true
		}
		rt[id] = cnf
		return true
	})

	return rt
}

func (t *TopologyMgrImpl) ShardById(id string) (*DataShard, error) {

	shRaw, ok := t.shardMapping.Load(id)
	if !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_NO_DATASHARD, "shard with name %q not found", id)
	}

	sh, ok := shRaw.(*DataShard)
	if !ok {
		return nil, spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, "topology map corrupted for id").Detail(fmt.Sprintf("shard id is %s", id))
	}
	return sh, nil
}

func (t *TopologyMgrImpl) AddShard(sh *DataShard) {
	sh.SetOptions(sh.options)
	_, _ = t.shardMapping.LoadOrStore(sh.ID, sh)
}

func (t *TopologyMgrImpl) DropShard(id string) {
	t.shardMapping.Delete(id)
}

func (t *TopologyMgrImpl) SetOptions(id string, opt []GenericOption) {
	shRaw, ok := t.shardMapping.Load(id)
	if !ok {
		/* TODO: proper error report */
		return
	}

	sh := shRaw.(*DataShard)

	shCopy := *sh
	/* TODO: better copy */
	shCopy.SetOptions(opt)

	t.shardMapping.Store(id, &shCopy)
}

var TopMgr TopologyMgr

func TopMgrFromMap(shardMapping map[string]*DataShard) TopologyMgr {
	t := &TopologyMgrImpl{}
	for k, v := range shardMapping {
		t.shardMapping.Store(k, v)
	}
	return t
}

func InitShardMapping(shardMapping map[string]*DataShard) {
	TopMgr = TopMgrFromMap(shardMapping)
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
	ds.parsedAddresses = nil
	ds.parsedHosts = nil
	ds.tls = nil
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
	if ds.parsedAddresses == nil {
		ds.parsedHosts, ds.parsedAddresses = retrieveHostsFromOptions(ds.options)
	}
	return ds.parsedAddresses
}

func (ds *DataShard) HostsAZ() []config.Host {
	if ds.parsedHosts == nil {
		ds.parsedHosts, ds.parsedAddresses = retrieveHostsFromOptions(ds.options)
	}
	return ds.parsedHosts
}

func retrieveHostsFromOptions(options []GenericOption) ([]config.Host, []string) {
	hosts := retrieveRawHostsFromOptions(options)
	return parseHosts(hosts)
}

func retrieveRawHostsFromOptions(options []GenericOption) []string {
	hosts := make([]string, 0)
	for _, opt := range options {
		if opt.Name == "host" {
			hosts = append(hosts, opt.Arg)
		}
	}
	return hosts
}

func (ds *DataShard) TLS() *config.TLSConfig {
	if ds.tls == nil {
		ds.tls = TLSConfigFromOptions(ds.options)
	}
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
func DataShardToProto(shard *DataShard, hostsWithAZ bool) *proto.Shard {
	return &proto.Shard{
		Id:      shard.ID,
		Options: GenericOptionsToProto(shard.options, hostsWithAZ),
	}
}

func GenericOptionsToProto(options []GenericOption, hostsWithAZ bool) []*proto.GenericOption {
	protoOptions := make([]*proto.GenericOption, 0, len(options))
	for _, opt := range options {
		if !hostsWithAZ && opt.Name == "host" {
			continue
		}

		protoOptions = append(protoOptions, &proto.GenericOption{
			Name:  opt.Name,
			Value: opt.Arg,
		})
	}

	if !hostsWithAZ {
		_, addresses := retrieveHostsFromOptions(options)
		for _, addr := range addresses {
			protoOptions = append(protoOptions, &proto.GenericOption{
				Name:  "host",
				Value: addr,
			})
		}
	}

	return protoOptions
}

func GenericOptionsFromProto(protoOptions []*proto.GenericOption) ([]GenericOption, error) {
	options := make([]GenericOption, 0, len(protoOptions))
	for _, opt := range protoOptions {
		action, err := GenericOptionActionFromProto(opt.Action)
		if err != nil {
			return nil, err
		}
		options = append(options, GenericOption{
			Name:   strings.ToLower(opt.Name),
			Arg:    opt.Value,
			Action: action,
		})
	}
	return options, nil
}

func GenericOptionActionFromProto(action proto.GenericOption_Action) (GenericOptionAction, error) {
	switch action {
	case proto.GenericOption_ADD:
		return GenericOptionActionAdd, nil
	case proto.GenericOption_SET:
		return GenericOptionActionSet, nil
	case proto.GenericOption_DROP:
		return GenericOptionActionDrop, nil
	default:
		return -1, fmt.Errorf("unknown action %s", action.String())
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
func DataShardFromProto(shard *proto.Shard) (*DataShard, error) {
	options, err := GenericOptionsFromProto(shard.Options)
	if err != nil {
		return nil, err
	}
	return NewDataShard(shard.Id, config.DataShard, options), nil
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
	options = append(options, hostsToOptions(shard.RawHosts)...)
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

func OptionsFromSQL(options []spqrparser.GenericOption) ([]GenericOption, error) {
	m := make([]GenericOption, 0, len(options))

	for _, opt := range options {
		action, err := genericOptionActionFromSQL(opt.Action)
		if err != nil {
			return nil, err
		}

		m = append(m, GenericOption{
			Name:   opt.Name,
			Arg:    opt.Arg,
			Action: action,
		})
	}

	return m, nil
}

func genericOptionActionFromSQL(action spqrparser.OptionAction) (GenericOptionAction, error) {
	switch action {
	case spqrparser.OptionActionUnspecified:
		return GenericOptionActionUnspecified, nil
	case spqrparser.OptionActionAdd:
		return GenericOptionActionAdd, nil
	case spqrparser.OptionActionDrop:
		return GenericOptionActionDrop, nil
	case spqrparser.OptionActionSet:
		return GenericOptionActionSet, nil
	default:
		return -1, fmt.Errorf("unknown generic option action %d", action)
	}
}

func TLSConfigFromOptions(options []GenericOption) *config.TLSConfig {
	tls := &config.TLSConfig{}
	hasTLSOption := false

	for _, opt := range options {
		switch opt.Name {
		case "sslmode":
			tls.SslMode = opt.Arg
			hasTLSOption = true
		case "key_file":
			tls.KeyFile = opt.Arg
			hasTLSOption = true
		case "root_cert_file":
			tls.RootCertFile = opt.Arg
			hasTLSOption = true
		case "cert_file":
			tls.CertFile = opt.Arg
			hasTLSOption = true
		}
	}

	if !hasTLSOption {
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
