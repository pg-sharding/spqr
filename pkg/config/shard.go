package config

type InstanceCFG struct {
	ConnAddr string `json:"conn_addr" toml:"conn_addr" yaml:"conn_addr"`
	Proto    string `json:"proto" toml:"proto" yaml:"proto"`
}

type ShardType string

const (
	DataShard  = ShardType("DATA")
	WorldShard = ShardType("WORLD")
)

type ShardCfg struct {
	Hosts []*InstanceCFG `json:"hosts" toml:"hosts" yaml:"hosts"`

	ConnDB  string `json:"conn_db" toml:"conn_db" yaml:"conn_db"`
	ConnUsr string `json:"conn_usr" toml:"conn_usr" yaml:"conn_usr"`

	Passwd string    `json:"passwd" toml:"passwd" yaml:"passwd"`
	ShType ShardType `json:"shard_type" toml:"shard_type" yaml:"shard_type"`

	TLSCfg TLSConfig `json:"tls" yaml:"tls" toml:"tls"`
}
