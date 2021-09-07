package config

const LocalQrouter = "LOCAL"
const ShardQrouter = "SHARDING"

type QrouterConfig struct {
	Qtype      string `json:"qrouter_type" toml:"qrouter_type" yaml:"qrouter_type"`
	LocalShard string `json:"local_shard" toml:"local_shard" yaml:"local_shard"`
}
