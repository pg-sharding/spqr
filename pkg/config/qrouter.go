package config

type QrouterType string

const (
	LocalQrouter = QrouterType("LOCAL")
	ProxyQrouter = QrouterType("PROXY")
	ShardQrouter = QrouterType("SHARDING")
)

type QrouterConfig struct {
	Qtype string `json:"qrouter_type" toml:"qrouter_type" yaml:"qrouter_type"`
}
