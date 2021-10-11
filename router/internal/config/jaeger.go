package config

type JaegerCfg struct {
	JaegerUrl string `json:"jaeger_url" toml:"jaeger_url" yaml:"jaeger_url"`
}
