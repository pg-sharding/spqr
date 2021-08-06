package core

import (
	"sync"
)

type Rule struct {

	Usr string `json:"usr" yaml:"usr" toml:"usr"`
	DB string `json:"db" yaml:"db" toml:"db"`

	SHStorage Storage `json:"storage_cfg" yaml:"storage_cfg" toml:"storage_cfg"`

	ClientMax int `json:"client_max" yaml:"client_max" toml:"client_max"`

	TLSCfg TLSConfig

	mu * sync.Mutex
}

func InitRule(r *Rule ) {
	r.mu = &sync.Mutex{}
}

