package config

type PoolingMode string

const (
	PoolingModeSession     = PoolingMode("SESSION")
	PoolingModeTransaction = PoolingMode("TRANSACTION")
)
