package conn

const (
	SERVER_ACTIVE  = "ACTIVE"
	SERVER_PENDING = "PENDING"
)

type PoolingMode string

const (
	PoolingModeSession     = PoolingMode("SESSION")
	PoolingModeTransaction = PoolingMode("TRANSACTION")
)
