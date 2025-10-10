package qdb

import (
	"fmt"
	"math"

	"github.com/pg-sharding/spqr/router/rfqn"
)

type ShardKey struct {
	Name string
	RW   bool
}

type KeyRange struct {
	LowerBound     [][]byte `json:"from"`
	ShardID        string   `json:"shard_id"`
	KeyRangeID     string   `json:"key_range_id"`
	DistributionId string   `json:"distribution_id"`
}
type MoveKeyRangeStatus string

const (
	MoveKeyRangePlanned  = MoveKeyRangeStatus("PLANNED")
	MoveKeyRangeStarted  = MoveKeyRangeStatus("STARTED")
	MoveKeyRangeComplete = MoveKeyRangeStatus("COMPLETE")
)

type MoveKeyRange struct {
	MoveId     string             `json:"move_id"`
	ShardId    string             `json:"shard_id"`
	KeyRangeID string             `json:"key_range_id"`
	Status     MoveKeyRangeStatus `json:"status"`
}

type KeyRangeStatus string

const KRLocked = KeyRangeStatus("LOCKED")
const KRUnlocked = KeyRangeStatus("UNLOCKED")

type RouterState string

const (
	CLOSED = RouterState("CLOSED")
	OPENED = RouterState("OPENED")
)

type Router struct {
	Address string      `json:"address"`
	ID      string      `json:"id"`
	State   RouterState `json:"state,omitempty"`
}

func NewRouter(addr, id string, rst RouterState) *Router {
	return &Router{
		Address: addr,
		ID:      id,
		State:   rst,
	}
}

func (r Router) Addr() string {
	return r.Address
}

type Shard struct {
	ID       string   `json:"id"`
	RawHosts []string `json:"hosts"` // format host:port:availability_zone
}

func NewShard(ID string, hosts []string) *Shard {
	return &Shard{
		ID:       ID,
		RawHosts: hosts,
	}
}

var (
	ColumnTypeVarchar           = "varchar"
	ColumnTypeVarcharHashed     = "varchar hashed"
	ColumnTypeVarcharDeprecated = "_varchar"
	ColumnTypeInteger           = "integer"
	ColumnTypeUinteger          = "uinteger"
	ColumnTypeUUID              = "uuid"
)

type TypedColRef struct {
	ColName string `json:"column_name"`
	ColType string `json:"column_type"`
}

type RoutingExpr struct {
	ColRefs []TypedColRef `json:"column_refs_v1"`
}

type DistributionKeyEntry struct {
	Column       string `json:"column"`
	HashFunction string `json:"hash"`

	Expr RoutingExpr `json:"routing_expression"`
}

type DistributedRelation struct {
	Name               string                 `json:"name"`
	SchemaName         string                 `json:"schema_name,omitempty"`
	DistributionKey    []DistributionKeyEntry `json:"column_names"`
	ReplicatedRelation bool                   `json:"replicated_relation,omitempty"`
}

func (r *DistributedRelation) QualifiedName() *rfqn.RelationFQN {
	return &rfqn.RelationFQN{RelationName: r.Name, SchemaName: r.SchemaName}
}

type Distribution struct {
	ID       string   `json:"id"`
	ColTypes []string `json:"col_types,omitempty"`

	Relations map[string]*DistributedRelation `json:"relations"`
	Version   int64                           //if 0 then not exists
}

type ReferenceRelation struct {
	TableName             string            `json:"table_name"`
	SchemaVersion         uint64            `json:"schema_version"`
	ColumnSequenceMapping map[string]string `json:"column_sequence_mapping"`
	ShardIds              []string          `json:"shard_ids"`
}

func NewDistribution(id string, coltypes []string) *Distribution {
	distr := &Distribution{
		ID:        id,
		ColTypes:  coltypes,
		Relations: map[string]*DistributedRelation{},
	}

	return distr
}

type MoveTask struct {
	ID       string
	Bound    [][]byte `json:"bound"`
	KrIdTemp string   `json:"kr_id_temp"`
	State    int      `json:"state"`
}

type MoveTaskGroup struct {
	TaskIDs        []string
	Type           int    `json:"type"`
	ShardToId      string `json:"shard_to_id"`
	KrIdFrom       string `json:"kr_id_from"`
	KrIdTo         string `json:"kr_id_to"`
	TotalTaskCount int
	CurrentTaskInd int
}

type RedistributeTask struct {
	KrId      string `json:"kr_id"`
	ShardId   string `json:"shard_id"`
	BatchSize int    `json:"batch_size"`
	TempKrId  string `json:"temp_kr_id"`
	State     int    `json:"state"`
}

type BalancerTask struct {
	Type      int    `json:"type"`
	KrIdFrom  string `json:"krIdFrom"`
	KrIdTo    string `json:"krIdTo"`
	KrIdTemp  string `json:"krIdTemp"`
	ShardIdTo string `json:"shardIdTo"`
	KeyCount  int64  `json:"keyCount"`
	State     int    `json:"state"`
}

type Sequence struct {
	RelName string `json:"rel_name"`
	ColName string `json:"col_name"`
}

type SequenceIdRange struct {
	Left  int64
	Right int64
}

func NewSequenceIdRange(left int64, right int64) (*SequenceIdRange, error) {
	if left <= right {
		return &SequenceIdRange{Left: left, Right: right}, nil
	}
	return nil, fmt.Errorf("invalid id range: start=%d > end=%d", left, right)
}

func NewRangeBySize(currentRight int64, rangeSize uint64) (*SequenceIdRange, error) {
	if rangeSize >= math.MaxInt64 {
		return nil, fmt.Errorf("invalid (case 0) id-range request: current=%d, request for=%d", currentRight, rangeSize)
	}
	if rangeSize < 1 {
		return nil, fmt.Errorf("invalid (case 1) id-range request: current=%d, request for=%d", currentRight, rangeSize)
	}
	newRight := currentRight + int64(rangeSize) - 1
	if currentRight > newRight {
		return nil, fmt.Errorf("invalid (case 2) id-range request: current=%d, request for=%d", currentRight, rangeSize)
	}
	return NewSequenceIdRange(currentRight, newRight)
}
