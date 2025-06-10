package rrelation

type ReferenceRelation struct {
	Name                  string
	SchemaVersion         uint64
	ColumnSequenceMapping map[string]string
	ShardId               []string
}
