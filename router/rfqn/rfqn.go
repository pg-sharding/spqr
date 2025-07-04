package rfqn

import (
	"fmt"
	"strings"

	"github.com/pg-sharding/lyx/lyx"
)

type RelationFQN struct {
	RelationName string
	SchemaName   string
}

func RelationFQNFromRangeRangeVar(rv *lyx.RangeVar) RelationFQN {
	return RelationFQN{
		RelationName: rv.RelationName,
		SchemaName:   rv.SchemaName,
	}
}
func (n RelationFQN) String() string {
	if len(n.SchemaName) < 1 {
		return n.RelationName
	}
	return n.SchemaName + "." + n.RelationName
}
func ParseFQN(str string) (*RelationFQN, error) {
	parts := strings.Split(str, ".")
	if len(str) == 0 || len(strings.TrimSpace(str)) == 0 {
		return nil, fmt.Errorf("invalid qualified name='%v' (case0)", str)
	}
	if len(parts) == 1 {
		return &RelationFQN{RelationName: parts[0]}, nil
	} else if len(parts) == 2 {
		schema := parts[0]
		table := parts[1]
		if len(schema) == 0 || len(table) == 0 ||
			strings.TrimSpace(schema) != schema || strings.TrimSpace(table) != table {
			return nil, fmt.Errorf("invalid qualified name='%v'  (case2)", str)
		}
		return &RelationFQN{SchemaName: schema, RelationName: table}, nil
	}
	return nil, fmt.Errorf("invalid qualified name='%v' (case1)", str)
}
