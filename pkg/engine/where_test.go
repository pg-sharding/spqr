package engine

import (
	"testing"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/stretchr/testify/assert"
)

func TestMatchRow(t *testing.T) {
	row := [][]byte{[]byte("alice"), []byte("30")}
	idx := map[string]int{"name": 0, "age": 1}

	col := func(name string) *lyx.ColumnRef { return &lyx.ColumnRef{ColName: name} }
	str := func(v string) *lyx.AExprSConst { return &lyx.AExprSConst{Value: v} }
	eq := func(colName, val string) *lyx.AExprOp {
		return &lyx.AExprOp{Op: "=", Left: col(colName), Right: str(val)}
	}

	tests := []struct {
		name      string
		condition lyx.Node
		wantMatch bool
		wantErr   bool
	}{
		{
			name:      "nil condition always matches",
			condition: nil,
			wantMatch: true,
		},
		{
			name:      "empty expression always matches",
			condition: &lyx.AExprEmpty{},
			wantMatch: true,
		},
		{
			name:      "equal: value matches",
			condition: eq("name", "alice"),
			wantMatch: true,
		},
		{
			name:      "equal: value does not match",
			condition: eq("name", "bob"),
			wantMatch: false,
		},
		{
			name:      "equal: case-sensitive mismatch",
			condition: eq("name", "Alice"),
			wantMatch: false,
		},
		{
			name:      "equal: left operand is not a column ref",
			condition: &lyx.AExprOp{Op: "=", Left: str("x"), Right: str("alice")},
			wantMatch: true,
			wantErr:   true,
		},
		{
			name:      "equal: right operand is not a string const",
			condition: &lyx.AExprOp{Op: "=", Left: col("name"), Right: col("age")},
			wantMatch: true,
			wantErr:   true,
		},
		{
			name:      "equal: column does not exist in index",
			condition: eq("missing_col", "alice"),
			wantMatch: true,
			wantErr:   true,
		},
		{
			name:      "and: both sides true",
			condition: &lyx.AExprOp{Op: "and", Left: eq("name", "alice"), Right: eq("age", "30")},
			wantMatch: true,
		},
		{
			name:      "and: left side false short-circuits",
			condition: &lyx.AExprOp{Op: "and", Left: eq("name", "bob"), Right: eq("missing_col", "alice")},
			wantMatch: false,
		},
		{
			name:      "and: right side false",
			condition: &lyx.AExprOp{Op: "and", Left: eq("name", "alice"), Right: eq("age", "99")},
			wantMatch: false,
		},
		{
			name:      "or: both sides false",
			condition: &lyx.AExprOp{Op: "or", Left: eq("name", "bob"), Right: eq("age", "99")},
			wantMatch: false,
		},
		{
			name:      "or: left side true short-circuits",
			condition: &lyx.AExprOp{Op: "or", Left: eq("name", "alice"), Right: eq("missing_col", "alice")},
			wantMatch: true,
		},
		{
			name:      "or: right side true",
			condition: &lyx.AExprOp{Op: "or", Left: eq("name", "bob"), Right: eq("age", "30")},
			wantMatch: true,
		},
		{
			name:      "operator is case-insensitive (AND uppercase)",
			condition: &lyx.AExprOp{Op: "AND", Left: eq("name", "alice"), Right: eq("age", "30")},
			wantMatch: true,
		},
		{
			name:      "unsupported operator returns error",
			condition: &lyx.AExprOp{Op: ">", Left: col("age"), Right: str("10")},
			wantMatch: true,
			wantErr:   true,
		},
		{
			name:      "unsupported node type returns false without error",
			condition: &lyx.AExprIConst{Value: 1},
			wantMatch: false,
		},
		{
			name: "nested logic: (name=alice AND age=30) OR name=bob",
			condition: &lyx.AExprOp{
				Op: "or",
				Left: &lyx.AExprOp{
					Op:    "and",
					Left:  eq("name", "alice"),
					Right: eq("age", "30"),
				},
				Right: eq("name", "bob"),
			},
			wantMatch: true,
		},
		{
			name: "error propagation: left true, right has missing column",
			condition: &lyx.AExprOp{
				Op:    "and",
				Left:  eq("name", "alice"),
				Right: eq("missing", "val"),
			},
			wantMatch: true,
			wantErr:   true,
		},
		{
			name:      "and: left side errors",
			condition: &lyx.AExprOp{Op: "and", Left: eq("missing", "val"), Right: eq("name", "alice")},
			wantMatch: true,
			wantErr:   true,
		},
		{
			name:      "or: left side errors",
			condition: &lyx.AExprOp{Op: "or", Left: eq("missing", "val"), Right: eq("name", "alice")},
			wantMatch: true,
			wantErr:   true,
		},
		{
			name:      "or: left false, right side errors",
			condition: &lyx.AExprOp{Op: "or", Left: eq("name", "bob"), Right: eq("missing", "val")},
			wantMatch: true,
			wantErr:   true,
		},
		{
			name:      "column name is case-sensitive",
			condition: eq("NAME", "alice"),
			wantMatch: true,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := MatchRow(row, idx, tt.condition)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.wantMatch, match)
		})
	}
}
