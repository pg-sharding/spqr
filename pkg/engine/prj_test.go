package engine

import (
	"testing"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/stretchr/testify/assert"
)

func TestExtractProjectionColumns(t *testing.T) {
	colRef := func(name string) *lyx.ColumnRef { return &lyx.ColumnRef{ColName: name} }
	resTarget := func(name string, val lyx.Node) *lyx.ResTarget { return &lyx.ResTarget{Name: name, Value: val} }

	tests := []struct {
		name string
		list []lyx.Node
		want []string
	}{
		{"nil", nil, nil},
		{"empty", []lyx.Node{}, nil},
		{"star", []lyx.Node{colRef("*")}, nil},
		{"restarget column", []lyx.Node{resTarget("a", colRef("a"))}, []string{"a"}},
		{"empty colname is nil", []lyx.Node{colRef("")}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ExtractProjectionColumns(tt.list))
		})
	}
}
