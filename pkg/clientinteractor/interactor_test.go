package clientinteractor_test

import (
	"sort"
	"testing"

	"github.com/golang/mock/gomock"
	mock "github.com/pg-sharding/spqr/pkg/mock/clientinteractor"

	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/spqr/pkg/clientinteractor"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

// TestSimpleWhere tests the MatchRow function with a simple where clause.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestSimpleWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseLeaf{
		Op:     "=",
		ColRef: spqrparser.ColumnRef{ColName: "a"},
		Value:  "1",
	}
	expected := true

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.NoError(err)
	assert.Equal(expected, actual)
}

// TestSimpleNoMatchWhere tests the MatchRow function with a simple where clause that is expected to not match.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestSimpleNoMatchWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseLeaf{
		Op:     "=",
		ColRef: spqrparser.ColumnRef{ColName: "a"},
		Value:  "2",
	}
	expected := false

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.NoError(err)
	assert.Equal(expected, actual)
}

// TestAndNoMatchWhere tests the MatchRow function with a complex where clause that is expected to not match.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestAndNoMatchWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseOp{
		Op: "and",
		Left: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "b"},
			Value:  "2",
		},
		Right: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "a"},
			Value:  "2",
		},
	}
	expected := false

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.Nil(err)
	assert.Equal(expected, actual)
}

// TestOrMatchWhere tests the MatchRow function with a complex where clause that uses the OR operator.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestOrMatchWhere(t *testing.T) {
	assert := assert.New(t)

	row := []string{"1", "2", "3"}
	rowDesc := map[string]int{
		"a": 0,
		"b": 1,
		"c": 2,
	}
	where := spqrparser.WhereClauseOp{
		Op: "or",
		Left: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "a"},
			Value:  "2",
		},
		Right: spqrparser.WhereClauseLeaf{
			Op:     "=",
			ColRef: spqrparser.ColumnRef{ColName: "b"},
			Value:  "2",
		},
	}
	expected := true

	actual, err := clientinteractor.MatchRow(row, rowDesc, where)
	assert.NoError(err)
	assert.Equal(expected, actual)
}

func TestGetColumnsMap(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		name        string
		header      []string
		expectedMap map[string]int
	}{
		{
			name:        "Simple header",
			header:      []string{"a", "b", "c"},
			expectedMap: map[string]int{"a": 0, "b": 1, "c": 2},
		},
		{
			name:        "Empty header",
			header:      []string{},
			expectedMap: map[string]int{},
		},
		{
			name:        "Nil header",
			header:      nil,
			expectedMap: map[string]int{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			tableDescMock := mock.NewMockTableDesc(ctrl)
			tableDescMock.EXPECT().GetHeader().Return(testCase.header)
			assert.Equal(testCase.expectedMap, clientinteractor.GetColumnsMap(tableDescMock))
		})
	}

}

func TestSortableWithContext(t *testing.T) {
	data := [][]string{[]string{"a", "b"}, []string{"b", "a"}}
	rev_data := [][]string{[]string{"b", "a"}, []string{"a", "b"}}
	sortable := clientinteractor.SortableWithContext{data, 0, clientinteractor.DESC}
	sort.Sort(sortable)
	assert.Equal(t, data, rev_data)
}
