package engine

import (
	"strings"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

// TODO : unit tests

// MatchRow checks if a row matches a given condition in a WHERE clause.
//
// Parameters:
// - row ([]string): The row of data to be checked.
// - nameToIndex (map[string]int): A map that maps column names to their respective indices in the row.
// - condition (spqrparser.WhereClauseNode): The condition to be checked against the row.
//
// Returns:
// - bool: True if the row matches the condition, false otherwise.
// - error: An error if there was a problem evaluating the condition.
func MatchRow(row []string, nameToIndex map[string]int, condition spqrparser.WhereClauseNode) (bool, error) {
	if condition == nil {
		return true, nil
	}
	switch where := condition.(type) {
	case spqrparser.WhereClauseEmpty:
		return true, nil
	case spqrparser.WhereClauseOp:
		switch strings.ToLower(where.Op) {
		case "and":
			left, err := MatchRow(row, nameToIndex, where.Left)
			if err != nil {
				return true, err
			}
			if !left {
				return false, nil
			}
			right, err := MatchRow(row, nameToIndex, where.Right)
			if err != nil {
				return true, err
			}
			return right, nil
		case "or":
			left, err := MatchRow(row, nameToIndex, where.Left)
			if err != nil {
				return true, err
			}
			if left {
				return true, nil
			}
			right, err := MatchRow(row, nameToIndex, where.Right)
			if err != nil {
				return true, err
			}
			return right, nil
		default:
			return true, spqrerror.Newf(spqrerror.SPQR_COMPLEX_QUERY, "not supported logic operation: %s", where.Op)
		}
	case spqrparser.WhereClauseLeaf:
		switch where.Op {
		case "=":
			i, ok := nameToIndex[where.ColRef.ColName]
			if !ok {
				return true, spqrerror.Newf(spqrerror.SPQR_COMPLEX_QUERY, "column %s does not exist", where.ColRef.ColName)
			}
			return row[i] == where.Value, nil
		default:
			return true, spqrerror.Newf(spqrerror.SPQR_COMPLEX_QUERY, "not supported operation %s", where.Op)
		}
	default:
		return false, nil
	}
}
