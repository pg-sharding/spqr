package engine

import (
	"strings"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
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
func MatchRow(row [][]byte, nameToIndex map[string]int, condition lyx.Node) (bool, error) {
	if condition == nil {
		return true, nil
	}
	switch where := condition.(type) {
	case *lyx.AExprEmpty:
		return true, nil
	case *lyx.AExprOp:
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
		case "=":
			cr, ok := where.Left.(*lyx.ColumnRef)
			if !ok {
				return true, spqrerror.New(spqrerror.SPQR_COMPLEX_QUERY, "left operand is not a column ref")
			}
			cv, ok := where.Right.(*lyx.AExprSConst)
			if !ok {
				return true, spqrerror.New(spqrerror.SPQR_COMPLEX_QUERY, "right operand is not a string const")
			}
			i, ok := nameToIndex[cr.ColName]
			if !ok {
				return true, spqrerror.Newf(spqrerror.SPQR_COMPLEX_QUERY, "column %s does not exist", cr.ColName)
			}
			/*XXX: use operator here */
			return string(row[i]) == cv.Value, nil
		default:
			return true, spqrerror.Newf(spqrerror.SPQR_COMPLEX_QUERY, "not supported logic operation: %s", where.Op)
		}

	default:
		return false, nil
	}
}

func FilterRows(tts *tupleslot.TupleTableSlot, where lyx.Node) (*tupleslot.TupleTableSlot, error) {

	var filtRows [][][]byte

	for _, row := range tts.Raw {

		match, err := MatchRow(row, tts.Desc.GetColumnsMap(), where)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}

		filtRows = append(filtRows, row)
	}

	tts.Raw = filtRows
	return tts, nil
}
