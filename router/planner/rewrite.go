package planner

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/rfqn"
)

func RewriteUpdateToDelete(query string, rqdn *rfqn.RelationFQN) (string, error) {

	if query[len(query)-1] == ';' {
		query = query[:len(query)-1]
	}

	// Find the WHERE keyword
	// we expect query in form of simple update, no CTE or RETURNING.
	valuesKeywordStart := strings.Index(strings.ToUpper(query), "WHERE")
	if valuesKeywordStart == -1 {
		return "", fmt.Errorf("invalid query: missing VALUES clause")
	}

	return fmt.Sprintf(`DELETE FROM "%s" %s`, rqdn.String(), query[valuesKeywordStart:]), nil
}

func RewriteDistributedRelInsertForIndexes(query string, iis []*distributions.UniqueIndex) (string, error) {
	spqrlog.Zero.Debug().Str("query", query).Msg("rewrite rel insert for indexes")
	if query[len(query)-1] == ';' {
		query = query[:len(query)-1]
	}

	query += " RETURNING "

	for ind, is := range iis {
		spqrlog.Zero.Debug().Str("id", is.ID).Strs("columns", is.Columns).Msg("got index")
		if ind == 0 {
			query += strings.Join(is.Columns, ", ")
		} else {
			query += " , " + strings.Join(is.Columns, ", ")
		}
	}

	return query, nil
}

func RewriteDistributedRelBatchInsert(query string, shs []kr.ShardKey) (*plan.ScatterPlan, error) {

	p := &plan.ScatterPlan{
		SubPlan: &plan.ModifyTable{},
	}

	// Find the VALUES keyword
	valuesKeywordStart := strings.Index(strings.ToUpper(query), "VALUES")
	if valuesKeywordStart == -1 {
		return nil, fmt.Errorf("invalid query: missing VALUES clause")
	}

	// Find and process each VALUES clause
	pos := valuesKeywordStart + 6

	mp := map[string]string{}
	frst := map[string]bool{}
	for _, sh := range shs {
		frst[sh.Name] = true
		mp[sh.Name] = query[:pos] + " " /* add one space to make query pretty */
	}

	valIndx := 0

	for {
		// Skip whitespace
		for pos < len(query) && unicode.IsSpace(rune(query[pos])) {
			pos++
		}

		if pos >= len(query) {
			break
		}

		// Look for opening parenthesis of VALUES clause
		if query[pos] != '(' {
			// If not a parenthesis and we've processed at least one VALUES clause, we're done
			if valIndx == len(shs) {
				break
			}
			return nil, fmt.Errorf("invalid query: expected opening parenthesis for VALUES clause")
		}

		// Find matching closing parenthesis for this VALUES clause
		valuesOpenInd := pos
		valuesCloseInd := findMatchingClosingParenthesis(query, valuesOpenInd)
		if valuesCloseInd == -1 {
			return nil, fmt.Errorf("invalid query: missing closing parenthesis in VALUES clause")
		}

		// Format the VALUES clause content
		if !frst[shs[valIndx].Name] {
			mp[shs[valIndx].Name] += ", "
		}

		mp[shs[valIndx].Name] += query[valuesOpenInd : valuesCloseInd+1]

		// Move past this VALUES clause
		pos = valuesCloseInd + 1

		// Skip whitespace and look for comma
		whitespaceStart := pos
		for pos < len(query) && unicode.IsSpace(rune(query[pos])) {
			pos++
		}

		if pos >= len(query) || query[pos] != ',' {
			// No more VALUES clauses, preserve the whitespace and add remaining query
			if whitespaceStart < len(query) {
				for sh := range mp {
					mp[sh] += query[whitespaceStart:]
				}
			}
			break
		}

		// Skip the comma
		pos++

		frst[shs[valIndx].Name] = false
		valIndx++
	}

	for sh := range frst {
		p.ExecTargets = append(p.ExecTargets, kr.ShardKey{
			Name: sh,
		})
	}

	sort.Slice(p.ExecTargets, func(i, j int) bool {
		return p.ExecTargets[i].Name < p.ExecTargets[j].Name
	})

	p.OverwriteQuery = mp

	return p, nil
}

func RewriteReferenceRelationAutoIncInsert(query string, colname string, nextvalGen func() (string, error)) (string, error) {
	// Find the position of the opening parenthesis for the column list
	colsOpenInd := strings.Index(query, "(")
	if colsOpenInd == -1 {
		return "", fmt.Errorf("invalid query: missing column list")
	}

	// Find the position of the closing parenthesis for the column list using balanced parentheses
	colsCloseInd := findMatchingClosingParenthesis(query, colsOpenInd)
	if colsCloseInd == -1 {
		return "", fmt.Errorf("invalid query: missing closing parenthesis in column list")
	}

	// Build the new column list with the added column at the beginning
	originalContent := query[colsOpenInd+1 : colsCloseInd]
	newColumnList := formatInsertValue(colname, originalContent)
	newQuery := query[:colsOpenInd+1] + newColumnList

	// Find the VALUES keyword
	valuesKeywordStart := strings.Index(strings.ToUpper(query[colsCloseInd:]), "VALUES")
	if valuesKeywordStart == -1 {
		return "", fmt.Errorf("invalid query: missing VALUES clause")
	}
	valuesKeywordStart += colsCloseInd

	// Add the part between column list and VALUES
	newQuery += query[colsCloseInd : valuesKeywordStart+6] // +6 for "VALUES"

	// Find and process each VALUES clause
	pos := valuesKeywordStart + 6
	first := true

	for {
		// Skip whitespace
		for pos < len(query) && unicode.IsSpace(rune(query[pos])) {
			pos++
		}

		if pos >= len(query) {
			break
		}

		// Look for opening parenthesis of VALUES clause
		if query[pos] != '(' {
			// If not a parenthesis and we've processed at least one VALUES clause, we're done
			if !first {
				break
			}
			return "", fmt.Errorf("invalid query: expected opening parenthesis for VALUES clause")
		}

		// Find matching closing parenthesis for this VALUES clause
		valuesOpenInd := pos
		valuesCloseInd := findMatchingClosingParenthesis(query, valuesOpenInd)
		if valuesCloseInd == -1 {
			return "", fmt.Errorf("invalid query: missing closing parenthesis in VALUES clause")
		}

		// Generate next value
		nextval, err := nextvalGen()
		if err != nil {
			return "", err
		}

		// Format the VALUES clause content
		originalValuesContent := query[valuesOpenInd+1 : valuesCloseInd]
		newValuesContent := formatInsertValue(nextval, originalValuesContent)

		// Add the VALUES clause with the new column value
		if first {
			// First VALUES clause - preserve original spacing after VALUES keyword
			spaceBetween := query[valuesKeywordStart+6 : valuesOpenInd]
			newQuery += spaceBetween + "(" + newValuesContent + ")"
			first = false
		} else {
			newQuery += ", (" + newValuesContent + ")"
		}

		// Move past this VALUES clause
		pos = valuesCloseInd + 1

		// Skip whitespace and look for comma
		whitespaceStart := pos
		for pos < len(query) && unicode.IsSpace(rune(query[pos])) {
			pos++
		}

		if pos >= len(query) || query[pos] != ',' {
			// No more VALUES clauses, preserve the whitespace and add remaining query
			if whitespaceStart < len(query) {
				newQuery += query[whitespaceStart:]
			}
			break
		}

		// Skip the comma
		pos++
	}

	return newQuery, nil
}

// formatInsertValue formats a new value to insert while preserving original formatting
func formatInsertValue(newValue, originalContent string) string {
	if len(originalContent) > 0 && unicode.IsSpace(rune(originalContent[0])) {
		// Multiline format: no space after comma
		return newValue + "," + originalContent
	}
	// Single line format: space after comma
	return newValue + ", " + originalContent
}

// findMatchingClosingParenthesis finds the matching closing parenthesis for an opening parenthesis at the given position
// It properly handles nested parentheses and string literals
func findMatchingClosingParenthesis(query string, openPos int) int {
	if openPos >= len(query) || query[openPos] != '(' {
		return -1
	}

	level := 0
	inSingleQuote := false
	inDoubleQuote := false

	for i := openPos; i < len(query); i++ {
		char := query[i]

		// Handle string literals
		if !inDoubleQuote && char == '\'' {
			inSingleQuote = !inSingleQuote
		} else if !inSingleQuote && char == '"' {
			inDoubleQuote = !inDoubleQuote
		} else if !inSingleQuote && !inDoubleQuote {
			// Only count parentheses outside of string literals
			switch char {
			case '(':
				level++
			case ')':
				level--
				if level == 0 {
					return i
				}
			}
		}
	}

	return -1 // No matching closing parenthesis found
}

func InsertSequenceValue(ctx context.Context,
	query string,
	columns []string,
	ColumnSequenceMapping map[string]string,
	idCache IdentityRouterCache,
) (string, error) {

	for colName, seqName := range ColumnSequenceMapping {
		if slices.Contains(columns, colName) {
			continue
		}

		newQuery, err := RewriteReferenceRelationAutoIncInsert(query, colName, func() (string, error) {
			v, err := idCache.NextVal(ctx, seqName)
			if err != nil {
				return "", err
			}
			return strconv.FormatInt(v, 10), nil
		})
		if err != nil {
			return "", err
		}
		query = newQuery
	}

	return query, nil
}

// XXX: Rewrite this using native plan.QueryVX/analyzeQueryVx
func getMaxPrepStmtId(s lyx.Node) int {
	ret := 1

	switch ins := s.(type) {
	case *lyx.Insert:
		if ins.SubSelect != nil {
			switch q := ins.SubSelect.(type) {
			case *lyx.ValueClause:
				for _, v := range q.Values {
					for _, el := range v {
						switch val := el.(type) {
						case *lyx.ParamRef:
							if val.Number+1 > ret {
								ret = val.Number + 1
							}
						}
					}
				}
			}
		}
	}

	return ret
}

func InsertSequenceParamRef(ctx context.Context,
	query string,
	ColumnSequenceMapping map[string]string,
	stmt lyx.Node,
	def *prepstatement.PreparedStatementDefinition,
) (string, error) {

	for colName, seqName := range ColumnSequenceMapping {

		newQuery, err := RewriteReferenceRelationAutoIncInsert(query, colName, func() (string, error) {
			// what param ref is max for given query?

			// analyze lyx statement
			maxId := getMaxPrepStmtId(stmt)
			def.OverwriteRemoveParamIds = map[int]struct{}{maxId: {}}
			def.SeqName = seqName

			return fmt.Sprintf("$%d", maxId), nil
		})
		if err != nil {
			return "", err
		}
		def.Query = newQuery
		query = newQuery
	}

	return query, nil
}
