package planner

import (
	"fmt"
	"strconv"
	"strings"
)

func RewriteReferenceRelationAutoIncInsert(query string, colname string, nextvalGen func() (int64, error)) (string, error) {
	// Find the position of the opening parenthesis for the column list
	colsOpenInd := strings.Index(query, "(")
	if colsOpenInd == -1 {
		return "", fmt.Errorf("invalid query: missing column list")
	}

	// Find the position of the closing parenthesis for the column list using balanced parentheses
	colsCloseInd := findMatchingCloseParen(query, colsOpenInd)
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
		for pos < len(query) && isWhitespace(query[pos]) {
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
		valuesCloseInd := findMatchingCloseParen(query, valuesOpenInd)
		if valuesCloseInd == -1 {
			return "", fmt.Errorf("invalid query: missing closing parenthesis in VALUES clause")
		}

		// Generate next value
		nextval, err := nextvalGen()
		if err != nil {
			return "", err
		}
		nextvalStr := strconv.FormatInt(nextval, 10)

		// Format the VALUES clause content
		originalValuesContent := query[valuesOpenInd+1 : valuesCloseInd]
		newValuesContent := formatInsertValue(nextvalStr, originalValuesContent)

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
		for pos < len(query) && isWhitespace(query[pos]) {
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

// isWhitespace checks if a character is whitespace
func isWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// formatInsertValue formats a new value to insert while preserving original formatting
func formatInsertValue(newValue, originalContent string) string {
	if len(originalContent) > 0 && isWhitespace(originalContent[0]) {
		// Multiline format: no space after comma
		return newValue + "," + originalContent
	}
	// Single line format: space after comma
	return newValue + ", " + originalContent
}

// findMatchingCloseParen finds the matching closing parenthesis for an opening parenthesis at the given position
// It properly handles nested parentheses and string literals
func findMatchingCloseParen(query string, openPos int) int {
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
