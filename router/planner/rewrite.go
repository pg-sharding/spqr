package planner

import (
	"fmt"
	"strconv"
	"strings"
)

func ModifyQuery(query string, colname string, nextval int64) (string, error) {
	nextvalStr := strconv.FormatInt(nextval, 10)

	// Find the position of the opening parenthesis for the column list
	colsOpenInd := strings.Index(query, "(")
	if colsOpenInd == -1 {
		return "", fmt.Errorf("invalid query: missing column list")
	}

	// Find the position of the closing parenthesis for the column list
	colsCloseInd := strings.Index(query[colsOpenInd:], ")")
	if colsCloseInd == -1 {
		return "", fmt.Errorf("invalid query: missing closing parenthesis in column list")
	}
	colsCloseInd += colsOpenInd

	// Find the position of the opening parenthesis for the values list
	valuesOpenInd := strings.Index(query[colsCloseInd:], "(")
	if valuesOpenInd == -1 {
		return "", fmt.Errorf("invalid query: missing values list")
	}
	valuesOpenInd += colsCloseInd

	// Find the position of the closing parenthesis for the values list
	valuesCloseInd := strings.Index(query[valuesOpenInd:], ")")
	if valuesCloseInd == -1 {
		return "", fmt.Errorf("invalid query: missing closing parenthesis in values list")
	}
	valuesCloseInd += valuesOpenInd

	// Construct the modified query
	newQuery := query[:colsOpenInd+1] + colname + ", " + query[colsOpenInd+1:colsCloseInd] +
		query[colsCloseInd:valuesOpenInd+1] + nextvalStr + ", " + query[valuesOpenInd+1:valuesCloseInd] +
		query[valuesCloseInd:]

	return newQuery, nil
}
