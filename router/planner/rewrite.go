package planner

import (
	"fmt"
	"strconv"
	"strings"
)

func ModifyQuery(query string, colname string, nextvalGen func() (int64, error)) (string, error) {

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

	newQuery := query[:colsOpenInd+1] + colname + ", " + query[colsOpenInd+1:colsCloseInd]

	first := true

	var valuesCloseInd int

	lookupStart := colsCloseInd

	for {
		// Find the position of the opening parenthesis for the values list
		valuesOpenInd := strings.Index(query[lookupStart:], "(")
		if valuesOpenInd == -1 {
			if !first {
				break
			}
			return "", fmt.Errorf("invalid query: missing values list")
		}

		valuesOpenInd += lookupStart

		// Find the position of the closing parenthesis for the values list
		valuesCloseInd = strings.Index(query[valuesOpenInd:], ")")
		if valuesCloseInd == -1 {
			return "", fmt.Errorf("invalid query: missing closing parenthesis in values list")
		}
		valuesCloseInd += valuesOpenInd

		lookupStart = valuesCloseInd + 1

		nextval, err := nextvalGen()
		if err != nil {
			return "", err
		}
		nextvalStr := strconv.FormatInt(nextval, 10)

		// Construct the modified query
		if first {
			newQuery += query[colsCloseInd:valuesOpenInd+1] +
				nextvalStr + ", " + query[valuesOpenInd+1:valuesCloseInd] + ")"
		} else {
			newQuery += ", (" + nextvalStr + ", " + query[valuesOpenInd+1:valuesCloseInd] + ")"
		}

		first = false
	}

	newQuery += query[lookupStart:]

	return newQuery, nil
}
