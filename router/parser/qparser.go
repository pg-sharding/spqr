package parser

import (
	"github.com/pg-sharding/lyx/lyx"
)

type QParser struct {
}

// TODO : unit tests
func (qp *QParser) Parse(query string) (lyx.Node, string, error) {

	comment := ""
	for i := range len(query) - 4 {

		if query[i] != '/' || query[i+1] != '*' {
			continue
		}
		j := i + 2

		for ; j+1 < len(query); j++ {
			if query[j] == '*' && query[j+1] == '/' {
				break
			}
		}

		if j+1 >= len(query) {
			break
		}

		comment = query[i+2 : j]
	}

	routerStmts, err := lyx.Parse(query)
	if err != nil {
		return nil, comment, err
	}

	return routerStmts, comment, nil
}
