package parser

import (
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type QParser struct {
	query string
	stmt  lyx.Node
}

func (qp *QParser) OriginQuery() string {
	return qp.query
}

func (qp *QParser) SetOriginQuery(q string) {
	qp.query = q
}

func (qp *QParser) Stmt() lyx.Node {
	return qp.stmt
}

func (qp *QParser) SetStmt(stmt lyx.Node) {
	qp.stmt = stmt
}

// TODO : unit tests
func (qp *QParser) Parse(query string) ([]lyx.Node, string, error) {
	qp.query = query

	comment := ""
	for i := 0; i < len(query)-4; {

		if query[i] != '/' || query[i+1] != '*' {
			i++
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

		if len(comment) == 0 {
			comment = query[i+2 : j]
		} else {
			comment = comment + "," + query[i+2:j]
		}
		i = j + 3
	}

	spqrlog.Zero.Debug().Str("query", query).Msg("parsing client query")

	routerStmts, pos, err := lyx.Parse(query)
	if err != nil {
		return nil, comment, &spqrerror.SpqrError{
			Err:       err,
			Position:  int32(pos),
			ErrorCode: spqrerror.PgSyntaxError,
		}
	}

	qp.stmt = routerStmts[0]

	return routerStmts, comment, nil
}
