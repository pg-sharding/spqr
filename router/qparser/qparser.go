package qparser

import (
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type QParser struct {
	query     string
	stmt      lyx.Node
	stmts     []lyx.Node
	locations []lyx.ParseLoc
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

func (qp *QParser) Stmts() []lyx.Node {
	return qp.stmts
}

func (qp *QParser) SetStmts(stmts []lyx.Node) {
	qp.stmts = stmts
}

func (qp *QParser) StatementLocations() []lyx.ParseLoc {
	return qp.locations
}

func (qp *QParser) SetStatementLocations(locations []lyx.ParseLoc) {
	qp.locations = locations
}

func (qp *QParser) StatementQuery(index int) string {
	if index < 0 || index+1 >= len(qp.locations) {
		return ""
	}
	return qp.query[qp.locations[index]:qp.locations[index+1]]
}

// TODO : unit tests
func (qp *QParser) Parse(query string) ([]lyx.Node, []string, error) {
	qp.query = query
	qp.stmts = nil
	qp.locations = nil

	comments := ExtractComments(query)

	spqrlog.Zero.Debug().Str("query", query).Msg("parsing client query")

	routerStmts, locations, pos, err := lyx.ParseWithLocations(query)
	if err != nil {
		return nil, comments, &spqrerror.SpqrError{
			Err:       err,
			Position:  int32(pos),
			ErrorCode: spqrerror.PG_SYNTAX_ERROR,
		}
	}

	qp.stmt = routerStmts[0]
	qp.stmts = routerStmts
	qp.locations = locations

	return routerStmts, comments, nil
}

func ExtractComments(query string) []string {
	comments := []string{}
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

		comments = append(comments, query[i+2:j])
		i = j + 3
	}
	return comments
}
