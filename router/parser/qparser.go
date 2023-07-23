package parser

import (
	"strings"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/qrouter"

	"github.com/pg-sharding/lyx/lyx"
)

type QParser struct {
	query string
	state ParseState
	stmt  lyx.Node
}

func (qp *QParser) State() ParseState {
	return qp.state
}

func (qp *QParser) Stmt() lyx.Node {
	return qp.stmt
}

func (qp *QParser) Query() string {
	return qp.query
}

type ParseState interface{}

type ParseStateTXBegin struct {
	ParseState
}

type ParseStateTXRollback struct {
	ParseState
}

type ParseStateTXCommit struct {
	ParseState
}

type ParseStateQuery struct {
	ParseState
}

type ParseStateEmptyQuery struct {
	ParseState
}

type ParseStateErr struct {
	ParseState
}

type ParseStateSkip struct {
	ParseState
}

type ParseStateSetStmt struct {
	ParseState
	Name  string
	Value string
}

type ParseStateSetLocalStmt struct {
	ParseState
}

type ParseStateResetStmt struct {
	ParseState
	Name string
}

type ParseStateResetAllStmt struct {
	ParseState
}

type ParseStateResetMetadataStmt struct {
	ParseState
	Setting string
}

type ParseStatePrepareStmt struct {
	ParseState
	Name  string
	Query string
}

type ParseStateExecute struct {
	ParseState
	ParamsQuerySuf string
	Name           string
}

type ParseStateExplain struct {
	ParseState
	Query lyx.Node
}

func (qp *QParser) Parse(query string) (ParseState, string, error) {
	qp.query = query

	comment := ""
	for i := 0; i+4 < len(query); i++ {

		if !(query[i] == '/' && query[i+1] == '*') {
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

	qp.stmt = nil

	routerStmts, err := lyx.Parse(query)
	if err != nil {
		return qp.stmt, "", qrouter.ComplexQuery
	}
	if routerStmts == nil {
		qp.state = ParseStateEmptyQuery{}
		return qp.state, comment, nil
	}

	qp.stmt = routerStmts
	qp.state = ParseStateQuery{}

	if err != nil {
		return ParseStateQuery{}, comment, nil
	}

	spqrlog.Zero.Debug().Type("stmt-type", routerStmts).Msg("parsed query statements")
	qp.state = ParseStateQuery{}

	switch q := routerStmts.(type) {
	case *lyx.Explain:
		varStmt := ParseStateExplain{}
		/* TODO: get query herte*/
		// varStmt.Query = q.Stmt
		return varStmt, comment, nil
	case *lyx.Execute:
		varStmt := ParseStateExecute{}
		varStmt.Name = q.Id
		ss := strings.Split(strings.Split(strings.ToLower(query), "execute")[1], strings.ToLower(varStmt.Name))[1]

		varStmt.ParamsQuerySuf = ss
		qp.state = varStmt

		return varStmt, comment, nil
	case *lyx.Prepare:
		varStmt := ParseStatePrepareStmt{}
		spqrlog.Zero.Debug().
			Type("query-type", q).
			Msg("prep stmt query")
		varStmt.Name = q.Id
		// prepare *name* as *query*
		ss := strings.Split(strings.Split(strings.Split(strings.ToLower(query), "prepare")[1], strings.ToLower(varStmt.Name))[1], "as")[1]
		varStmt.Query = ss
		qp.query = ss
		spqrlog.Zero.Debug().
			Str("name", varStmt.Name).
			Str("query", varStmt.Query).
			Msg("parsed prep stmt")
		qp.state = varStmt

		return qp.state, comment, nil
	case *lyx.VarSet:
		if q.IsLocal {
			qp.state = ParseStateSetLocalStmt{}
			return qp.state, comment, nil
		}
		switch q.Type {
		case lyx.VarTypeReset:
			switch q.Name {
			case "session_authorization", "role":
				qp.state = ParseStateResetMetadataStmt{
					Setting: q.Name,
				}
			case "all":
				qp.state = ParseStateResetAllStmt{}
			default:
				varStmt := ParseStateResetStmt{}
				varStmt.Name = q.Name
				qp.state = varStmt
			}
		/* TBD: support multi-set */
		// case pgquery.VariableSetKind_VAR_SET_MULTI:
		// 	qp.state = ParseStateSetLocalStmt{}
		// 	return qp.state, comment, nil
		case lyx.VarTypeSet:
			varStmt := ParseStateSetStmt{}
			varStmt.Name = q.Name

			varStmt.Value = q.Value

			qp.state = varStmt
		}
		return qp.state, comment, nil
	case *lyx.Begin:
		qp.state = ParseStateTXBegin{}
		return qp.state, comment, nil
	case *lyx.Commit:
		qp.state = ParseStateTXCommit{}
		return qp.state, comment, nil
	case *lyx.Rollback:
		qp.state = ParseStateTXRollback{}
		return qp.state, comment, nil
	default:
	}

	return ParseStateQuery{}, comment, nil
}
