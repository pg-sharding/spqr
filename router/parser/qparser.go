package parser

import (
	"strings"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type QParser struct {
	query string
	state ParseState
	stmt  lyx.Node
}

func (qp *QParser) Stmt() lyx.Node {
	return qp.stmt
}

func (qp *QParser) SetStmt(stmt lyx.Node) {
	qp.stmt = stmt
}

type ParseState any

type ParseStateTXBegin struct {
	ParseState
	Options []lyx.TransactionModeItem
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
	Name  string
	Value string
}

type ParseStateResetStmt struct {
	ParseState
	Name string
}

type ParseStateShowStmt struct {
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

// TODO : unit tests
func (qp *QParser) Parse(query string) (ParseState, string, error) {
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

	qp.stmt = nil
	spqrlog.Zero.Debug().Str("query", query).Msg("parsing client query")

	routerStmts, err := lyx.Parse(query)
	if err != nil {
		return nil, comment, err
	}
	if routerStmts == nil {
		qp.state = ParseStateEmptyQuery{}
		return qp.state, comment, nil
	}

	qp.stmt = routerStmts
	qp.state = ParseStateQuery{}

	spqrlog.Zero.Debug().Type("stmt-type", routerStmts).Msg("parsed query statements")
	qp.state = ParseStateQuery{}

	switch q := routerStmts.(type) {
	case *lyx.Explain:
		varStmt := ParseStateExplain{}
		/* TODO: get query here*/
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
	case *lyx.VariableShowStmt:
		return ParseStateShowStmt{
			Name: q.Name,
		}, comment, nil
	case *lyx.VariableSetStmt:
		spqrlog.Zero.Debug().
			Str("name", q.Name).
			Str("query", string(q.Kind)).
			Bool("local", q.IsLocal).
			Bool("session", q.Session).
			Msg("parsed set stmt")
		// XXX: TODO: support
		if q.IsLocal {
			qp.state = ParseStateSetLocalStmt{
				Name:  q.Name,
				Value: q.Value[0],
			}
			return qp.state, comment, nil
		}

		switch q.Kind {
		case lyx.VarTypeResetAll:
			qp.state = ParseStateResetAllStmt{}
		case lyx.VarTypeReset:
			switch q.Name {
			case "session_authorization", "role":
				qp.state = ParseStateResetMetadataStmt{
					Setting: q.Name,
				}
			default:
				varStmt := ParseStateResetStmt{}
				varStmt.Name = q.Name
				qp.state = varStmt
			}
		/* TBD: support multi-set */
		// case pgquery.VariableSetKind_VAR_SET_MULTI:
		// 	qp.state = ParseStateSetLocalStmt{}
		// 	return qp.state, comment, nil
		case lyx.VarTypeSet, "":
			varStmt := ParseStateSetStmt{}
			varStmt.Name = q.Name
			if len(q.Value) > 0 {
				varStmt.Value = q.Value[0]
			}

			qp.state = varStmt
		}
		return qp.state, comment, nil
	case *lyx.TransactionStmt:
		switch q.Kind {
		case lyx.TRANS_STMT_BEGIN:
			qp.state = ParseStateTXBegin{
				Options: q.Options,
			}
			return qp.state, comment, nil
		case lyx.TRANS_STMT_COMMIT:
			qp.state = ParseStateTXCommit{}
			return qp.state, comment, nil
		case lyx.TRANS_STMT_ROLLBACK:
			qp.state = ParseStateTXRollback{}
			return qp.state, comment, nil
		default:
		}
	default:
	}

	return ParseStateQuery{}, comment, nil
}
