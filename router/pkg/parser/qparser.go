package parser

import (
	"fmt"
	"strings"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

type QParser struct {
	stmt  *pgquery.ParseResult
	query string
	state ParseState
}

func (qp *QParser) Reset() {
	qp.stmt = nil
}

func (qp *QParser) Stmt() (*pgquery.ParseResult, error) {
	parsedStmt, err := pgquery.Parse(qp.query)
	if err != nil {
		return nil, err
	}
	qp.stmt = parsedStmt
	return qp.stmt, nil
}

func (qp *QParser) State() ParseState {
	return qp.state
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
	Query *pgquery.Node
}

func (qp *QParser) Parse(query string) (ParseState, error) {
	qp.query = query

	pstmt, err := pgquery.Parse(query)

	spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed query stmt is %T", pstmt)

	if err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "got error while parsing stmt %s: %s", query, err)
	} else {
		qp.state = ParseStateQuery{}

		spqrlog.Logger.Printf(spqrlog.DEBUG2, "%v", pstmt.GetStmts())

		if len(pstmt.GetStmts()) == 0 {
			qp.state = ParseStateEmptyQuery{}
			return qp.state, nil
		}

		for _, node := range pstmt.GetStmts() {
			switch q := node.Stmt.Node.(type) {
			case *pgquery.Node_ExplainStmt:
				varStmt := ParseStateExplain{}
				varStmt.Query = q.ExplainStmt.Query
				return varStmt, nil
			case *pgquery.Node_ExecuteStmt:
				varStmt := ParseStateExecute{}
				varStmt.Name = q.ExecuteStmt.Name
				ss := strings.Split(strings.Split(strings.ToLower(query), "execute")[1], strings.ToLower(varStmt.Name))[1]

				varStmt.ParamsQuerySuf = ss
				qp.state = varStmt
				return varStmt, nil
			case *pgquery.Node_PrepareStmt:
				varStmt := ParseStatePrepareStmt{}
				spqrlog.Logger.Printf(spqrlog.DEBUG1, "prep stmt query is %v", q)
				varStmt.Name = q.PrepareStmt.Name
				// prepare *name* as *query*
				ss := strings.Split(strings.Split(strings.Split(strings.ToLower(query), "prepare")[1], strings.ToLower(varStmt.Name))[1], "as")[1]
				varStmt.Query = ss
				qp.query = ss
				spqrlog.Logger.Printf(spqrlog.DEBUG1, "parsed prep stmt %s %s", varStmt.Name, varStmt.Query)
				qp.state = varStmt
				return qp.state, nil
			//case *pgquery.Node_ExecuteStmt:
			//	query.ExecuteStmt.Name
			case *pgquery.Node_VariableSetStmt:
				if q.VariableSetStmt.IsLocal {
					qp.state = ParseStateSetLocalStmt{}
					return qp.state, nil
				}

				switch q.VariableSetStmt.Kind {
				case pgquery.VariableSetKind_VAR_RESET:
					switch q.VariableSetStmt.Name {
					case "session_authorization", "role":
						qp.state = ParseStateResetMetadataStmt{
							Setting: q.VariableSetStmt.Name,
						}
					case "all":
						qp.state = ParseStateResetAllStmt{}
					default:
						varStmt := ParseStateResetStmt{}
						varStmt.Name = q.VariableSetStmt.Name
						qp.state = varStmt
					}

				case pgquery.VariableSetKind_VAR_SET_MULTI:
					qp.state = ParseStateSetLocalStmt{}
					return qp.state, nil
				case pgquery.VariableSetKind_VAR_SET_VALUE:
					varStmt := ParseStateSetStmt{}
					varStmt.Name = q.VariableSetStmt.Name

					for _, node := range q.VariableSetStmt.Args {
						switch nq := node.Node.(type) {
						case *pgquery.Node_AConst:
							switch act := nq.AConst.Val.Node.(type) {
							case *pgquery.Node_String_:
								varStmt.Value = act.String_.Str
							case *pgquery.Node_Integer:
								varStmt.Value = fmt.Sprintf("%d", act.Integer.Ival)
							}
						}
					}

					qp.state = varStmt
				}
				return qp.state, nil
			case *pgquery.Node_TransactionStmt:
				switch q.TransactionStmt.Kind {
				case pgquery.TransactionStmtKind_TRANS_STMT_BEGIN:
					qp.state = ParseStateTXBegin{}
					return qp.state, nil
				case pgquery.TransactionStmtKind_TRANS_STMT_COMMIT:
					qp.state = ParseStateTXCommit{}
					return qp.state, nil
				case pgquery.TransactionStmtKind_TRANS_STMT_ROLLBACK:
					qp.state = ParseStateTXRollback{}
					return qp.state, nil
				}
			default:
			}
		}
	}

	return ParseStateQuery{}, nil
}
