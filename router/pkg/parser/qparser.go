package parser

import (
	"fmt"
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	pgquery "github.com/pganalyze/pg_query_go/v2"
)

type QParser struct {
	stmt  sqlparser.Statement
	q     *pgproto3.Query
	state ParseState
}

func (qp *QParser) Reset() {
	qp.stmt = nil
}

func (qp *QParser) Stmt() sqlparser.Statement {
	parsedStmt, _ := sqlparser.Parse(qp.q.String)
	qp.stmt = parsedStmt
	return qp.stmt
}

func (qp *QParser) State() ParseState {
	return qp.state
}

func (qp *QParser) Q() *pgproto3.Query {
	return qp.q
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

func (qp *QParser) Parse(query *pgproto3.Query) (ParseState, error) {
	qp.q = query

	pstmt, err := pgquery.Parse(query.String)

	spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed query stmt is %T", pstmt)

	if err != nil {
		spqrlog.Logger.Printf(spqrlog.ERROR, "got error while parsing stmt %s: %s", query.String, err)
	} else {
		qp.state = ParseStateQuery{}

		spqrlog.Logger.Printf(spqrlog.DEBUG2, "%v", pstmt.GetStmts())

		if len(pstmt.GetStmts()) == 0 {
			qp.state = ParseStateEmptyQuery{}
			return qp.state, nil
		}

		for _, node := range pstmt.GetStmts() {
			switch q := node.Stmt.Node.(type) {
			//case *pgquery.Node_PrepareStmt:
			//	varStmt := ParseStatePrepareStmt{}
			//	varStmt.Name = q.PrepareStmt.Name
			//	varStmt.Query = query.String
			//	qp.state = varStmt
			//case *pgquery.Node_ExecuteStmt:
			//	q.ExecuteStmt.Name
			case *pgquery.Node_VariableSetStmt:
				if q.VariableSetStmt.IsLocal {
					qp.state = ParseStateSetLocalStmt{}
					return qp.state, nil
				}
				if q.VariableSetStmt.Kind == pgquery.VariableSetKind_VAR_RESET {
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

				} else if q.VariableSetStmt.Kind == pgquery.VariableSetKind_VAR_SET_VALUE {
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

func (qp *QParser) IsRouterCommand() bool {
	if qp.stmt == nil {
		return false
	}
	switch qp.stmt.(type) {
	case *sqlparser.Set:
		return false
	default:
		return false
	}
}
