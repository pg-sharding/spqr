package parser

import (
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
type ParseStateResetStmt struct {
	ParseState
	Name string
}
type ParseStateResetAllStmt struct {
	ParseState
}

func (qp *QParser) Parse(q *pgproto3.Query) (ParseState, error) {
	qp.q = q

	pstmt, err := pgquery.Parse(q.String)

	spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed query stmt is %T", pstmt)

	if err != nil {
		spqrlog.Logger.PrintError(err)
	} else {
		qp.state = ParseStateQuery{}

		spqrlog.Logger.Printf(spqrlog.DEBUG2, "%v", pstmt.GetStmts())

		if len(pstmt.GetStmts()) == 0 {
			qp.state = ParseStateEmptyQuery{}
			return qp.state, nil
		}

		for _, node := range pstmt.GetStmts() {
			spqrlog.Logger.Printf(spqrlog.DEBUG2, "parsed query stmt node is %T", node.Stmt.Node)
			switch q := node.Stmt.Node.(type) {
			case *pgquery.Node_VariableSetStmt:
				if q.VariableSetStmt.Kind == pgquery.VariableSetKind_VAR_RESET {
					if q.VariableSetStmt.Name == "role" {
						qp.state = ParseStateQuery{}
					} else if q.VariableSetStmt.Name == "all" {
						qp.state = ParseStateResetAllStmt{}
					} else {
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
							varStmt.Value = nq.AConst.Val.String()
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

	parsedStmt, err := sqlparser.Parse(q.String)
	if err != nil {
		return ParseStateErr{}, err
	}

	qp.stmt = parsedStmt

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
