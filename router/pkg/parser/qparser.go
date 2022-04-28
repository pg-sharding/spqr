package parser

import (
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	"github.com/wal-g/tracelog"
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

type ParseState int

const (
	ParseStateTXBegin    = 0
	ParseStateTXRollback = 1
	ParseStateTXCommit   = 2
	ParseStateQuery      = 3
	ParseStateEmptyQuery = 4
	ParseStateErr        = 5
	ParseStateSkip       = 6
)

func (qp *QParser) Parse(q *pgproto3.Query) (ParseState, error) {
	qp.q = q

	pstmt, err := pgquery.Parse(q.String)

	if err != nil {
		tracelog.ErrorLogger.PrintError(err)
	} else {
		qp.state = ParseStateQuery

		tracelog.InfoLogger.Printf("%v", pstmt.GetStmts())

		if len(pstmt.GetStmts()) == 0 {
			qp.state = ParseStateEmptyQuery
			return ParseStateEmptyQuery, nil
		}

		for _, node := range pstmt.GetStmts() {
			switch q := node.Stmt.Node.(type) {
			case *pgquery.Node_TransactionStmt:
				switch q.TransactionStmt.Kind {
				case pgquery.TransactionStmtKind_TRANS_STMT_BEGIN:
					qp.state = ParseStateTXBegin
					return ParseStateTXBegin, nil
				case pgquery.TransactionStmtKind_TRANS_STMT_COMMIT:
					qp.state = ParseStateTXCommit
					return ParseStateTXCommit, nil
				case pgquery.TransactionStmtKind_TRANS_STMT_ROLLBACK:
					qp.state = ParseStateTXRollback
					return ParseStateTXRollback, nil
				}
			default:
			}
		}
	}

	parsedStmt, err := sqlparser.Parse(q.String)
	if err != nil {
		return ParseStateErr, err
	}

	qp.stmt = parsedStmt

	return ParseStateQuery, nil
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
