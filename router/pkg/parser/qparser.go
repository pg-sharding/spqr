package parser

import (
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	pgproto3 "github.com/jackc/pgproto3/v2"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	"github.com/wal-g/tracelog"
)

type QParser struct {
	stmt sqlparser.Statement
	q    pgproto3.Query
}

func (qp *QParser) Reset() {
	qp.stmt = nil
}

func (qp *QParser) Stmt() sqlparser.Statement {
	return qp.stmt
}

func (qp *QParser) Q() pgproto3.Query {
	return qp.q
}

type ParseState int

const (
	ParseStateTXBegin    = 0
	ParseStateTXRollback = 1
	ParseStateTXCommit   = 2
	ParseStateQuery      = 3
	ParseStateErr        = 4
)

func (qp *QParser) Parse(q pgproto3.Query) (ParseState, error) {
	qp.q = q

	pstmt, err := pgquery.Parse(q.String)

	if err != nil {
		tracelog.ErrorLogger.PrintError(err)
	} else {
		for _, node := range pstmt.GetStmts() {
			switch q := node.Stmt.Node.(type) {
			case *pgquery.Node_TransactionStmt:
				switch q.TransactionStmt.Kind {
				case pgquery.TransactionStmtKind_TRANS_STMT_BEGIN:
					return ParseStateTXBegin, nil
				case pgquery.TransactionStmtKind_TRANS_STMT_COMMIT:
					return ParseStateTXCommit, nil
				case pgquery.TransactionStmtKind_TRANS_STMT_ROLLBACK:
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
