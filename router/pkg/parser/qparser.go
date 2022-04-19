package parser

import (
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/jackc/pgproto3/v2"
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

func (qp *QParser) Parse(q pgproto3.Query) error {
	parsedStmt, err := sqlparser.Parse(q.String)
	if err != nil {
		return err
	}
	qp.q = q
	qp.stmt = parsedStmt

	return nil
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
