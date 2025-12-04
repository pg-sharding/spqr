package qdb

import (
	"fmt"

	"github.com/google/uuid"
	protos "github.com/pg-sharding/spqr/pkg/protos"
)

const (
	CMD_PUT = iota
	CMD_DELETE
)

type QdbStatement struct {
	CmdType int32
	Key     string
	Value   string
	// for case when qdb have more than one KV-storage
	Extension string
}

func NewQdbStatement(cmdType int32, key string, value string) (*QdbStatement, error) {
	if cmdType != CMD_PUT && cmdType != CMD_DELETE {
		return nil, fmt.Errorf("unknown type of QdbStatement: %d", cmdType)
	}
	return &QdbStatement{CmdType: cmdType, Key: key, Value: value}, nil
}

func NewQdbStatementExt(cmdType int32, key string, value string, extension string) (*QdbStatement, error) {
	if stmt, err := NewQdbStatement(cmdType, key, value); err != nil {
		return nil, err
	} else {
		stmt.Extension = extension
		return stmt, nil
	}
}

func (s *QdbStatement) ToProto() *protos.QdbTransactionCmd {
	return &protos.QdbTransactionCmd{Command: s.CmdType, Key: s.Key, Value: s.Value}
}

func QdbStmtFromProto(protoStmt *protos.QdbTransactionCmd) (*QdbStatement, error) {
	return NewQdbStatementExt(protoStmt.Command, protoStmt.Key, protoStmt.Value, protoStmt.Extension)
}

func SliceToProto(stmts []QdbStatement) []*protos.QdbTransactionCmd {
	result := make([]*protos.QdbTransactionCmd, len(stmts))
	for i, qdbStmt := range stmts {
		result[i] = qdbStmt.ToProto()
	}
	return result
}

type QdbTransaction struct {
	transactionId uuid.UUID
	commands      []QdbStatement
}

func (t *QdbTransaction) Id() uuid.UUID {
	return t.transactionId
}

func NewTransaction() (*QdbTransaction, error) {
	transactionId := uuid.New()
	return &QdbTransaction{transactionId: transactionId, commands: make([]QdbStatement, 0)}, nil
}

func NewTransactionWithCmd(transactionId uuid.UUID, commands []QdbStatement) *QdbTransaction {
	return &QdbTransaction{transactionId: transactionId, commands: commands}
}

func (t *QdbTransaction) Append(qdbCommands []QdbStatement) error {
	if len(qdbCommands) == 0 {
		return fmt.Errorf("cant't add empty list of DB changes to transaction %s", t.transactionId)
	}
	t.commands = append(t.commands, qdbCommands...)
	return nil
}

func (t *QdbTransaction) Validate() error {
	if len(t.commands) == 0 {
		return fmt.Errorf("transaction %s haven't statements", t.transactionId)
	}
	return nil
}
