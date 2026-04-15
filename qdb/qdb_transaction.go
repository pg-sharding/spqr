package qdb

import (
	"fmt"

	"github.com/google/uuid"
)

const (
	CmdPut = iota
	CmdDelete
)

type QdbStatement struct {
	CmdType int32
	Key     string
	Value   any
	// for case when qdb have more than one KV-storage
	Extension string
}

func NewQdbStatement(cmdType int32, key string, value any) (*QdbStatement, error) {
	if cmdType != CmdPut && cmdType != CmdDelete {
		return nil, fmt.Errorf("unknown type of QdbStatement: %d", cmdType)
	}
	return &QdbStatement{CmdType: cmdType, Key: key, Value: value}, nil
}

func NewQdbStatementExt(cmdType int32, key string, value any, extension string) (*QdbStatement, error) {
	if stmt, err := NewQdbStatement(cmdType, key, value); err != nil {
		return nil, err
	} else {
		stmt.Extension = extension
		return stmt, nil
	}
}

type QdbTransaction struct {
	transactionID uuid.UUID
	commands      []QdbStatement
}

func (t *QdbTransaction) ID() uuid.UUID {
	return t.transactionID
}

func NewTransaction() (*QdbTransaction, error) {
	transactionID := uuid.New()
	return &QdbTransaction{transactionID: transactionID, commands: make([]QdbStatement, 0)}, nil
}

func NewTransactionWithCmd(transactionID uuid.UUID, commands []QdbStatement) *QdbTransaction {
	return &QdbTransaction{transactionID: transactionID, commands: commands}
}

func (t *QdbTransaction) Append(qdbCommands []QdbStatement) error {
	if len(qdbCommands) == 0 {
		return fmt.Errorf("cant't add empty list of DB changes to transaction %s", t.transactionID)
	}
	t.commands = append(t.commands, qdbCommands...)
	return nil
}

func (t *QdbTransaction) Validate() error {
	if len(t.commands) == 0 {
		return fmt.Errorf("transaction %s haven't statements", t.transactionID)
	}
	return nil
}
