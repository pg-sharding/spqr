package qdb

import (
	"fmt"

	"github.com/google/uuid"
)

const (
	CmdPut = iota
	CmdDelete
)

const DefaultMaxTxnSize uint16 = 128 // like ETCD max-txn-ops default value

type XRecord struct {
	CmdType int32
	Key     string
	Value   any
	// for case when qdb have more than one KV-storage
	Extension string
}

func NewQdbStatement(cmdType int32, key string, value any) (*XRecord, error) {
	if cmdType != CmdPut && cmdType != CmdDelete {
		return nil, fmt.Errorf("unknown type of QdbStatement: %d", cmdType)
	}
	return &XRecord{CmdType: cmdType, Key: key, Value: value}, nil
}

func NewQdbStatementExt(cmdType int32, key string, value any, extension string) (*XRecord, error) {
	if stmt, err := NewQdbStatement(cmdType, key, value); err != nil {
		return nil, err
	} else {
		stmt.Extension = extension
		return stmt, nil
	}
}

type QdbTransaction struct {
	transactionId uuid.UUID
	commands      []XRecord
}

func (t *QdbTransaction) Id() uuid.UUID {
	return t.transactionId
}

func NewTransaction() (*QdbTransaction, error) {
	transactionId := uuid.New()
	return &QdbTransaction{transactionId: transactionId, commands: make([]XRecord, 0)}, nil
}

func NewTransactionWithCmd(transactionId uuid.UUID, commands []XRecord) *QdbTransaction {
	return &QdbTransaction{transactionId: transactionId, commands: commands}
}

func (t *QdbTransaction) Append(qdbCommands []XRecord) error {
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
