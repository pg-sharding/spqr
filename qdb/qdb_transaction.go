package qdb

import (
	"fmt"

	"github.com/google/uuid"
)

const (
	CmdPut = iota
	CmdDelete
	CmdV2
)

const (
	DropKR = iota
)

const DefaultMaxTxnSize uint16 = 128 // like ETCD max-txn-ops default value

type QdbStatement struct {
	CmdType int32
	SubType uint32
	Key     string
	Value   any
	Version uint64
	// XXX: change this to []byte
	Payload string
}

func NewQdbStatement(cmdType int32, key string, value any) (*QdbStatement, error) {
	if cmdType != CmdPut && cmdType != CmdDelete {
		return nil, fmt.Errorf("unknown type of QdbStatement: %d", cmdType)
	}
	return &QdbStatement{CmdType: cmdType, Key: key, Value: value}, nil
}

func NewQLogRecordV2(subType uint32, payload string) QdbStatement {
	return QdbStatement{CmdType: CmdV2, SubType: subType, Payload: payload}
}

func NewQdbStatementExt(cmdType int32, key string, value any, extension string) (*QdbStatement, error) {
	if stmt, err := NewQdbStatement(cmdType, key, value); err != nil {
		return nil, err
	} else {
		stmt.Payload = extension
		return stmt, nil
	}
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
