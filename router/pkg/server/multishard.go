package server

import (
	"crypto/tls"
	"fmt"
	"github.com/jackc/pgproto3/v2"
	"golang.org/x/xerrors"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/pkg/datashard"
)

type multiShardResponseState int

const (
	RowDescriptorsState = multiShardResponseState(iota)
	DataRowsState       = multiShardResponseState(iota)
	CCState             = multiShardResponseState(iota)
	RFQState            = multiShardResponseState(iota)
	ErrState            = multiShardResponseState(iota)
)

type MultiShardConnState struct {
	rstate      multiShardResponseState
	messageChan chan pgproto3.BackendMessage
	errChan     chan error
}

type MultiShardServer struct {
	rule         *config.BERule
	activeShards []datashard.Shard
	shardsState  map[string]*MultiShardConnState

	pool datashard.DBPool

	state multiShardResponseState
}

func NewMultiShardServer(rule *config.BERule, pool datashard.DBPool) (Server, error) {
	ret := &MultiShardServer{
		rule:         rule,
		pool:         pool,
		activeShards: []datashard.Shard{},
		state:        RowDescriptorsState,
		shardsState:  map[string]*MultiShardConnState{},
	}

	return ret, nil
}

func (m *MultiShardServer) HasPrepareStatement(hash uint64) bool {
	panic("implement me")
}

func (m *MultiShardServer) PrepareStatement(hash uint64) {}

func (m *MultiShardServer) Reset() error {
	return nil
}

func (m *MultiShardServer) AddShard(shkey kr.ShardKey) error {
	sh, err := m.pool.Connection(shkey, m.rule)
	if err != nil {
		return err
	}

	m.activeShards = append(m.activeShards, sh)
	return nil
}

func (m *MultiShardServer) UnRouteShard(sh kr.ShardKey) error {

	for _, activeShard := range m.activeShards {
		if activeShard.Name() == sh.Name {
			return nil
		}
	}

	return xerrors.New("unrouted datashard does not match any of active")
}

func (m *MultiShardServer) AddTLSConf(cfg *tls.Config) error {
	for _, shard := range m.activeShards {
		_ = shard.ReqBackendSsl(cfg)
	}

	return nil
}

func (m *MultiShardServer) Send(msg pgproto3.FrontendMessage) error {
	for _, shard := range m.activeShards {
		spqrlog.Logger.Printf(spqrlog.DEBUG4, "sending %+v to sh %v", msg, shard.Name())
		err := shard.Send(msg)
		if err != nil {
			spqrlog.Logger.PrintError(err)
			return err
		}
	}

	return nil
}

func (m *MultiShardServer) Receive() (pgproto3.BackendMessage, error) {

	// we expect result in format
	// shard1: (RowDescription, DataRow1, DataRow2, ..., DataRowN1, CommandComplete, RFQ),
	// shard2: (RowDescription, DataRow1, DataRow2, ..., DataRowN2, CommandComplete, RFQ),
	// .....
	// shardM: (RowDescription, DataRow1, DataRow2, ..., DataRowNM, CommandComplete, RFQ)

	// step1: collect row descriptions

	switch m.state {
	case RowDescriptorsState:
		var rd *pgproto3.RowDescription
		for i, shard := range m.activeShards {
			msg, err := shard.Receive()
			if err != nil {
				return nil, err
			}
			switch q := msg.(type) {
			case *pgproto3.RowDescription:
				if i == 0 {
					// copy row descr
					*rd = *q
				} else {
					if len(rd.Fields) != len(q.Fields) {
						return nil, fmt.Errorf("incompatable row descriptions from shards")
					} else {
						for j, fd := range q.Fields {
							if rd.Fields[j].DataTypeOID != fd.DataTypeOID {
								return nil, fmt.Errorf("incompatable field descriptions from shards")
							}
						}
					}
				}

			default:
				return nil, fmt.Errorf("unexpected message type %T from shard %v while collecting row descriptions", msg, shard.Name())
			}
			m.shardsState[shard.Name()] = &MultiShardConnState{
				rstate:      RowDescriptorsState,
				messageChan: make(chan pgproto3.BackendMessage),
			}
		}
		// run shard data row goroutines
		for _, shard := range m.activeShards {
			shastate := m.shardsState[shard.Name()]
			shastate.rstate = DataRowsState
			go func(shard datashard.Shard, shastate *MultiShardConnState) {
				for {
					msg, err := shard.Receive()
					if err != nil {
						shastate.rstate = ErrState
						shastate.errChan <- err
						return
					}

					switch q := msg.(type) {
					case *pgproto3.DataRow:
						shastate.messageChan <- q
					case *pgproto3.CommandComplete:
						close(shastate.messageChan)
						close(shastate.errChan)
						shastate.rstate = CCState
						return
					default:
						shastate.errChan <- fmt.Errorf("unexpected msg in flow %T", msg)
						return
					}
				}
			}(shard, shastate)
		}
		m.state = DataRowsState
		return rd, nil
	case DataRowsState:

		for {
			cntClosed := 0
			for _, shard := range m.shardsState {
				switch shard.rstate {
				case CCState:
					cntClosed++
				case DataRowsState:
					select {
					case msg := <-shard.messageChan:
						return msg, nil
					default:
						// continue
					}
				case ErrState:
					select {
					// abort here
					case err := <-shard.errChan:
						return nil, err
					default:
						return nil, fmt.Errorf("unexpected state %d", shard.rstate)
					}
				}
			}
			if cntClosed == len(m.activeShards) {
				m.state = CCState
				return &pgproto3.CommandComplete{
					CommandTag: []byte("multiselect"),
				}, nil
			}
		}
	case CCState:
		var rfq *pgproto3.ReadyForQuery
		for i, shard := range m.activeShards {
			// state is CCState
			msg, err := shard.Receive()
			if err != nil {
				return nil, err
			}
			switch q := msg.(type) {
			case *pgproto3.ReadyForQuery:
				if i == 0 {
					rfq = q
				} else if q.TxStatus != rfq.TxStatus {
					return nil, fmt.Errorf("unexpected tx status %v differ from %v", q.TxStatus, rfq.TxStatus)
				}
				break
			default:
				return nil, fmt.Errorf("unexpected msg %T", msg)
			}
		}

		return rfq, nil
	default:
		spqrlog.Logger.Printf(spqrlog.DEBUG5, "broken multi shard connection state %v", m.state)
		return nil, fmt.Errorf("broken mutlishard connection state %v", m.state)
	}
}

func (m *MultiShardServer) Cleanup() error {

	if m.rule.PoolRollback {
		if err := m.Send(&pgproto3.Query{
			String: "ROLLBACK",
		}); err != nil {
			return err
		}
	}

	if m.rule.PoolDiscard {
		if err := m.Send(&pgproto3.Query{
			String: "DISCARD ALL",
		}); err != nil {
			return err
		}
	}

	m.shardsState = map[string]*MultiShardConnState{}
	m.state = RowDescriptorsState

	return nil
}

func (m *MultiShardServer) Sync() int {
	//TODO implement me
	panic("implement me")
}

var _ Server = &MultiShardServer{}
