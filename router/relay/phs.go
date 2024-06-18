package relay

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pg-sharding/spqr/router/parser"
	"github.com/pg-sharding/spqr/router/poolmgr"
)

type CacheEntry struct {
	ps   parser.ParseState
	comm string
	err  error
}

type SimpleProtoStateHandler struct {
	cmngr poolmgr.PoolMgr

	caching bool
	cache   map[string]CacheEntry
}

// ParseSQL implements ProtoStateHandler.
func (s *SimpleProtoStateHandler) ParseSQL(rst RelayStateMgr, query string) (parser.ParseState, string, error) {
	if !s.caching {
		return rst.Parse(query)
	} else {
		if ce, ok := s.cache[query]; ok {
			return ce.ps, ce.comm, ce.err
		} else {
			st, comm, err := rst.Parse(query)
			if err != nil {
				s.cache[query] = CacheEntry{
					ps:   st,
					comm: comm,
					err:  err,
				}
			}
			return st, comm, err
		}
	}
}

// query in commit query. maybe commit or commit `name`
func (s *SimpleProtoStateHandler) ExecCommit(rst RelayStateMgr, query string) error {
	if !s.cmngr.ConnectionActive(rst) {
		return fmt.Errorf("client relay has no connection to shards")
	}
	rst.AddQuery(&pgproto3.Query{
		String: query,
	})
	ok, err := rst.ProcessMessageBuf(true, true, false, s.cmngr)
	if ok {
		rst.Client().CommitActiveSet()
	}
	return err
}

func (s *SimpleProtoStateHandler) ExecRollback(rst RelayStateMgr, query string) error {
	rst.AddQuery(&pgproto3.Query{
		String: query,
	})
	ok, err := rst.ProcessMessageBuf(true, true, false, s.cmngr)
	if ok {
		rst.Client().Rollback()
	}
	return err
}

func (s *SimpleProtoStateHandler) ExecSet(rst RelayStateMgr, query string, name, value string) error {
	if len(name) == 0 {
		// some session charactericctic, ignore
		return rst.Client().ReplyCommandComplete("SET")
	}
	if !s.cmngr.ConnectionActive(rst) {
		rst.Client().SetParam(name, value)
		return rst.Client().ReplyCommandComplete("SET")
	}
	spqrlog.Zero.Debug().Str("name", name).Str("value", value).Msg("execute set query")
	rst.AddQuery(&pgproto3.Query{String: query})
	if ok, err := rst.ProcessMessageBuf(true, true, false, s.cmngr); err != nil {
		return err
	} else if ok {
		rst.Client().SetParam(name, value)
	}
	return nil
}

func (s *SimpleProtoStateHandler) ExecReset(rst RelayStateMgr, query, setting string) error {
	if s.cmngr.ConnectionActive(rst) {
		return rst.ProcessMessage(rst.Client().ConstructClientParams(), true, false, s.cmngr)
	}
	return nil
}

func (s *SimpleProtoStateHandler) ExecResetMetadata(rst RelayStateMgr, query string, setting string) error {
	if !s.cmngr.ConnectionActive(rst) {

		return nil
	}
	rst.AddQuery(&pgproto3.Query{String: query})
	_, err := rst.ProcessMessageBuf(true, true, false, s.cmngr)
	if err != nil {
		return err
	}

	rst.Client().ResetParam(setting)
	if setting == "session_authorization" {
		rst.Client().ResetParam("role")
	}
	return nil
}

func (s *SimpleProtoStateHandler) ExecSetLocal(rst RelayStateMgr, query, name, value string) error {
	if s.cmngr.ConnectionActive(rst) {
		rst.AddQuery(&pgproto3.Query{String: query})
		_, err := rst.ProcessMessageBuf(true, true, false, s.cmngr)
		return err
	}
	return nil
}

var _ ProtoStateHandler = &SimpleProtoStateHandler{}

func NewSimpleProtoStateHandler(cmngr poolmgr.PoolMgr, caching bool) ProtoStateHandler {
	return &SimpleProtoStateHandler{
		cmngr:   cmngr,
		caching: caching,
		cache:   map[string]CacheEntry{},
	}
}
