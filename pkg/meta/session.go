package meta

import (
	"context"
	"fmt"
	"sync"
)

func NewConsoleSession(mgr EntityMgr) *ConsoleSession {
	return &ConsoleSession{
		mgr:  mgr,
		mu:   sync.Mutex{},
		inTx: false,
	}
}

type ConsoleSession struct {
	mu   sync.Mutex
	inTx bool

	mgr   EntityMgr
	txMgr EntityMgr
}

func (s *ConsoleSession) IsInTx() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inTx
}

func (s *ConsoleSession) SetMgr(mgr EntityMgr) {
	s.mgr = mgr
}

func (s *ConsoleSession) EffectiveMgr() EntityMgr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.txMgr != nil {
		return s.txMgr
	}
	return s.mgr
}

func (s *ConsoleSession) Begin(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inTx {
		return fmt.Errorf("there is already a transaction in progress")
	}

	txMgr := s.mgr.Snapshot()
	if err := txMgr.Begin(ctx); err != nil {
		return err
	}
	s.txMgr = txMgr
	s.inTx = true
	return nil
}

func (s *ConsoleSession) Commit(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inTx {
		return fmt.Errorf("there is no transaction in progress")
	}

	txMgr := s.txMgr
	s.txMgr = nil
	s.inTx = false

	xrecords := txMgr.XRecords()
	if err := txMgr.Commit(ctx); err != nil {
		return err
	}

	return s.mgr.ApplyXRecords(ctx, xrecords)
}

func (s *ConsoleSession) Rollback(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.inTx {
		return fmt.Errorf("there is no transaction in progress")
	}

	txMgr := s.txMgr

	if err := txMgr.Rollback(ctx); err != nil {
		return err
	}
	s.txMgr = nil
	s.inTx = false
	return nil
}
