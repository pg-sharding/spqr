package transaction

import (
	"context"
	"fmt"
	"sync"

	"github.com/pg-sharding/spqr/pkg/meta"
)

type ConsoleSession struct {
	mu      sync.Mutex
	inTx    bool
	snapMgr meta.EntityMgr // snapshot-backed manager, nil when not in tx
	buffer  []*XRecord
}

func (s *ConsoleSession) IsInTx() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inTx
}

func (s *ConsoleSession) EffectiveMgr(realMgr meta.EntityMgr) meta.EntityMgr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inTx && s.snapMgr != nil {
		return s.snapMgr
	}
	return realMgr
}

func (s *ConsoleSession) Begin(ctx context.Context, realMgr meta.EntityMgr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inTx {
		return fmt.Errorf("there is already a transaction in progress")
	}
	buf := make([]*XRecord, 0)
	snapMgr, err := createSnapshotMgr(realMgr, buf)
	if err != nil {
		return err
	}
	s.inTx = true
	s.buffer = buf
	s.snapMgr = snapMgr
	return nil
}

func (s *ConsoleSession) Commit(ctx context.Context, realMgr meta.EntityMgr) error {
	s.mu.Lock()
	if !s.inTx {
		s.mu.Unlock()
		return fmt.Errorf("there is no transaction in progress")
	}
	s.inTx = false
	s.snapMgr = nil
	s.buffer = nil
	s.mu.Unlock()

	for _, rec := range s.buffer {
		if err := meta.ApplyXRecords(ctx, realMgr, rec); err != nil {
			return fmt.Errorf("commit failed on %s: %w", rec.MethodName, err)
		}
	}
	return nil
}

func (s *ConsoleSession) Rollback() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inTx = false
	s.snapMgr = nil
	s.buffer = nil
}
