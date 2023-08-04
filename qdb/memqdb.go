package qdb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type MemQDB struct {
	mu sync.RWMutex

	Locks        map[string]*sync.RWMutex            `json:"locks"`
	Freq         map[string]bool                     `json:"freq"`
	Krs          map[string]*KeyRange                `json:"krs"`
	Shards       map[string]*Shard                   `json:"shards"`
	Shrules      map[string]*ShardingRule            `json:"shrules"`
	Dataspaces   map[string]*Dataspace               `json:"dataspaces"`
	Routers      map[string]*Router                  `json:"routers"`
	Transactions map[string]*DataTransferTransaction `json:"transactions"`

	backupPath string
	/* caches */
}

var _ QDB = &MemQDB{}

func NewMemQDB(backupPath string) (*MemQDB, error) {
	return &MemQDB{
		Freq:       map[string]bool{},
		Krs:        map[string]*KeyRange{},
		Locks:      map[string]*sync.RWMutex{},
		Shards:     map[string]*Shard{},
		Shrules:    map[string]*ShardingRule{},
		Dataspaces: map[string]*Dataspace{},
		Routers:    map[string]*Router{},

		backupPath: backupPath,
	}, nil
}

func RestoreQDB(backupPath string) (*MemQDB, error) {
	qdb, err := NewMemQDB(backupPath)
	if err != nil {
		return nil, err
	}
	if backupPath == "" {
		return qdb, nil
	}
	if _, err := os.Stat(backupPath); err != nil {
		spqrlog.Zero.Info().Err(err).Msg("memqdb backup file not exists. Creating new one.")
		f, err := os.Create(backupPath)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		return qdb, nil
	}
	data, err := os.ReadFile(backupPath)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, qdb)

	for kr, locked := range qdb.Freq {
		if locked {
			qdb.Locks[kr].Lock()
		}
	}

	if err != nil {
		return nil, err
	}
	return qdb, nil
}

func (q *MemQDB) DumpState() error {
	if q.backupPath == "" {
		return nil
	}
	tmpPath := q.backupPath + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	state, err := json.MarshalIndent(q, "", "	")

	if err != nil {
		return err
	}

	_, err = f.Write(state)
	if err != nil {
		return err
	}
	f.Close()

	err = os.Rename(tmpPath, q.backupPath)
	if err != nil {
		return err
	}

	return nil
}

// ==============================================================================
//                               SHARDING RULES
// ==============================================================================

func (q *MemQDB) AddShardingRule(ctx context.Context, rule *ShardingRule) error {
	spqrlog.Zero.Debug().Interface("rule", rule).Msg("memqdb: add sharding rule")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Shrules, rule.ID, rule))
}

func (q *MemQDB) DropShardingRule(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().Str("rule", id).Msg("memqdb: drop sharding rule")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.Shrules, id))
}

func (q *MemQDB) DropShardingRuleAll(ctx context.Context) ([]*ShardingRule, error) {
	spqrlog.Zero.Debug().Msg("memqdb: drop sharding rule all")
	q.mu.Lock()
	defer q.mu.Unlock()

	var ret []*ShardingRule
	for _, v := range q.Shrules {
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	err := ExecuteCommands(q.DumpState, NewDropCommand(q.Shrules))
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (q *MemQDB) GetShardingRule(ctx context.Context, id string) (*ShardingRule, error) {
	spqrlog.Zero.Debug().Str("rule", id).Msg("memqdb: get sharding rule")
	rule, ok := q.Shrules[id]
	if ok {
		return rule, nil
	}
	return nil, fmt.Errorf("rule with id %s not found", id)
}

func (q *MemQDB) ListShardingRules(ctx context.Context) ([]*ShardingRule, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list sharding rules")
	q.mu.RLock()
	defer q.mu.RUnlock()
	var ret []*ShardingRule
	for _, v := range q.Shrules {
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	return ret, nil
}

func (q *MemQDB) MatchShardingRules(ctx context.Context, m func(shrules map[string]*ShardingRule) error) error {
	spqrlog.Zero.Debug().Msg("memqdb: list sharding rules")
	q.mu.RLock()
	defer q.mu.RUnlock()
	return m(q.Shrules)
}

// ==============================================================================
//                                 KEY RANGES
// ==============================================================================

func (q *MemQDB) AddKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().Interface("key-range", keyRange).Msg("memqdb: add key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Krs, keyRange.KeyRangeID, keyRange),
		NewUpdateCommand(q.Locks, keyRange.KeyRangeID, &sync.RWMutex{}),
		NewUpdateCommand(q.Freq, keyRange.KeyRangeID, false))
}

func (q *MemQDB) GetKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: get key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.Krs[id]
	if !ok {
		return nil, fmt.Errorf("there is no key range %s", id)
	}

	return krs, nil
}

func (q *MemQDB) UpdateKeyRange(ctx context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().Interface("key-range", keyRange).Msg("memqdb: update key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Krs, keyRange.KeyRangeID, keyRange))
}

func (q *MemQDB) DropKeyRange(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: drop key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.Krs, id),
		NewDeleteCommand(q.Freq, id), NewDeleteCommand(q.Locks, id))
}

func (q *MemQDB) DropKeyRangeAll(ctx context.Context) error {
	spqrlog.Zero.Debug().Msg("memqdb: drop all key ranges")
	q.mu.Lock()
	defer q.mu.Unlock()

	var locks []*sync.RWMutex

	return ExecuteCommands(q.DumpState,
		NewCustomCommand(func() {
			for _, l := range q.Locks {
				l.Lock()
				locks = append(locks, l)
			}
			spqrlog.Zero.Debug().Msg("memqdb: acquired all locks")
		}, func() {}),
		NewDropCommand(q.Krs), NewDropCommand(q.Locks),
		NewCustomCommand(func() {
			for _, l := range locks {
				l.Unlock()
			}
		},
			func() {}),
	)
}

func (q *MemQDB) ListKeyRanges(_ context.Context) ([]*KeyRange, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list all key ranges")
	q.mu.RLock()
	defer q.mu.RUnlock()

	var ret []*KeyRange

	for _, el := range q.Krs {
		ret = append(ret, el)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].KeyRangeID < ret[j].KeyRangeID
	})

	return ret, nil
}

func (q *MemQDB) LockKeyRange(_ context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: lock key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.Krs[id]
	if !ok {
		return nil, fmt.Errorf("key range '%s' does not exist", id)
	}

	err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.Freq, id, true),
		NewCustomCommand(q.Locks[id].Lock, q.Locks[id].Unlock))
	if err != nil {
		return nil, err
	}

	return krs, nil
}

func (q *MemQDB) UnlockKeyRange(_ context.Context, id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: lock key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.Freq[id] {
		return fmt.Errorf("key range %v not locked", id)
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Freq, id, false),
		NewCustomCommand(q.Locks[id].Unlock, q.Locks[id].Lock))
}

func (q *MemQDB) CheckLockedKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: check locked key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	krs, ok := q.Krs[id]
	if !ok {
		return nil, fmt.Errorf("no such krid")
	}

	if !q.Freq[id] {
		return nil, fmt.Errorf("key range %v not locked", id)
	}

	return krs, nil
}

func (q *MemQDB) ShareKeyRange(id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: sharing key with key")

	q.Locks[id].RLock()
	defer q.Locks[id].RUnlock()

	return nil
}

// ==============================================================================
//                           Transfer transactions
// ==============================================================================

func (q *MemQDB) RecordTransferTx(ctx context.Context, key string, info *DataTransferTransaction) error {
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Transactions, key, info))
}

func (q *MemQDB) GetTransferTx(ctx context.Context, key string) (*DataTransferTransaction, error) {
	return q.Transactions[key], nil
}

func (q *MemQDB) RemoveTransferTx(ctx context.Context, key string) error {
	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.Transactions, key))
}

// ==============================================================================
//                                  ROUTERS
// ==============================================================================

func (q *MemQDB) AddRouter(ctx context.Context, r *Router) error {
	spqrlog.Zero.Debug().Interface("router", r).Msg("memqdb: add router")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Routers, r.ID, r))
}

func (q *MemQDB) DeleteRouter(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().Str("router", id).Msg("memqdb: delete router")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.Routers, id))
}

func (q *MemQDB) ListRouters(ctx context.Context) ([]*Router, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list routers")
	q.mu.Lock()
	defer q.mu.Unlock()

	var ret []*Router
	for _, v := range q.Routers {
		// TODO replace with new
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	return ret, nil
}

func (q *MemQDB) LockRouter(ctx context.Context, id string) error {
	return nil
}

// ==============================================================================
//                                  SHARDS
// ==============================================================================

func (q *MemQDB) AddShard(ctx context.Context, shard *Shard) error {
	spqrlog.Zero.Debug().Interface("shard", shard).Msg("memqdb: add shard")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Shards, shard.ID, shard))
}

func (q *MemQDB) ListShards(ctx context.Context) ([]*Shard, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list shards")
	q.mu.Lock()
	defer q.mu.Unlock()

	var ret []*Shard
	for _, v := range q.Shards {
		// TODO replace with new
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	return ret, nil
}

func (q *MemQDB) GetShard(ctx context.Context, id string) (*Shard, error) {
	spqrlog.Zero.Debug().Str("shard", id).Msg("memqdb: get shard")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.Shards[id]; ok {
		return &Shard{ID: id}, nil
	}

	return nil, fmt.Errorf("unknown shard %s", id)
}

// ==============================================================================
//                                 DATASPACES
// ==============================================================================

func (q *MemQDB) AddDataspace(ctx context.Context, dataspace *Dataspace) error {
	spqrlog.Zero.Debug().Interface("dataspace", dataspace).Msg("memqdb: add dataspace")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Dataspaces, dataspace.ID, dataspace))
}

func (q *MemQDB) ListDataspaces(ctx context.Context) ([]*Dataspace, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list dataspaces")
	q.mu.RLock()
	defer q.mu.RUnlock()
	var ret []*Dataspace
	for _, v := range q.Dataspaces {
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	return ret, nil
}
