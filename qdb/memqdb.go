package qdb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

type MemQDB struct {
	// TODO create more mutex per map if needed
	mu sync.RWMutex

	Locks                map[string]*sync.RWMutex            `json:"locks"`
	Freq                 map[string]bool                     `json:"freq"`
	Krs                  map[string]*KeyRange                `json:"krs"`
	Shards               map[string]*Shard                   `json:"shards"`
	Distributions        map[string]*Distribution            `json:"distributions"`
	RelationDistribution map[string]string                   `json:"relation_distribution"`
	Routers              map[string]*Router                  `json:"routers"`
	Transactions         map[string]*DataTransferTransaction `json:"transactions"`
	Coordinator          string                              `json:"coordinator"`
	TaskGroup            *TaskGroup                          `json:"taskGroup"`

	backupPath string
	/* caches */
}

var _ XQDB = &MemQDB{}

func NewMemQDB(backupPath string) (*MemQDB, error) {
	return &MemQDB{
		Freq:                 map[string]bool{},
		Krs:                  map[string]*KeyRange{},
		Locks:                map[string]*sync.RWMutex{},
		Shards:               map[string]*Shard{},
		Distributions:        map[string]*Distribution{},
		RelationDistribution: map[string]string{},
		Routers:              map[string]*Router{},
		Transactions:         map[string]*DataTransferTransaction{},

		backupPath: backupPath,
	}, nil
}

// TODO : unit tests
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

// TODO : unit tests
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
//                               KEY RANGE MOVES
// ==============================================================================

func (q *MemQDB) RecordKeyRangeMove(ctx context.Context, m *MoveKeyRange) error {
	// TODO implement
	return nil
}

func (q *MemQDB) ListKeyRangeMoves(ctx context.Context) ([]*MoveKeyRange, error) {
	// TODO implement
	return nil, nil
}

func (q *MemQDB) UpdateKeyRangeMoveStatus(ctx context.Context, moveId string, s MoveKeyRangeStatus) error {
	// TODO implement
	return nil
}

func (q *MemQDB) DeleteKeyRangeMove(ctx context.Context, moveId string) error {
	// TODO implement
	return nil
}

// ==============================================================================
//                                 KEY RANGES
// ==============================================================================

// TODO : unit tests
func (q *MemQDB) CreateKeyRange(_ context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().Interface("key-range", keyRange).Msg("memqdb: add key range")

	q.mu.RLock()
	if _, ok := q.Krs[keyRange.KeyRangeID]; ok {
		q.mu.RUnlock()
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range \"%s\" already exists", keyRange.KeyRangeID)
	}
	q.mu.RUnlock()

	q.mu.Lock()
	defer q.mu.Unlock()

	if len(keyRange.DistributionId) > 0 && keyRange.DistributionId != "default" {
		if _, ok := q.Distributions[keyRange.DistributionId]; !ok {
			return spqrerror.New(spqrerror.SPQR_NO_DISTRIBUTION, fmt.Sprintf("no such distribution %s", keyRange.DistributionId))
		}
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Krs, keyRange.KeyRangeID, keyRange),
		NewUpdateCommand(q.Locks, keyRange.KeyRangeID, &sync.RWMutex{}),
		NewUpdateCommand(q.Freq, keyRange.KeyRangeID, false))
}

func (q *MemQDB) GetKeyRange(_ context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: get key range")
	q.mu.RLock()
	defer q.mu.RUnlock()

	krs, ok := q.Krs[id]
	if !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "there is no key range %s", id)
	}

	return krs, nil
}

// TODO : unit tests
func (q *MemQDB) UpdateKeyRange(_ context.Context, keyRange *KeyRange) error {
	spqrlog.Zero.Debug().Interface("key-range", keyRange).Msg("memqdb: update key range")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Krs, keyRange.KeyRangeID, keyRange))
}

// TODO : unit tests
func (q *MemQDB) DropKeyRange(_ context.Context, id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: drop key range")

	q.mu.Lock()
	defer q.mu.Unlock()

	lock, ok := q.Locks[id]
	if !ok {
		return nil
	}
	if !lock.TryLock() {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range \"%s\" is locked", id)
	}
	defer lock.Unlock()

	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.Krs, id),
		NewDeleteCommand(q.Freq, id), NewDeleteCommand(q.Locks, id))
}

// TODO : unit tests
func (q *MemQDB) DropKeyRangeAll(_ context.Context) error {
	spqrlog.Zero.Debug().Msg("memqdb: drop all key ranges")
	q.mu.Lock()
	defer q.mu.Unlock()

	// Wait until key range will be unlocked
	var locks []*sync.RWMutex
	defer func() {
		for _, l := range locks {
			l.Unlock()
		}
	}()
	for krId, l := range q.Locks {
		if !l.TryLock() {
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range \"%s\" is locked", krId)
		}
		locks = append(locks, l)
	}
	spqrlog.Zero.Debug().Msg("memqdb: acquired all locks")

	return ExecuteCommands(q.DumpState, NewDropCommand(q.Krs), NewDropCommand(q.Locks))
}

// TODO : unit tests
func (q *MemQDB) ListKeyRanges(_ context.Context, distribution string) ([]*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("distribution", distribution).
		Msg("memqdb: list key ranges")
	q.mu.RLock()
	defer q.mu.RUnlock()

	var ret []*KeyRange

	for _, el := range q.Krs {
		if el.DistributionId == distribution {
			ret = append(ret, el)
		}
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].KeyRangeID < ret[j].KeyRangeID
	})

	return ret, nil
}

// TODO : unit tests
func (q *MemQDB) ListAllKeyRanges(_ context.Context) ([]*KeyRange, error) {
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

// TODO : unit tests
func (q *MemQDB) TryLockKeyRange(lock *sync.RWMutex, id string, read bool) error {
	res := false
	if read {
		res = lock.TryRLock()
	} else {
		res = lock.TryLock()
	}
	if !res {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range \"%s\" is locked", id)
	}

	if _, ok := q.Krs[id]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range '%s' deleted after lock acquired", id)
	}
	return nil
}

// TODO : unit tests
func (q *MemQDB) LockKeyRange(_ context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: lock key range")
	q.mu.RLock()
	defer q.mu.RUnlock()
	defer spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: exit: lock key range")

	krs, ok := q.Krs[id]
	if !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range '%s' does not exist", id)
	}

	err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.Freq, id, true),
		NewCustomCommand(func() error {
			if lock, ok := q.Locks[id]; ok {
				return q.TryLockKeyRange(lock, id, false)
			}
			return nil
		}, func() error {
			if lock, ok := q.Locks[id]; ok {
				lock.Unlock()
			}
			return nil
		}))
	if err != nil {
		return nil, err
	}

	return krs, nil
}

// TODO : unit tests
func (q *MemQDB) UnlockKeyRange(_ context.Context, id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: unlock key range")
	q.mu.RLock()
	defer q.mu.RUnlock()
	defer spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: exit: unlock key range")

	if !q.Freq[id] {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v not locked", id)
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Freq, id, false),
		NewCustomCommand(func() error {
			if lock, ok := q.Locks[id]; ok {
				lock.Unlock()
			}
			return nil
		}, func() error {
			if lock, ok := q.Locks[id]; ok {
				return q.TryLockKeyRange(lock, id, false)
			}
			return nil
		}))
}

// TODO : unit tests
func (q *MemQDB) CheckLockedKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: check locked key range")
	q.mu.RLock()
	defer q.mu.RUnlock()

	krs, err := q.GetKeyRange(ctx, id)
	if err != nil {
		return nil, err
	}

	if !q.Freq[id] {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v not locked", id)
	}

	return krs, nil
}

// TODO : unit tests
func (q *MemQDB) ShareKeyRange(id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: sharing key with key")

	q.mu.RLock()
	defer q.mu.RUnlock()

	lock, ok := q.Locks[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, "no such key")
	}

	err := q.TryLockKeyRange(lock, id, true)
	if err != nil {
		return err
	}
	defer lock.RUnlock()

	return nil
}

// ==============================================================================
//                           Transfer transactions
// ==============================================================================

// TODO : unit tests
func (q *MemQDB) RecordTransferTx(_ context.Context, key string, info *DataTransferTransaction) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Transactions, key, info))
}

// TODO : unit tests
func (q *MemQDB) GetTransferTx(_ context.Context, key string) (*DataTransferTransaction, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	ans, ok := q.Transactions[key]
	if !ok {
		return nil, nil
	}
	return ans, nil
}

// TODO : unit tests
func (q *MemQDB) RemoveTransferTx(_ context.Context, key string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.Transactions, key))
}

// ==============================================================================
//	                           COORDINATOR LOCK
// ==============================================================================

func (q *MemQDB) TryCoordinatorLock(_ context.Context) error {
	return nil
}

// TODO : unit tests
func (q *MemQDB) UpdateCoordinator(_ context.Context, address string) error {
	spqrlog.Zero.Debug().Str("address", address).Msg("memqdb: update coordinator address")

	q.mu.Lock()
	defer q.mu.Unlock()

	q.Coordinator = address
	return nil
}

func (q *MemQDB) GetCoordinator(ctx context.Context) (string, error) {
	spqrlog.Zero.Debug().Str("address", q.Coordinator).Msg("memqdb: get coordinator address")
	return q.Coordinator, nil
}

// ==============================================================================
//                                  ROUTERS
// ==============================================================================

// TODO : unit tests
func (q *MemQDB) AddRouter(_ context.Context, r *Router) error {
	spqrlog.Zero.Debug().Interface("router", r).Msg("memqdb: add router")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Routers, r.ID, r))
}

// TODO : unit tests
func (q *MemQDB) DeleteRouter(_ context.Context, id string) error {
	spqrlog.Zero.Debug().Str("router", id).Msg("memqdb: delete router")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.Routers, id))
}

// TODO : unit tests
func (q *MemQDB) OpenRouter(_ context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("router", id).
		Msg("memqdb: open router")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.Routers[id].State = OPENED

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Routers, id, q.Routers[id]))
}

// TODO : unit tests
func (q *MemQDB) CloseRouter(_ context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("router", id).
		Msg("memqdb: open router")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.Routers[id].State = CLOSED

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Routers, id, q.Routers[id]))
}

// TODO : unit tests
func (q *MemQDB) ListRouters(_ context.Context) ([]*Router, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list routers")
	q.mu.RLock()
	defer q.mu.RUnlock()

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

// ==============================================================================
//                                  SHARDS
// ==============================================================================

// TODO : unit tests
func (q *MemQDB) AddShard(_ context.Context, shard *Shard) error {
	spqrlog.Zero.Debug().Interface("shard", shard).Msg("memqdb: add shard")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Shards, shard.ID, shard))
}

// TODO : unit tests
func (q *MemQDB) ListShards(_ context.Context) ([]*Shard, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list shards")
	q.mu.RLock()
	defer q.mu.RUnlock()

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

// TODO : unit tests
func (q *MemQDB) GetShard(_ context.Context, id string) (*Shard, error) {
	spqrlog.Zero.Debug().Str("shard", id).Msg("memqdb: get shard")
	q.mu.RLock()
	defer q.mu.RUnlock()

	if _, ok := q.Shards[id]; ok {
		return &Shard{ID: id}, nil
	}

	return nil, spqrerror.Newf(spqrerror.SPQR_NO_DATASHARD, "unknown shard %s", id)
}

// TODO : unit tests
func (q *MemQDB) DropShard(_ context.Context, id string) error {
	spqrlog.Zero.Debug().Str("shard", id).Msg("memqdb: drop shard")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.Shards, id)
	return nil
}

// ==============================================================================
//                                 DISTRIBUTIONS
// ==============================================================================

// TODO : unit tests
func (q *MemQDB) CreateDistribution(_ context.Context, distribution *Distribution) error {
	spqrlog.Zero.Debug().Interface("distribution", distribution).Msg("memqdb: add distribution")
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, r := range distribution.Relations {
		q.RelationDistribution[r.Name] = distribution.ID
		_ = ExecuteCommands(q.DumpState, NewUpdateCommand(q.RelationDistribution, r.Name, distribution.ID))
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, distribution.ID, distribution))
}

// TODO : unit tests
func (q *MemQDB) ListDistributions(_ context.Context) ([]*Distribution, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list distributions")
	q.mu.RLock()
	defer q.mu.RUnlock()
	var ret []*Distribution
	for _, v := range q.Distributions {
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	return ret, nil
}

// TODO : unit tests
func (q *MemQDB) DropDistribution(_ context.Context, id string) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: delete distribution")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.Distributions[id]; !ok {
		return spqrerror.New(spqrerror.SPQR_NO_DISTRIBUTION, "no such distribution")
	}

	for t, ds := range q.RelationDistribution {
		if ds == id {
			if err := ExecuteCommands(q.DumpState, NewDeleteCommand(q.RelationDistribution, t)); err != nil {
				return err
			}
		}
	}

	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.Distributions, id))
}

// TODO : unit tests
func (q *MemQDB) AlterDistributionAttach(_ context.Context, id string, rels []*DistributedRelation) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: attach table to distribution")
	q.mu.Lock()
	defer q.mu.Unlock()

	if ds, ok := q.Distributions[id]; !ok {
		return spqrerror.New(spqrerror.SPQR_NO_DISTRIBUTION, "no such distribution")
	} else {
		for _, r := range rels {
			if _, ok := q.RelationDistribution[r.Name]; ok {
				return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is already attached", r.Name)
			}

			ds.Relations[r.Name] = &DistributedRelation{
				Name:            r.Name,
				DistributionKey: r.DistributionKey,
			}
			q.RelationDistribution[r.Name] = id
			if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.RelationDistribution, r.Name, id)); err != nil {
				return err
			}
		}

		return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, id, ds))
	}
}

// TODO: unit tests
func (q *MemQDB) AlterDistributionDetach(_ context.Context, id string, relName string) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: attach table to distribution")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.Distributions[id]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_NO_DISTRIBUTION, "distribution \"%s\" not found", id)
	}
	delete(ds.Relations, relName)
	if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, id, ds)); err != nil {
		return err
	}

	err := ExecuteCommands(q.DumpState, NewDeleteCommand(q.RelationDistribution, relName))
	return err
}

// TODO : unit tests
func (q *MemQDB) GetDistribution(_ context.Context, id string) (*Distribution, error) {
	spqrlog.Zero.Debug().Msg("memqdb: get distribution")
	q.mu.RLock()
	defer q.mu.RUnlock()

	if ds, ok := q.Distributions[id]; !ok {
		// DEPRECATE this
		return nil, spqrerror.Newf(spqrerror.SPQR_NO_DISTRIBUTION, "distribution \"%s\" not found", id)
	} else {
		return ds, nil
	}
}

func (q *MemQDB) GetRelationDistribution(_ context.Context, relation string) (*Distribution, error) {
	spqrlog.Zero.Debug().Msg("memqdb: get distribution for table")
	q.mu.RLock()
	defer q.mu.RUnlock()

	if ds, ok := q.RelationDistribution[relation]; !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_NO_DISTRIBUTION, "distribution for relation \"%s\" not found", relation)
	} else {
		// if there is no distr by key ds
		// then we have corruption
		return q.Distributions[ds], nil
	}
}

// ==============================================================================
//                                   TASKS
// ==============================================================================

func (q *MemQDB) GetTaskGroup(_ context.Context) (*TaskGroup, error) {
	spqrlog.Zero.Debug().Msg("memqdb: get task group")
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.TaskGroup == nil {
		return &TaskGroup{
			Tasks: []*Task{},
		}, nil
	}
	return q.TaskGroup, nil
}

func (q *MemQDB) WriteTaskGroup(_ context.Context, group *TaskGroup) error {
	spqrlog.Zero.Debug().Msg("memqdb: write task group")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.TaskGroup = group
	return nil
}

func (q *MemQDB) RemoveTaskGroup(_ context.Context) error {
	spqrlog.Zero.Debug().Msg("memqdb: remove task group")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.TaskGroup = nil
	return nil
}
