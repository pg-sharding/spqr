package qdb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/router/rfqn"

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
	MoveTaskGroups       map[string]*MoveTaskGroup           `json:"taskGroup"`
	StopMoveTaskGroup    map[string]bool                     `json:"stop_move_task_group"`
	MoveTask             *MoveTask                           `json:"move_task"`
	TotalKeys            map[string]int64                    `json:"total_keys"`
	RedistributeTask     *RedistributeTask                   `json:"redistribute_ask"`
	BalancerTask         *BalancerTask                       `json:"balancer_task"`
	ReferenceRelations   map[string]*ReferenceRelation       `json:"reference_relations"`
	Sequences            map[string]bool                     `json:"sequences"`
	ColumnSequence       map[string]string                   `json:"column_sequence"`
	SequenceToValues     map[string]int64                    `json:"sequence_to_values"`
	SequenceLock         sync.RWMutex

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
		Sequences:            map[string]bool{},
		ColumnSequence:       map[string]string{},
		SequenceToValues:     map[string]int64{},
		ReferenceRelations:   map[string]*ReferenceRelation{},
		MoveTaskGroups:       map[string]*MoveTaskGroup{},
		StopMoveTaskGroup:    map[string]bool{},
		TotalKeys:            map[string]int64{},

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
		spqrlog.Zero.Info().Err(err).Msg("memqdb backup file not exists. Creating new one")
		f, err := os.Create(backupPath)
		if err != nil {
			return nil, err
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				spqrlog.Zero.Debug().Err(err).Msg("failed to close file")
			}
		}(f)
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
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close file")
		}
	}(f)

	state, err := json.MarshalIndent(q, "", "	")

	if err != nil {
		return err
	}

	_, err = f.Write(state)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			spqrlog.Zero.Debug().Err(err).Msg("failed to close file")
		}
	}(f)

	err = os.Rename(tmpPath, q.backupPath)
	if err != nil {
		return err
	}

	return nil
}

// ==============================================================================
//                               MISC
// ==============================================================================

func (q *MemQDB) dropKeyRangeCommands(krId string) []Command {
	return []Command{
		NewDeleteCommand(q.Krs, krId),
		NewDeleteCommand(q.Freq, krId),
		NewDeleteCommand(q.Locks, krId),
	}
}

func (q *MemQDB) createKeyRangeCommands(keyRange *KeyRange) []Command {
	return []Command{
		NewUpdateCommand(q.Krs, keyRange.KeyRangeID, keyRange),
		NewUpdateCommand(q.Locks, keyRange.KeyRangeID, &sync.RWMutex{}),
		NewUpdateCommand(q.Freq, keyRange.KeyRangeID, false),
	}
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
			return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, fmt.Sprintf("no such distribution %s", keyRange.DistributionId))
		}
	}

	return ExecuteCommands(q.DumpState, q.createKeyRangeCommands(keyRange)...)
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

	_, ok := q.Krs[id]
	if !ok {
		return nil
	}

	lock, ok := q.Locks[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, fmt.Sprintf("no lock in MemQDB for key range \"%s\"", id))
	}
	if !lock.TryLock() {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v is locked", id)
	}
	defer lock.Unlock()

	return ExecuteCommands(q.DumpState, q.dropKeyRangeCommands(id)...)
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
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v is locked", id)
	}

	if _, ok := q.Krs[id]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range '%s' deleted after lock acquired", id)
	}
	return nil
}
func (q *MemQDB) NoWaitLockKeyRange(ctx context.Context, id string) (*KeyRange, error) {
	return q.LockKeyRange(ctx, id)
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
func (q *MemQDB) ListLockedKeyRanges(ctx context.Context) ([]string, error) {
	spqrlog.Zero.Debug().
		Str("key-range lock request", "").
		Msg("memqdb: get list locked key range")
	q.mu.RLock()
	defer q.mu.RUnlock()
	result := make([]string, 0, len(q.Locks))
	for lk, v := range q.Freq {
		if v {
			result = append(result, lk)
		}
	}
	return result, nil
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

// TODO: unit tests
func (q *MemQDB) RenameKeyRange(_ context.Context, krId, krIdNew string) error {
	spqrlog.Zero.Debug().
		Str("id", krId).
		Str("new id", krIdNew).
		Msg("memqdb: rename key range")

	q.mu.Lock()
	defer q.mu.Unlock()

	kr, ok := q.Krs[krId]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, fmt.Sprintf("key range '%s' not found", krId))
	}
	if _, ok = q.Krs[krIdNew]; ok {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, fmt.Sprintf("key range '%s' already exists", krIdNew))
	}

	kr.KeyRangeID = krIdNew
	commands := make([]Command, 0)
	commands = append(commands, q.dropKeyRangeCommands(krId)...)
	commands = append(commands, q.createKeyRangeCommands(kr)...)
	return ExecuteCommands(q.DumpState, commands...)
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

func (q *MemQDB) TryCoordinatorLock(_ context.Context, _ string) error {
	return nil
}

// TODO : unit tests
func (q *MemQDB) UpdateCoordinator(_ context.Context, address string) error {
	q.mu.Lock()
	changed := q.Coordinator != address
	q.Coordinator = address
	q.mu.Unlock()

	if changed {
		spqrlog.Zero.Debug().Str("address", address).Msg("memqdb: update coordinator address")
	}
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
func (q *MemQDB) DeleteRouterAll(_ context.Context) error {
	spqrlog.Zero.Debug().Msg("memqdb: unregister all routers")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewDropCommand(q.Routers))
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

	if _, ok := q.Shards[shard.ID]; ok {
		return fmt.Errorf("shard with id %s already exists", shard.ID)
	}

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
//                              REFERENCE RELATIONS
// ==============================================================================

// CreateReferenceRelation implements XQDB.
func (q *MemQDB) CreateReferenceRelation(ctx context.Context, r *ReferenceRelation) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ReferenceRelations[r.TableName] = r
	return nil
}

// GetReferenceRelation implements XQDB.
func (q *MemQDB) GetReferenceRelation(_ context.Context, relName *rfqn.RelationFQN) (*ReferenceRelation, error) {
	tableName := relName.RelationName

	spqrlog.Zero.Debug().Str("id", tableName).Msg("memqdb: get reference relation")
	q.mu.RLock()
	defer q.mu.RUnlock()

	if rr, ok := q.ReferenceRelations[tableName]; !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "reference relation \"%s\" not found", tableName)
	} else {
		return rr, nil
	}
}

// AlterReferenceRelationStorage implements XQDB.
func (q *MemQDB) AlterReferenceRelationStorage(ctx context.Context, relName *rfqn.RelationFQN, shs []string) error {
	tableName := relName.RelationName
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.ReferenceRelations[tableName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "reference relation \"%s\" not found", tableName)
	}
	q.ReferenceRelations[tableName].ShardIds = shs
	return nil
}

// DropReferenceRelation implements XQDB.
func (q *MemQDB) DropReferenceRelation(ctx context.Context, relName *rfqn.RelationFQN) error {
	tableName := relName.RelationName
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.ReferenceRelations[tableName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "reference relation \"%s\" not found", tableName)
	}
	delete(q.ReferenceRelations, tableName)
	return nil
}

// ListReferenceRelations implements XQDB.
func (q *MemQDB) ListReferenceRelations(ctx context.Context) ([]*ReferenceRelation, error) {
	var rrs []*ReferenceRelation
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, r := range q.ReferenceRelations {
		rrs = append(rrs, r)
	}

	return rrs, nil
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
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
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
func (q *MemQDB) AlterDistributionAttach(ctx context.Context, id string, rels []*DistributedRelation) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: attach table to distribution")
	q.mu.Lock()
	defer q.mu.Unlock()

	if ds, ok := q.Distributions[id]; !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	} else {
		for _, r := range rels {
			if _, ok := q.RelationDistribution[r.Name]; ok {
				return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is already attached", r.Name)
			}

			ds.Relations[r.Name] = r
			q.RelationDistribution[r.Name] = id
			if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.RelationDistribution, r.Name, id)); err != nil {
				return err
			}
		}

		return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, id, ds))
	}
}

// TODO: unit tests
func (q *MemQDB) AlterDistributionDetach(ctx context.Context, id string, relName *rfqn.RelationFQN) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: attach table to distribution")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.Distributions[id]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution \"%s\" not found", id)
	}

	if err := q.AlterSequenceDetachRelation(ctx, relName); err != nil {
		return err
	}

	delete(ds.Relations, relName.RelationName)
	if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, id, ds)); err != nil {
		return err
	}

	err := ExecuteCommands(q.DumpState, NewDeleteCommand(q.RelationDistribution, relName.RelationName))
	return err
}

// TODO : unit tests
func (q *MemQDB) AlterDistributedRelation(ctx context.Context, id string, rel *DistributedRelation) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: alter distributed relation")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.Distributions[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}
	if dsID, ok := q.RelationDistribution[rel.Name]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", rel.Name)
	} else if dsID != id {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"", rel.Name, dsID, id)
	}

	ds.Relations[rel.Name] = rel
	if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.RelationDistribution, rel.Name, id)); err != nil {
		return err
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, id, ds))
}

// TODO : unit tests
func (q *MemQDB) AlterDistributedRelationSchema(ctx context.Context, id string, relName string, schemaName string) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: alter distributed relation schema")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.Distributions[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}
	if dsID, ok := q.RelationDistribution[relName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relName)
	} else if dsID != id {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"", relName, dsID, id)
	}

	ds.Relations[relName].SchemaName = schemaName
	if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.RelationDistribution, relName, id)); err != nil {
		return err
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, id, ds))
}

// TODO : unit tests
func (q *MemQDB) AlterReplicatedRelationSchema(ctx context.Context, id string, relName string, schemaName string) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: alter distributed relation schema")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.Distributions[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}
	if dsID, ok := q.RelationDistribution[relName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relName)
	} else if dsID != id {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"", relName, dsID, id)
	}

	rel, ok := q.ReferenceRelations[relName]
	if !ok {
		return fmt.Errorf("reference relation \"%s\" not found", relName)
	}

	ds.Relations[relName].SchemaName = schemaName
	rel.SchemaName = schemaName

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, id, ds), NewUpdateCommand(q.ReferenceRelations, relName, rel))
}

// TODO : unit tests
func (q *MemQDB) AlterDistributedRelationDistributionKey(ctx context.Context, id string, relName string, distributionKey []DistributionKeyEntry) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: alter distributed relation distribution key")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.Distributions[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}
	if dsID, ok := q.RelationDistribution[relName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relName)
	} else if dsID != id {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"", relName, dsID, id)
	}

	ds.Relations[relName].DistributionKey = distributionKey
	if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.RelationDistribution, relName, id)); err != nil {
		return err
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, id, ds))
}

// TODO : unit tests
func (q *MemQDB) GetDistribution(_ context.Context, id string) (*Distribution, error) {
	spqrlog.Zero.Debug().Str("id", id).Msg("memqdb: get distribution")
	q.mu.RLock()
	defer q.mu.RUnlock()

	if ds, ok := q.Distributions[id]; !ok {
		// DEPRECATE this
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution \"%s\" not found", id)
	} else {
		return ds, nil
	}
}

// TODO : unit tests
func (q *MemQDB) CheckDistribution(_ context.Context, id string) (bool, error) {
	spqrlog.Zero.Debug().Str("id", id).Msg("memqdb: check distribution")
	q.mu.RLock()
	defer q.mu.RUnlock()

	_, ok := q.Distributions[id]
	return ok, nil
}

func (q *MemQDB) GetRelationDistribution(_ context.Context, relation *rfqn.RelationFQN) (*Distribution, error) {
	spqrlog.Zero.Debug().Str("relation", relation.RelationName).Msg("memqdb: get distribution for table")
	q.mu.RLock()
	defer q.mu.RUnlock()

	if ds, ok := q.RelationDistribution[relation.RelationName]; !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution for relation \"%s\" not found", relation)
	} else {
		// if there is no distr by key ds
		// then we have corruption
		return q.Distributions[ds], nil
	}
}

// ==============================================================================
//                                   TASKS
// ==============================================================================

func (q *MemQDB) ListTaskGroups(_ context.Context) (map[string]*MoveTaskGroup, error) {
	spqrlog.Zero.Debug().
		Msg("memqdb: list task groups")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.MoveTaskGroups, nil
}

// TODO: unit tests
func (q *MemQDB) GetMoveTaskGroup(_ context.Context, id string) (*MoveTaskGroup, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: get task group")
	q.mu.RLock()
	defer q.mu.RUnlock()

	group, ok := q.MoveTaskGroups[id]
	if !ok {
		return nil, nil
	}
	return group, nil
}

// TODO: unit tests
func (q *MemQDB) WriteMoveTaskGroup(_ context.Context, id string, group *MoveTaskGroup, totalKeys int64, moveTask *MoveTask) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: write task group")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.MoveTaskGroups[id] = group
	q.StopMoveTaskGroup[id] = false
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.MoveTaskGroups, id, group), NewUpdateCommand(q.StopMoveTaskGroup, id, false))
}

// TODO: unit tests
func (q *MemQDB) RemoveMoveTaskGroup(_ context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: remove task group")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.MoveTaskGroups, id)
	delete(q.StopMoveTaskGroup, id)
	return nil
}

// TODO: unit tests
func (q *MemQDB) GetMoveTaskGroupTotalKeys(_ context.Context, id string) (int64, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: get task group total keys")
	q.mu.RLock()
	defer q.mu.RUnlock()

	val, ok := q.TotalKeys[id]
	if !ok {
		return 0, nil
	}
	return val, nil
}

// TODO: unit tests
func (q *MemQDB) UpdateMoveTaskGroupTotalKeys(_ context.Context, id string, totalKeys int64) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: get task group total keys")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.TotalKeys[id] = totalKeys
	return nil
}

// TODO: unit tests
func (q *MemQDB) AddMoveTaskGroupStopFlag(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: put task group stop flag")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.StopMoveTaskGroup[id] = true
	return nil
}

// TODO: unit tests
func (q *MemQDB) CheckMoveTaskGroupStopFlag(ctx context.Context, id string) (bool, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: put task group stop flag")
	q.mu.RLock()
	defer q.mu.RUnlock()

	val, ok := q.StopMoveTaskGroup[id]
	if !ok {
		return false, nil
	}

	return val, nil
}

// TODO: unit tests
func (q *MemQDB) GetMoveTask(ctx context.Context) (*MoveTask, error) {
	spqrlog.Zero.Debug().Msg("memqdb: get move task")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.MoveTask, nil
}

// TODO: unit tests
func (q *MemQDB) WriteMoveTask(ctx context.Context, task *MoveTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("memqdb: write move task")
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.MoveTask != nil {
		return fmt.Errorf("failed to write move task: another task already exists")
	}
	q.MoveTask = task
	return nil
}

// TODO: unit tests
func (q *MemQDB) UpdateMoveTask(ctx context.Context, task *MoveTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("memqdb: update move task")
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.MoveTask == nil || q.MoveTask.ID != task.ID {
		return fmt.Errorf("failed to update move task: IDs differ")
	}

	q.MoveTask = task
	return nil
}

// TODO: unit tests
func (q *MemQDB) RemoveMoveTask(ctx context.Context) error {
	spqrlog.Zero.Debug().Msg("memqdb: remove move task")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.MoveTask = nil
	return nil
}

// TODO: unit tests
func (q *MemQDB) GetRedistributeTask(_ context.Context) (*RedistributeTask, error) {
	spqrlog.Zero.Debug().Msg("memqdb: get redistribute task")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.RedistributeTask, nil
}

// TODO: unit tests
func (q *MemQDB) WriteRedistributeTask(_ context.Context, task *RedistributeTask) error {
	spqrlog.Zero.Debug().Msg("memqdb: write redistribute task")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.RedistributeTask = task
	return nil
}

// TODO: unit tests
func (q *MemQDB) RemoveRedistributeTask(_ context.Context) error {
	spqrlog.Zero.Debug().Msg("memqdb: remove redistribute task")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.RedistributeTask = nil
	return nil
}

// TODO: unit tests
func (q *MemQDB) GetBalancerTask(_ context.Context) (*BalancerTask, error) {
	spqrlog.Zero.Debug().Msg("memqdb: get balancer task")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.BalancerTask, nil
}

// TODO: unit tests
func (q *MemQDB) WriteBalancerTask(_ context.Context, task *BalancerTask) error {
	spqrlog.Zero.Debug().Msg("memqdb: write balancer task")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.BalancerTask = task
	return nil
}

// TODO: unit tests
func (q *MemQDB) RemoveBalancerTask(_ context.Context) error {
	spqrlog.Zero.Debug().Msg("memqdb: remove balancer task")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.BalancerTask = nil
	return nil
}

func (q *MemQDB) CreateSequence(_ context.Context, seqName string, initialValue int64) error {
	spqrlog.Zero.Debug().
		Str("sequence", seqName).Msg("memqdb: alter sequence attach")

	q.Sequences[seqName] = true
	q.SequenceToValues[seqName] = initialValue
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Sequences, seqName, true))
}

func (q *MemQDB) AlterSequenceAttach(_ context.Context, seqName string, relName *rfqn.RelationFQN, colName string) error {
	spqrlog.Zero.Debug().
		Str("sequence", seqName).
		Str("relation", relName.RelationName).
		Str("column", colName).Msg("memqdb: alter sequence attach")

	if _, ok := q.Sequences[seqName]; !ok {
		return fmt.Errorf("sequence %s does not exist", seqName)
	}

	key := fmt.Sprintf("%s_%s", relName, colName)
	q.ColumnSequence[key] = seqName
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.ColumnSequence, key, seqName))
}

func (q *MemQDB) AlterSequenceDetachRelation(_ context.Context, relName *rfqn.RelationFQN) error {
	spqrlog.Zero.Debug().
		Str("relation", relName.RelationName).
		Msg("memqdb: detach relation from sequence")

	for col := range q.ColumnSequence {
		rel := strings.Split(col, "_")[0]
		if rel == relName.RelationName {
			if err := ExecuteCommands(q.DumpState, NewDeleteCommand(q.ColumnSequence, col)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (q *MemQDB) DropSequence(ctx context.Context, seqName string, force bool) error {
	for col, colSeq := range q.ColumnSequence {
		if colSeq == seqName && !force {
			data := strings.Split(col, "_")
			relName := data[0]
			colName := data[1]
			return spqrerror.Newf(spqrerror.SPQR_SEQUENCE_ERROR, "column %q is attached to sequence", fmt.Sprintf("%s.%s", relName, colName))
		}
	}

	if _, ok := q.Sequences[seqName]; !ok {
		return nil
	}

	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.Sequences, seqName),
		NewDeleteCommand(q.SequenceToValues, seqName))
}

func (q *MemQDB) GetRelationSequence(_ context.Context, relName *rfqn.RelationFQN) (map[string]string, error) {
	spqrlog.Zero.Debug().
		Str("relation", relName.RelationName).
		Interface("mapping", q.ColumnSequence).Msg("memqdb: get relation sequence")

	mapping := map[string]string{}
	for key, seqName := range q.ColumnSequence {
		data := strings.Split(key, "_")
		seqRelName := data[0]
		colName := data[1]

		if seqRelName == relName.RelationName {
			mapping[colName] = seqName
		}
	}
	return mapping, nil
}

func (q *MemQDB) ListSequences(_ context.Context) ([]string, error) {
	seqNames := []string{}
	for seqName := range q.Sequences {
		seqNames = append(seqNames, seqName)
	}
	sort.Strings(seqNames)
	return seqNames, nil
}

func (q *MemQDB) NextRange(_ context.Context, seqName string, rangeSize uint64) (*SequenceIdRange, error) {
	q.SequenceLock.Lock()
	defer q.SequenceLock.Unlock()
	spqrlog.Zero.Debug().
		Str("sequence", seqName).
		Msg("memqdb: get next value for sequence")

	nextval := q.SequenceToValues[seqName] + 1

	if idRange, err := NewRangeBySize(nextval, rangeSize); err != nil {
		return nil, fmt.Errorf("invalid id-range request: current=%d, request for=%d", nextval, rangeSize)
	} else {

		q.SequenceToValues[seqName] = idRange.Right
		if errDB := ExecuteCommands(q.DumpState, NewUpdateCommand(q.SequenceToValues, seqName, idRange.Right)); errDB != nil {
			return nil, errDB
		}
		return idRange, nil
	}

}

func (q *MemQDB) CurrVal(_ context.Context, seqName string) (int64, error) {
	q.SequenceLock.Lock()
	defer q.SequenceLock.Unlock()
	spqrlog.Zero.Debug().
		Str("sequence", seqName).
		Msg("memqdb: get next value for sequence")

	next := q.SequenceToValues[seqName]
	return next, nil
}
