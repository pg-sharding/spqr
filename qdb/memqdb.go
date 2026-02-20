package qdb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/router/rfqn"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
)

const (
	// maps of MemQDB as `extensions` of QdbStatement
	MapRelationDistribution = "RelationDistribution"
	MapDistributions        = "Distributions"
	MapKrs                  = "Krs"
	MapFreq                 = "Freq"
	MapLocks                = "Locks"
	MapKrVersions           = "KrVersions"
)

type MemQDB struct {
	// TODO create more mutex per map if needed
	mu sync.RWMutex

	Locks                       map[string]*sync.RWMutex            `json:"locks"`
	Freq                        map[string]bool                     `json:"freq"`
	Krs                         map[string]*internalKeyRange        `json:"krs"`
	KrVersions                  map[string]int                      `json:"kr_versions"`
	Shards                      map[string]*Shard                   `json:"shards"`
	Distributions               map[string]*Distribution            `json:"distributions"`
	RelationDistribution        map[string]string                   `json:"relation_distribution"`
	Routers                     map[string]*Router                  `json:"routers"`
	Transactions                map[string]*DataTransferTransaction `json:"transactions"`
	Coordinator                 string                              `json:"coordinator"`
	MoveTaskGroups              map[string]*MoveTaskGroup           `json:"taskGroup"`
	TaskGroupIDToStatus         map[string]*TaskGroupStatus         `json:"task_group_statuses"`
	StopMoveTaskGroup           map[string]bool                     `json:"stop_move_task_group"`
	MoveTasks                   map[string]*MoveTask                `json:"move_tasks"`
	TotalKeys                   map[string]int64                    `json:"total_keys"`
	RedistributeTasks           map[string]*RedistributeTask        `json:"redistribute_tasks"`
	RedistributeTaskTaskGroupId map[string]string                   `json:"redistribute_task_task_group"`
	KeyRangeRedistributeTasks   map[string]string                   `json:"key_range_redistribute_tasks"`
	BalancerTask                *BalancerTask                       `json:"balancer_task"`
	ReferenceRelations          map[string]*ReferenceRelation       `json:"reference_relations"`
	Sequences                   map[string]bool                     `json:"sequences"`
	ColumnSequence              map[string]string                   `json:"column_sequence"`
	SequenceToValues            map[string]int64                    `json:"sequence_to_values"`
	TaskGroupMoveTaskID         map[string]string                   `json:"task_group_move_task"`
	UniqueIndexes               map[string]*UniqueIndex             `json:"unique_indexes"`
	UniqueIndexesByRel          map[string]map[string]*UniqueIndex  `json:"unique_indexes_by_relation"`

	TwoPhaseTx map[string]*TwoPCInfo `json:"two_phase_info"`

	SequenceLock sync.RWMutex

	backupPath        string
	activeTransaction uuid.UUID
	/* caches */
}

var _ XQDB = &MemQDB{}
var _ DCStateKeeper = &MemQDB{}

func NewMemQDB(backupPath string) (*MemQDB, error) {
	return &MemQDB{
		Freq:                        map[string]bool{},
		Krs:                         map[string]*internalKeyRange{},
		KrVersions:                  map[string]int{},
		Locks:                       map[string]*sync.RWMutex{},
		Shards:                      map[string]*Shard{},
		Distributions:               map[string]*Distribution{},
		RelationDistribution:        map[string]string{},
		Routers:                     map[string]*Router{},
		Transactions:                map[string]*DataTransferTransaction{},
		Sequences:                   map[string]bool{},
		ColumnSequence:              map[string]string{},
		SequenceToValues:            map[string]int64{},
		ReferenceRelations:          map[string]*ReferenceRelation{},
		MoveTaskGroups:              map[string]*MoveTaskGroup{},
		RedistributeTasks:           map[string]*RedistributeTask{},
		RedistributeTaskTaskGroupId: map[string]string{},
		TaskGroupIDToStatus:         map[string]*TaskGroupStatus{},
		StopMoveTaskGroup:           map[string]bool{},
		TotalKeys:                   map[string]int64{},
		MoveTasks:                   map[string]*MoveTask{},
		TwoPhaseTx:                  map[string]*TwoPCInfo{},
		UniqueIndexes:               map[string]*UniqueIndex{},
		UniqueIndexesByRel:          map[string]map[string]*UniqueIndex{},

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
		NewDeleteCommand(q.KrVersions, krId),
	}
}

func (q *MemQDB) createKeyRangeCommands(keyRange *KeyRange) []Command {
	return []Command{
		NewUpdateCommand(q.Krs, keyRange.KeyRangeID, keyRangeToInternal(keyRange)),
		NewUpdateCommand(q.Locks, keyRange.KeyRangeID, &sync.RWMutex{}),
		NewUpdateCommand(q.Freq, keyRange.KeyRangeID, keyRange.Locked),
		NewUpdateCommand(q.KrVersions, keyRange.KeyRangeID, 1),
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

func (q *MemQDB) createKeyRangeQdbStatements(keyRange *KeyRange) ([]QdbStatement, error) {
	commands := make([]QdbStatement, 5)
	if keyRangeJSON, err := json.Marshal(*keyRange); err != nil {
		return nil, err
	} else {
		cmd, err := NewQdbStatementExt(CMD_CMP_VERSION, keyRange.KeyRangeID, 0, MapKrVersions)
		if err != nil {
			return nil, err
		}
		commands[0] = *cmd
		if cmd, err = NewQdbStatementExt(CMD_PUT, keyRange.KeyRangeID, string(keyRangeJSON), MapKrs); err != nil {
			return nil, err
		}
		commands[1] = *cmd
		if cmd, err = NewQdbStatementExt(CMD_PUT, keyRange.KeyRangeID, strconv.FormatBool(keyRange.Locked), MapLocks); err != nil {
			return nil, err
		}
		commands[2] = *cmd
		if cmd, err = NewQdbStatementExt(CMD_PUT, keyRange.KeyRangeID, strconv.FormatBool(keyRange.Locked), MapFreq); err != nil {
			return nil, err
		}
		commands[3] = *cmd
		if cmd, err = NewQdbStatementExt(CMD_PUT, keyRange.KeyRangeID, 1, MapKrVersions); err != nil {
			return nil, err
		}
		commands[4] = *cmd
	}
	return commands, nil
}

func (q *MemQDB) dropKeyRangeQdbStatements(keyRangeId string) ([]QdbStatement, error) {
	commands := make([]QdbStatement, 3)

	cmd, err := NewQdbStatementExt(CMD_DELETE, keyRangeId, "", MapKrs)
	if err != nil {
		return nil, err
	}
	commands[0] = *cmd
	cmd, err = NewQdbStatementExt(CMD_DELETE, keyRangeId, "", MapLocks)
	if err != nil {
		return nil, err
	}
	commands[1] = *cmd
	cmd, err = NewQdbStatementExt(CMD_DELETE, keyRangeId, "", MapFreq)
	if err != nil {
		return nil, err
	}
	commands[2] = *cmd
	return commands, nil
}

func (q *MemQDB) updateKeyRangeQdbStatements(keyRange *KeyRange) ([]QdbStatement, error) {
	commands := make([]QdbStatement, 3)
	if keyRangeJSON, err := json.Marshal(*keyRange); err != nil {
		return nil, err
	} else {
		if cmd, err := NewQdbStatementExt(CMD_CMP_VERSION, keyRange.KeyRangeID, keyRange.Version, MapKrVersions); err != nil {
			return nil, err
		} else {
			commands[0] = *cmd
		}
		if cmd, err := NewQdbStatementExt(CMD_PUT, keyRange.KeyRangeID, string(keyRangeJSON), MapKrs); err != nil {
			return nil, err
		} else {
			commands[1] = *cmd
		}
		if cmd, err := NewQdbStatementExt(CMD_PUT, keyRange.KeyRangeID, keyRange.Version+1, MapKrVersions); err != nil {
			return nil, err
		} else {
			commands[4] = *cmd
		}
	}
	return commands, nil
}

// TODO : unit tests
func (q *MemQDB) CreateKeyRange(_ context.Context, keyRange *KeyRange) ([]QdbStatement, error) {
	spqrlog.Zero.Debug().Interface("key-range", keyRange).Msg("memqdb: add key range")

	if len(keyRange.DistributionId) > 0 && keyRange.DistributionId != "default" {
		if _, ok := q.Distributions[keyRange.DistributionId]; !ok {
			return nil, spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, fmt.Sprintf("no such distribution %s", keyRange.DistributionId))
		}
	}

	return q.createKeyRangeQdbStatements(keyRange)
}

func (q *MemQDB) GetKeyRange(_ context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: get key range")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.getKeyrangeInternal(id)
}

func (q *MemQDB) getKeyrangeInternal(id string) (*KeyRange, error) {
	kRangeInt, ok := q.Krs[id]
	if !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "there is no key range %s", id)
	}
	isLocked := false
	if v, ok := q.Freq[id]; ok {
		isLocked = v
	}

	return keyRangeFromInternal(kRangeInt, isLocked, q.KrVersions[id]), nil
}

// TODO : unit tests
func (q *MemQDB) UpdateKeyRange(_ context.Context, keyRange *KeyRange) ([]QdbStatement, error) {
	spqrlog.Zero.Debug().Interface("key-range", keyRange).Msg("memqdb: update key range")

	return q.updateKeyRangeQdbStatements(keyRange)
}

// TODO : unit tests
func (q *MemQDB) DropKeyRange(_ context.Context, id string) ([]QdbStatement, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: drop key range")

	q.mu.Lock()
	defer q.mu.Unlock()

	_, ok := q.Krs[id]
	if !ok {
		return []QdbStatement{}, nil
	}

	lock, ok := q.Locks[id]
	if !ok {
		return nil, spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, fmt.Sprintf("no lock in MemQDB for key range \"%s\"", id))
	}
	if !lock.TryLock() {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v is locked", id)
	}
	defer lock.Unlock()
	return q.dropKeyRangeQdbStatements(id)
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

	return ExecuteCommands(q.DumpState, NewDropCommand(q.Krs), NewDropCommand(q.Locks), NewDropCommand(q.KrVersions))
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
			isLocked := false
			if v, ok := q.Freq[el.KeyRangeID]; ok {
				isLocked = v
			}
			ret = append(ret, keyRangeFromInternal(el, isLocked, q.KrVersions[el.KeyRangeID]))
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

	ret := make([]*KeyRange, 0, len(q.Krs))
	for _, el := range q.Krs {
		isLocked := false
		if v, ok := q.Freq[el.KeyRangeID]; ok {
			isLocked = v
		}
		ret = append(ret, keyRangeFromInternal(el, isLocked, q.KrVersions[el.KeyRangeID]))
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].KeyRangeID < ret[j].KeyRangeID
	})

	return ret, nil
}

// TODO : unit tests
func (q *MemQDB) tryLockKeyRange(lock *sync.RWMutex, id string, read bool) error {
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
	q.mu.Lock()
	defer q.mu.Unlock()
	defer spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: exit: lock key range")

	krs, ok := q.Krs[id]
	if !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range '%s' does not exist", id)
	}

	err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.Freq, id, true),
		NewCustomCommand(func() error {
			if lock, ok := q.Locks[id]; ok {
				return q.tryLockKeyRange(lock, id, false)
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

	return keyRangeFromInternal(krs, true, q.KrVersions[id]), nil
}

// TODO : unit tests
func (q *MemQDB) UnlockKeyRange(_ context.Context, id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: unlock key range")
	q.mu.Lock()
	defer q.mu.Unlock()
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
				return q.tryLockKeyRange(lock, id, false)
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

	krs, err := q.getKeyrangeInternal(id)
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

	err := q.tryLockKeyRange(lock, id, true)
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
	commands = append(commands, q.createKeyRangeCommands(keyRangeFromInternal(kr, false, 1))...)
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
	defer q.mu.Unlock()
	changed := q.Coordinator != address
	q.Coordinator = address

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
func (q *MemQDB) CreateDistribution(_ context.Context, distribution *Distribution) ([]QdbStatement, error) {
	spqrlog.Zero.Debug().Interface("distribution", distribution).Msg("memqdb: add distribution")
	q.mu.Lock()
	defer q.mu.Unlock()
	commands := make([]QdbStatement, 0, len(distribution.Relations)+1)
	for _, r := range distribution.Relations {
		q.RelationDistribution[r.Name] = distribution.ID
		if cmd, err := NewQdbStatementExt(CMD_PUT, r.Name, distribution.ID, MapRelationDistribution); err != nil {
			return nil, err
		} else {
			commands = append(commands, *cmd)
		}
	}

	if distributionJSON, err := json.Marshal(*distribution); err != nil {
		return nil, err
	} else {
		if cmd, err := NewQdbStatementExt(CMD_PUT, distribution.ID, string(distributionJSON), MapDistributions); err != nil {
			return nil, err
		} else {
			commands = append(commands, *cmd)
			return commands, nil
		}
	}
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
				return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is already attached", r.QualifiedName().String())
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
		return spqrerror.Newf(
			spqrerror.SPQR_INVALID_REQUEST,
			"relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"",
			rel.QualifiedName().String(), dsID, id)
	}

	ds.Relations[rel.Name] = rel
	if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.RelationDistribution, rel.Name, id)); err != nil {
		return err
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, id, ds))
}

// TODO : unit tests
func (q *MemQDB) AlterDistributedRelationSchema(ctx context.Context, id string, relation *rfqn.RelationFQN, schemaName string) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: alter distributed relation schema")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.Distributions[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}
	if dsID, ok := q.RelationDistribution[relation.RelationName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relation.String())
	} else if dsID != id {
		return spqrerror.Newf(
			spqrerror.SPQR_INVALID_REQUEST,
			"relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"",
			relation.String(), dsID, id)
	}

	ds.Relations[relation.RelationName].SchemaName = schemaName
	if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.RelationDistribution, relation.RelationName, id)); err != nil {
		return err
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, id, ds))
}

// TODO : unit tests
func (q *MemQDB) AlterReplicatedRelationSchema(ctx context.Context, id string, relation *rfqn.RelationFQN, schemaName string) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: alter distributed relation schema")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.Distributions[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}
	if dsID, ok := q.RelationDistribution[relation.RelationName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relation.String())
	} else if dsID != id {
		return spqrerror.Newf(
			spqrerror.SPQR_INVALID_REQUEST,
			"relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"",
			relation.String(), dsID, id)
	}

	rel, ok := q.ReferenceRelations[relation.RelationName]
	if !ok {
		return fmt.Errorf("reference relation \"%s\" not found", relation.String())
	}

	ds.Relations[relation.RelationName].SchemaName = schemaName
	rel.SchemaName = schemaName

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, id, ds), NewUpdateCommand(q.ReferenceRelations, relation.RelationName, rel))
}

// TODO : unit tests
func (q *MemQDB) AlterDistributedRelationDistributionKey(ctx context.Context, id string, relation *rfqn.RelationFQN, distributionKey []DistributionKeyEntry) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: alter distributed relation distribution key")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.Distributions[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}
	if dsID, ok := q.RelationDistribution[relation.RelationName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relation.String())
	} else if dsID != id {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"", relation.String(), dsID, id)
	}

	ds.Relations[relation.RelationName].DistributionKey = distributionKey
	if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.RelationDistribution, relation.RelationName, id)); err != nil {
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
//                               UNIQUE INDEXES
// ==============================================================================

func (q *MemQDB) ListUniqueIndexes(_ context.Context) (map[string]*UniqueIndex, error) {
	spqrlog.Zero.Debug().
		Msg("memqdb: list unique indexes")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.UniqueIndexes, nil
}

func (q *MemQDB) CreateUniqueIndex(_ context.Context, idx *UniqueIndex) error {
	spqrlog.Zero.Debug().
		Msg("memqdb: create unique index")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.Distributions[idx.DistributionId]
	if !ok {
		return fmt.Errorf("cannot create unique index: distribution \"%s\" not found", idx.DistributionId)
	}
	ds.UniqueIndexes[idx.ID] = idx

	idxs, ok := q.UniqueIndexesByRel[idx.Relation.String()]
	if !ok {
		idxs = make(map[string]*UniqueIndex)
	}
	for _, col := range idx.ColumnNames {
		idxs[col] = idx
	}

	q.UniqueIndexes[idx.ID] = idx
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, ds.ID, ds), NewUpdateCommand(q.UniqueIndexes, idx.ID, idx), NewUpdateCommand(q.UniqueIndexesByRel, idx.Relation.String(), idxs))
}

func (q *MemQDB) DropUniqueIndex(_ context.Context, id string) error {
	spqrlog.Zero.Debug().
		Msg("memqdb: drop unique index")
	q.mu.Lock()
	defer q.mu.Unlock()

	idx, ok := q.UniqueIndexes[id]
	if !ok {
		return fmt.Errorf("unique index \"%s\" not found", id)
	}

	ds, ok := q.Distributions[idx.DistributionId]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "unique index \"%s\" belongs to nonexistent distribution \"%s\"", idx.ID, idx.DistributionId)
	}
	delete(ds.UniqueIndexes, idx.ID)

	idxs, ok := q.UniqueIndexesByRel[idx.Relation.String()]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "unique index \"%s\" belongs to relation \"%s\", but index record not found", idx.ID, idx.Relation.String())
	}
	for _, col := range idx.ColumnNames {
		delete(idxs, col)
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.Distributions, ds.ID, ds), NewUpdateCommand(q.UniqueIndexesByRel, idx.Relation.String(), idxs), NewDeleteCommand(q.UniqueIndexes, idx.ID))
}

func (q *MemQDB) ListRelationIndexes(_ context.Context, relName *rfqn.RelationFQN) (map[string]*UniqueIndex, error) {

	q.mu.Lock()
	defer q.mu.Unlock()

	return q.UniqueIndexesByRel[relName.String()], nil
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

	if _, ok := q.MoveTaskGroups[id]; ok {
		return fmt.Errorf("could not write move task group: task group with ID \"%s\" already exists", id)
	}

	q.MoveTaskGroups[id] = group
	q.StopMoveTaskGroup[id] = false
	if group.Issuer != nil && group.Issuer.Type == IssuerRedistributeTask {
		q.RedistributeTaskTaskGroupId[group.Issuer.Id] = id
	}
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.MoveTaskGroups, id, group), NewUpdateCommand(q.StopMoveTaskGroup, id, false))
}

// TODO: unit tests
func (q *MemQDB) DropMoveTaskGroup(_ context.Context, id string) error {
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

func (q *MemQDB) GetMoveTaskByGroup(_ context.Context, taskGroupID string) (*MoveTask, error) {
	spqrlog.Zero.Debug().
		Str("task group id", taskGroupID).
		Msg("memqdb: get move task of a task group")
	q.mu.RLock()
	defer q.mu.RUnlock()

	id, ok := q.TaskGroupMoveTaskID[taskGroupID]
	if !ok {
		return nil, nil
	}
	task, ok := q.MoveTasks[id]
	if !ok {
		return nil, fmt.Errorf("move task \"%s\" not found", id)
	}
	return task, nil
}

func (q *MemQDB) ListMoveTasks(_ context.Context) (map[string]*MoveTask, error) {
	spqrlog.Zero.Debug().
		Msg("memqdb: list move tasks")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.MoveTasks, nil
}

// TODO: unit tests
func (q *MemQDB) GetMoveTask(ctx context.Context, id string) (*MoveTask, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: get move task")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.MoveTasks[id], nil
}

// TODO: unit tests
func (q *MemQDB) WriteMoveTask(ctx context.Context, task *MoveTask) error {
	spqrlog.Zero.Debug().
		Str("id", task.ID).
		Msg("memqdb: write move task")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.MoveTasks[task.ID]; ok {
		return fmt.Errorf("failed to write move task: another task already exists")
	}
	q.MoveTasks[task.ID] = task
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.MoveTasks, task.ID, task))
}

// TODO: unit tests
func (q *MemQDB) UpdateMoveTask(ctx context.Context, task *MoveTask) error {
	spqrlog.Zero.Debug().
		Str("id", task.ID).
		Msg("memqdb: update move task")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.MoveTasks[task.ID]; !ok {
		return fmt.Errorf("failed to update move task: IDs differ")
	}

	q.MoveTasks[task.ID] = task
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.MoveTasks, task.ID, task))
}

// TODO: unit tests
func (q *MemQDB) DropMoveTask(ctx context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: remove move task")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.MoveTasks, id)
	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.MoveTasks, id))
}

// TODO: unit tests
func (q *MemQDB) ListRedistributeTasks(_ context.Context) ([]*RedistributeTask, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list redistribute tasks")
	q.mu.RLock()
	defer q.mu.RUnlock()

	res := make([]*RedistributeTask, 0, len(q.RedistributeTasks))
	for _, task := range q.RedistributeTasks {
		res = append(res, task)
	}
	return res, nil
}

// TODO: unit tests
func (q *MemQDB) GetRedistributeTask(_ context.Context, id string) (*RedistributeTask, error) {
	spqrlog.Zero.Debug().Str("id", id).Msg("memqdb: get redistribute task")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.RedistributeTasks[id], nil
}

// TODO: unit tests
func (q *MemQDB) CreateRedistributeTask(_ context.Context, task *RedistributeTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("memqdb: create redistribute task")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.RedistributeTasks[task.ID]; ok {
		return fmt.Errorf("could not create redistribute task: redistribute task with ID \"%s\" already exists in QDB", task.ID)
	}
	if _, ok := q.KeyRangeRedistributeTasks[task.KeyRangeId]; ok {
		return fmt.Errorf("could not create redistribute task: task for key range \"%s\" already exists", task.KeyRangeId)
	}
	q.RedistributeTasks[task.ID] = task
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.RedistributeTasks, task.ID, task))
}

// TODO: unit tests
func (q *MemQDB) UpdateRedistributeTask(_ context.Context, task *RedistributeTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("memqdb: update redistribute task")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.RedistributeTasks[task.ID]; !ok {
		return fmt.Errorf("could not update redistribute task: redistribute task with ID \"%s\" doesn't exist in QDB", task.ID)
	}
	q.RedistributeTasks[task.ID] = task
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.RedistributeTasks, task.ID, task))
}

// TODO: unit tests
func (q *MemQDB) DropRedistributeTask(_ context.Context, task *RedistributeTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("memqdb: remove redistribute task")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.RedistributeTasks, task.ID)
	delete(q.KeyRangeRedistributeTasks, task.KeyRangeId)
	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.RedistributeTasks, task.ID), NewDeleteCommand(q.KeyRangeRedistributeTasks, task.KeyRangeId))
}

func (q *MemQDB) GetRedistributeTaskTaskGroupId(ctx context.Context, id string) (string, error) {
	spqrlog.Zero.Debug().Str("id", id).Msg("memqdb: get redistribute task task group ID")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.RedistributeTaskTaskGroupId[id], nil
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
func (q *MemQDB) DropBalancerTask(_ context.Context) error {
	spqrlog.Zero.Debug().Msg("memqdb: remove balancer task")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.BalancerTask = nil
	return nil
}

func (q *MemQDB) WriteTaskGroupStatus(ctx context.Context, id string, status *TaskGroupStatus) error {
	spqrlog.Zero.Debug().
		Str("task group ID", id).
		Str("state", status.State).
		Str("msg", status.Message).
		Msg("memqdb: write task group status")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.TaskGroupIDToStatus[id] = status
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.TaskGroupIDToStatus, id, status))
}

func (q *MemQDB) GetTaskGroupStatus(ctx context.Context, id string) (*TaskGroupStatus, error) {
	spqrlog.Zero.Debug().
		Str("task group ID", id).
		Msg("memqdb: get task group status")
	q.mu.RLock()
	defer q.mu.RUnlock()

	status := q.TaskGroupIDToStatus[id]
	return status, nil
}

func (q *MemQDB) GetAllTaskGroupStatuses(ctx context.Context) (map[string]*TaskGroupStatus, error) {
	spqrlog.Zero.Debug().
		Msg("memqdb: get task groups statuses")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.TaskGroupIDToStatus, nil
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

func (q *MemQDB) GetSequenceRelations(ctx context.Context, seqName string) ([]*rfqn.RelationFQN, error) {
	rels := []*rfqn.RelationFQN{}
	for col, seq := range q.ColumnSequence {
		if seq == seqName {
			s := strings.Split(col, "_")
			relName := s[len(s)-2]
			rels = append(rels, &rfqn.RelationFQN{RelationName: relName})
		}
	}
	return rels, nil
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

func (q *MemQDB) toRelationDistributionOperation(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CMD_DELETE:
		return NewDeleteCommand(q.RelationDistribution, stmt.Key), nil
	case CMD_PUT:
		val, ok := stmt.Value.(string)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for CMD_PUT, string is expected", stmt.Value)
		}
		return NewUpdateCommand(q.RelationDistribution, stmt.Key, val), nil
	default:
		return nil, fmt.Errorf("unsupported memqdb cmd %d (relation distribution)", stmt.CmdType)
	}
}
func (q *MemQDB) toDistributions(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CMD_DELETE:
		return NewDeleteCommand(q.Distributions, stmt.Key), nil
	case CMD_PUT:
		var distr Distribution
		val, ok := stmt.Value.(string)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for CMD_PUT, string is expected", stmt.Value)
		}
		if err := json.Unmarshal([]byte(val), &distr); err != nil {
			return nil, err
		} else {
			return NewUpdateCommand(q.Distributions, stmt.Key, &distr), nil
		}
	default:
		return nil, fmt.Errorf("unsupported memqdb cmd %d (distributions)", stmt.CmdType)
	}
}

func (q *MemQDB) toKeyRange(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CMD_DELETE:
		return NewDeleteCommand(q.Krs, stmt.Key), nil
	case CMD_PUT:
		val, ok := stmt.Value.(string)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for CMD_PUT, string is expected", stmt.Value)
		}
		var kr KeyRange
		if err := json.Unmarshal([]byte(val), &kr); err != nil {
			return nil, err
		}
		return NewUpdateCommand(q.Krs, stmt.Key, keyRangeToInternal(&kr)), nil
	default:
		return nil, fmt.Errorf("unsupported memDB cmd %d (key range)", stmt.CmdType)
	}
}

func (q *MemQDB) toFreq(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CMD_DELETE:
		return NewDeleteCommand(q.Freq, stmt.Key), nil
	case CMD_PUT:
		valFreq := true
		if stmt.Value == "false" {
			valFreq = false
		}
		return NewUpdateCommand(q.Freq, stmt.Key, valFreq), nil
	default:
		return nil, fmt.Errorf("unsupported memDB cmd %d (freq)", stmt.CmdType)
	}
}

func (q *MemQDB) toLock(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CMD_DELETE:
		return NewDeleteCommand(q.Locks, stmt.Key), nil
	case CMD_PUT:
		val, ok := stmt.Value.(string)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for CMD_PUT, string is expected", stmt.Value)
		}
		lock := &sync.RWMutex{}
		isLocked, err := strconv.ParseBool(val)
		if err != nil {
			return nil, err
		}
		if isLocked {
			if !lock.TryLock() {
				return nil, fmt.Errorf("can't set lock memDB cmd %d", stmt.CmdType)
			}
		}
		return NewUpdateCommand(q.Locks, stmt.Key, lock), nil
	default:
		return nil, fmt.Errorf("unsupported memDB cmd %d (lock)", stmt.CmdType)
	}
}

func (q *MemQDB) toKrVersion(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CMD_DELETE:
		return NewDeleteCommand(q.KrVersions, stmt.Key), nil
	case CMD_PUT:
		val, ok := stmt.Value.(int)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for MapKrVersions, int is expected", stmt.Value)
		}
		return NewUpdateCommand(q.KrVersions, stmt.Key, val), nil
	case CMD_CMP_VERSION:
		val, ok := stmt.Value.(int)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for MapKrVersions, int is expected", stmt.Value)
		}
		return NewCustomCommand(func() error {
			if q.KrVersions[stmt.Key] != val {
				return fmt.Errorf("failed to exec statements: key range \"%s\" unexpected version", stmt.Key)
			}
			return nil
		}, func() error { return nil }), nil
	default:
		return nil, fmt.Errorf("unsupported memDB cmd %d (lock)", stmt.CmdType)
	}
}

func (q *MemQDB) packMemqdbCommands(operations []QdbStatement) ([]Command, error) {
	memOperations := make([]Command, 0, len(operations))
	for _, stmt := range operations {
		switch stmt.Extension {
		case MapRelationDistribution:
			if operation, err := q.toRelationDistributionOperation(stmt); err != nil {
				return nil, err
			} else {
				memOperations = append(memOperations, operation)
			}
		case MapDistributions:
			if operation, err := q.toDistributions(stmt); err != nil {
				return nil, err
			} else {
				memOperations = append(memOperations, operation)
			}
		case MapKrs:
			operation, err := q.toKeyRange(stmt)
			if err != nil {
				return nil, err
			}
			memOperations = append(memOperations, operation)
		case MapFreq:
			operation, err := q.toFreq(stmt)
			if err != nil {
				return nil, err
			}
			memOperations = append(memOperations, operation)
		case MapLocks:
			operation, err := q.toLock(stmt)
			if err != nil {
				return nil, err
			}
			memOperations = append(memOperations, operation)
		case MapKrVersions:
			operation, err := q.toKrVersion(stmt)
			if err != nil {
				return nil, err
			}
			memOperations = append(memOperations, operation)
		default:
			return nil, fmt.Errorf("not implemented for transaction memqdb part %s", stmt.Extension)
		}
	}
	return memOperations, nil
}

func (q *MemQDB) ExecNoTransaction(ctx context.Context, operations []QdbStatement) error {
	spqrlog.Zero.Debug().Msg("memqdb: exec chunk commands without transaction")
	q.mu.Lock()
	defer q.mu.Unlock()
	if memOperations, err := q.packMemqdbCommands(operations); err != nil {
		return err
	} else {
		return ExecuteCommands(q.DumpState, memOperations...)
	}
}

func (q *MemQDB) CommitTransaction(ctx context.Context, transaction *QdbTransaction) error {
	spqrlog.Zero.Debug().Msg("memqdb: exec transaction")
	q.mu.Lock()
	defer q.mu.Unlock()

	if transaction == nil {
		return fmt.Errorf("cant't commit empty transaction")
	}
	if err := transaction.Validate(); err != nil {
		return fmt.Errorf("invalid transaction %s: %w", transaction.Id(), err)
	}
	if transaction.Id() != q.activeTransaction {
		return fmt.Errorf("transaction '%s' can't be committed", transaction.Id())
	}
	if memOperations, err := q.packMemqdbCommands(transaction.commands); err != nil {
		return err
	} else {
		return ExecuteCommands(q.DumpState, memOperations...)
	}
}

func (q *MemQDB) BeginTransaction(_ context.Context, transaction *QdbTransaction) error {
	spqrlog.Zero.Debug().Msg("memqdb: begin transaction")
	q.mu.Lock()
	defer q.mu.Unlock()
	if transaction == nil {
		return fmt.Errorf("empty transaction is not supported")
	}
	q.activeTransaction = transaction.Id()
	return nil
}

// ChangeTxStatus implements DCStateKeeper.
func (q *MemQDB) ChangeTxStatus(id string, state string) error {
	spqrlog.Zero.Debug().Msg("memqdb: ChangeTxStatus")
	q.mu.Lock()
	defer q.mu.Unlock()

	/* XXX: validate state outer layers? */

	info := q.TwoPhaseTx[id]
	info.State = state

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.TwoPhaseTx, id, info))
}

func (q *MemQDB) AcquireTxOwnership(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if info, ok := q.TwoPhaseTx[id]; ok {
		if info.Locked {
			return false
		}
		info.Locked = true
		return true
	}
	return false
}

func (q *MemQDB) ReleaseTxOwnership(gid string) {
	spqrlog.Zero.Debug().Str("gid", gid).Msg("memqdb: ReleaseTxOwnership")
	q.mu.Lock()
	defer q.mu.Unlock()

	if info, ok := q.TwoPhaseTx[gid]; ok {
		info.Locked = false
	}
}

// RecordTwoPhaseMembers implements DCStateKeeper.
// XXX: check that all members are valid spqr shards
func (q *MemQDB) RecordTwoPhaseMembers(id string, shards []string) error {
	spqrlog.Zero.Debug().Msg("memqdb: RecordTwoPhaseMembers")
	q.mu.Lock()
	defer q.mu.Unlock()

	info := &TwoPCInfo{
		Gid:       id,
		SHardsIds: shards,
		State:     TwoPhaseInitState,
		Locked:    true,
	}

	q.TwoPhaseTx[id] = info

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.TwoPhaseTx, id, info))
}

// TXCohortShards implements DCStateKeeper.
func (q *MemQDB) TXCohortShards(gid string) []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.TwoPhaseTx[gid].SHardsIds
}

// TXStatus implements DCStateKeeper.
func (q *MemQDB) TXStatus(gid string) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.TwoPhaseTx[gid].State
}

// ==============================================================================
//                               TASK GROUP STATE
// ==============================================================================

func (q *MemQDB) TryTaskGroupLock(ctx context.Context, tgId string, holder string) error {
	return fmt.Errorf("not implemented")
}

func (q *MemQDB) CheckTaskGroupLocked(ctx context.Context, tgId string) (bool, error) {
	return false, fmt.Errorf("not implemented")
}
