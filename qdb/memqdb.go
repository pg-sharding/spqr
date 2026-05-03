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
	MapSequences            = "Sequences"
	MapSequenceToValues     = "SequenceToValues"
)

type MemQDBState struct {
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
	StopMoveTaskGroup           map[string]string                   `json:"stop_move_task_group"`
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
}

type MemQDB struct {
	// TODO create more mutex per map if needed
	mu    sync.RWMutex
	State *MemQDBState

	SequenceLock sync.RWMutex

	backupPath        string
	activeTransaction uuid.UUID
	/* caches */
}

var _ XQDB = &MemQDB{}
var _ DCStateKeeper = &MemQDB{}

func NewMemQDB(backupPath string) (*MemQDB, error) {
	return &MemQDB{
		State: &MemQDBState{
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
			StopMoveTaskGroup:           map[string]string{},
			TotalKeys:                   map[string]int64{},
			MoveTasks:                   map[string]*MoveTask{},
			TwoPhaseTx:                  map[string]*TwoPCInfo{},
			UniqueIndexes:               map[string]*UniqueIndex{},
			UniqueIndexesByRel:          map[string]map[string]*UniqueIndex{},
		},

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

	for kr, locked := range qdb.State.Freq {
		if locked {
			qdb.State.Locks[kr].Lock()
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

func (q *MemQDB) SwapState(state *MemQDBState) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.State = state
}

// ==============================================================================
//                               MISC
// ==============================================================================

func (q *MemQDB) dropKeyRangeCommands(krId string) []Command {
	return []Command{
		NewDeleteCommand(q.State.Krs, krId),
		NewDeleteCommand(q.State.Freq, krId),
		NewDeleteCommand(q.State.Locks, krId),
		NewDeleteCommand(q.State.KrVersions, krId),
	}
}

func (q *MemQDB) createKeyRangeCommands(keyRange *KeyRange) []Command {
	return []Command{
		NewUpdateCommand(q.State.Krs, keyRange.KeyRangeID, keyRangeToInternal(keyRange)),
		NewUpdateCommand(q.State.Locks, keyRange.KeyRangeID, &sync.RWMutex{}),
		NewUpdateCommand(q.State.Freq, keyRange.KeyRangeID, keyRange.Locked),
		NewUpdateCommand(q.State.KrVersions, keyRange.KeyRangeID, 1),
	}
}

// ==============================================================================
//                               KEY RANGE MOVES
// ==============================================================================

func (q *MemQDB) RecordKeyRangeMove(_ context.Context, _ *MoveKeyRange) error {
	// TODO implement
	return nil
}

func (q *MemQDB) ListKeyRangeMoves(_ context.Context) ([]*MoveKeyRange, error) {
	// TODO implement
	return nil, nil
}

func (q *MemQDB) UpdateKeyRangeMoveStatus(_ context.Context, _ string, _ MoveKeyRangeStatus) error {
	// TODO implement
	return nil
}

func (q *MemQDB) DeleteKeyRangeMove(_ context.Context, _ string) error {
	// TODO implement
	return nil
}

// ==============================================================================
//                                 KEY RANGES
// ==============================================================================

func (q *MemQDB) createKeyRangeQdbStatements(keyRange *KeyRange) ([]QdbStatement, error) {
	commands := make([]QdbStatement, 4)
	if keyRangeJSON, err := json.Marshal(*keyRange); err != nil {
		return nil, err
	} else {
		cmd, err := NewQdbStatementExt(CmdPut, keyRange.KeyRangeID, string(keyRangeJSON), MapKrs)
		if err != nil {
			return nil, err
		}
		commands[0] = *cmd
		if cmd, err = NewQdbStatementExt(CmdPut, keyRange.KeyRangeID, strconv.FormatBool(keyRange.Locked), MapLocks); err != nil {
			return nil, err
		}
		commands[1] = *cmd
		if cmd, err = NewQdbStatementExt(CmdPut, keyRange.KeyRangeID, strconv.FormatBool(keyRange.Locked), MapFreq); err != nil {
			return nil, err
		}
		commands[2] = *cmd
		if cmd, err = NewQdbStatementExt(CmdPut, keyRange.KeyRangeID, 1, MapKrVersions); err != nil {
			return nil, err
		}
		commands[3] = *cmd
	}
	return commands, nil
}

func (q *MemQDB) dropKeyRangeQdbStatements(keyRangeId string) ([]QdbStatement, error) {
	commands := make([]QdbStatement, 3)

	cmd, err := NewQdbStatementExt(CmdDelete, keyRangeId, "", MapKrs)
	if err != nil {
		return nil, err
	}
	commands[0] = *cmd
	cmd, err = NewQdbStatementExt(CmdDelete, keyRangeId, "", MapLocks)
	if err != nil {
		return nil, err
	}
	commands[1] = *cmd
	cmd, err = NewQdbStatementExt(CmdDelete, keyRangeId, "", MapFreq)
	if err != nil {
		return nil, err
	}
	commands[2] = *cmd
	return commands, nil
}

func (q *MemQDB) updateKeyRangeQdbStatements(keyRange *KeyRange) ([]QdbStatement, error) {
	commands := make([]QdbStatement, 2)
	if keyRangeJSON, err := json.Marshal(*keyRange); err != nil {
		return nil, err
	} else {
		if cmd, err := NewQdbStatementExt(CmdPut, keyRange.KeyRangeID, string(keyRangeJSON), MapKrs); err != nil {
			return nil, err
		} else {
			commands[0] = *cmd
		}
		if cmd, err := NewQdbStatementExt(CmdPut, keyRange.KeyRangeID, keyRange.Version+1, MapKrVersions); err != nil {
			return nil, err
		} else {
			commands[1] = *cmd
		}
	}
	return commands, nil
}

// TODO : unit tests
func (q *MemQDB) CreateKeyRange(_ context.Context, keyRange *KeyRange) ([]QdbStatement, error) {
	spqrlog.Zero.Debug().Interface("key-range", keyRange).Msg("memqdb: add key range")

	if len(keyRange.DistributionId) > 0 && keyRange.DistributionId != "default" {
		if _, ok := q.State.Distributions[keyRange.DistributionId]; !ok {
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
	kRangeInt, ok := q.State.Krs[id]
	if !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "there is no key range %s", id)
	}
	isLocked := false
	if v, ok := q.State.Freq[id]; ok {
		isLocked = v
	}

	return keyRangeFromInternal(kRangeInt, isLocked, q.State.KrVersions[id]), nil
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

	_, ok := q.State.Krs[id]
	if !ok {
		return q.dropKeyRangeQdbStatements(id)
	}

	lock, ok := q.State.Locks[id]
	if !ok {
		return nil, spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, fmt.Sprintf("no lock in MemQDB for key range \"%s\"", id))
	}
	if !lock.TryLock() {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range is locked").Detail(fmt.Sprintf("Key range id is \"%v\"", id))
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
	for krId, l := range q.State.Locks {
		if !l.TryLock() {
			return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range is locked").Detail(fmt.Sprintf("Key range id is \"%v\"", krId))
		}
		locks = append(locks, l)
	}
	spqrlog.Zero.Debug().Msg("memqdb: acquired all locks")

	return ExecuteCommands(q.DumpState, NewDropCommand(q.State.Krs), NewDropCommand(q.State.Locks), NewDropCommand(q.State.KrVersions))
}

// TODO : unit tests
func (q *MemQDB) ListKeyRanges(_ context.Context, distribution string) ([]*KeyRange, error) {
	spqrlog.Zero.Debug().
		Str("distribution", distribution).
		Msg("memqdb: list key ranges")
	q.mu.RLock()
	defer q.mu.RUnlock()

	var ret []*KeyRange

	for _, el := range q.State.Krs {
		if el.DistributionId == distribution {
			isLocked := false
			if v, ok := q.State.Freq[el.KeyRangeID]; ok {
				isLocked = v
			}
			ret = append(ret, keyRangeFromInternal(el, isLocked, q.State.KrVersions[el.KeyRangeID]))
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

	ret := make([]*KeyRange, 0, len(q.State.Krs))
	for _, el := range q.State.Krs {
		isLocked := false
		if v, ok := q.State.Freq[el.KeyRangeID]; ok {
			isLocked = v
		}
		ret = append(ret, keyRangeFromInternal(el, isLocked, q.State.KrVersions[el.KeyRangeID]))
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
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range is locked").Detail(fmt.Sprintf("Key range id is \"%v\"", id))
	}

	if _, ok := q.State.Krs[id]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range '%s' deleted after lock acquired", id)
	}
	return nil
}

// TODO : unit tests
func (q *MemQDB) LockKeyRange(_ context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: lock key range")
	q.mu.Lock()
	defer q.mu.Unlock()
	defer spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: exit: lock key range")

	krs, ok := q.State.Krs[id]
	if !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range '%s' does not exist", id)
	}

	err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Freq, id, true),
		NewCustomCommand(func() error {
			if lock, ok := q.State.Locks[id]; ok {
				return q.tryLockKeyRange(lock, id, false)
			}
			return nil
		}, func() error {
			if lock, ok := q.State.Locks[id]; ok {
				lock.Unlock()
			}
			return nil
		}))
	if err != nil {
		return nil, err
	}

	return keyRangeFromInternal(krs, true, q.State.KrVersions[id]), nil
}

// TODO : unit tests
func (q *MemQDB) UnlockKeyRange(_ context.Context, id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: unlock key range")
	q.mu.Lock()
	defer q.mu.Unlock()
	defer spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: exit: unlock key range")

	if !q.State.Freq[id] {
		return spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v not locked", id)
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Freq, id, false),
		NewCustomCommand(func() error {
			if lock, ok := q.State.Locks[id]; ok {
				lock.Unlock()
			}
			return nil
		}, func() error {
			if lock, ok := q.State.Locks[id]; ok {
				return q.tryLockKeyRange(lock, id, false)
			}
			return nil
		}))
}
func (q *MemQDB) ListLockedKeyRanges(_ context.Context) ([]string, error) {
	spqrlog.Zero.Debug().
		Str("key-range lock request", "").
		Msg("memqdb: get list locked key range")
	q.mu.RLock()
	defer q.mu.RUnlock()
	result := make([]string, 0, len(q.State.Locks))
	for lk, v := range q.State.Freq {
		if v {
			result = append(result, lk)
		}
	}
	return result, nil
}

// TODO : unit tests
func (q *MemQDB) CheckLockedKeyRange(_ context.Context, id string) (*KeyRange, error) {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: check locked key range")
	q.mu.RLock()
	defer q.mu.RUnlock()

	krs, err := q.getKeyrangeInternal(id)
	if err != nil {
		return nil, err
	}

	if !q.State.Freq[id] {
		return nil, spqrerror.Newf(spqrerror.SPQR_KEYRANGE_ERROR, "key range %v not locked", id)
	}

	return krs, nil
}

// TODO : unit tests
func (q *MemQDB) ShareKeyRange(id string) error {
	spqrlog.Zero.Debug().Str("key-range", id).Msg("memqdb: sharing key with key")

	q.mu.RLock()
	defer q.mu.RUnlock()

	lock, ok := q.State.Locks[id]
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

	kr, ok := q.State.Krs[krId]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_KEYRANGE_ERROR, fmt.Sprintf("key range '%s' not found", krId))
	}
	if _, ok = q.State.Krs[krIdNew]; ok {
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
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Transactions, key, info))
}

// TODO : unit tests
func (q *MemQDB) GetTransferTx(_ context.Context, key string) (*DataTransferTransaction, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	ans, ok := q.State.Transactions[key]
	if !ok {
		return nil, nil
	}
	return ans, nil
}

// TODO : unit tests
func (q *MemQDB) RemoveTransferTx(_ context.Context, key string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.State.Transactions, key))
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
	changed := q.State.Coordinator != address
	q.State.Coordinator = address

	if changed {
		spqrlog.Zero.Debug().Str("address", address).Msg("memqdb: update coordinator address")
	}
	return nil
}

func (q *MemQDB) GetCoordinator(_ context.Context) (string, error) {
	spqrlog.Zero.Debug().Str("address", q.State.Coordinator).Msg("memqdb: get coordinator address")
	return q.State.Coordinator, nil
}

// ==============================================================================
//                                  ROUTERS
// ==============================================================================

// TODO : unit tests
func (q *MemQDB) AddRouter(_ context.Context, r *Router) error {
	spqrlog.Zero.Debug().Interface("router", r).Msg("memqdb: add router")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Routers, r.ID, r))
}

// TODO : unit tests
func (q *MemQDB) DeleteRouter(_ context.Context, id string) error {
	spqrlog.Zero.Debug().Str("router", id).Msg("memqdb: delete router")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.State.Routers, id))
}

// TODO : unit tests
func (q *MemQDB) DeleteRouterAll(_ context.Context) error {
	spqrlog.Zero.Debug().Msg("memqdb: unregister all routers")
	q.mu.Lock()
	defer q.mu.Unlock()

	return ExecuteCommands(q.DumpState, NewDropCommand(q.State.Routers))
}

// TODO : unit tests
func (q *MemQDB) OpenRouter(_ context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("router", id).
		Msg("memqdb: open router")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.State.Routers[id].State = OPENED

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Routers, id, q.State.Routers[id]))
}

// TODO : unit tests
func (q *MemQDB) CloseRouter(_ context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("router", id).
		Msg("memqdb: close router")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.State.Routers[id]; !ok {
		return fmt.Errorf("failed to close router: router \"%s\" not found", id)
	}
	q.State.Routers[id].State = CLOSED

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Routers, id, q.State.Routers[id]))
}

// TODO : unit tests
func (q *MemQDB) ListRouters(_ context.Context) ([]*Router, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list routers")
	q.mu.RLock()
	defer q.mu.RUnlock()

	var ret []*Router
	for _, v := range q.State.Routers {
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

	if _, ok := q.State.Shards[shard.ID]; ok {
		return fmt.Errorf("shard with id %s already exists", shard.ID)
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Shards, shard.ID, shard))
}

// TODO : unit tests
func (q *MemQDB) ListShards(_ context.Context) ([]*Shard, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list shards")
	q.mu.RLock()
	defer q.mu.RUnlock()

	var ret []*Shard
	for _, v := range q.State.Shards {
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

	if shard, ok := q.State.Shards[id]; ok {
		return shard, nil
	}

	return nil, spqrerror.Newf(spqrerror.SPQR_NO_DATASHARD, "unknown shard %s", id)
}

func (q *MemQDB) AlterShard(_ context.Context, newShard *Shard) error {
	spqrlog.Zero.Debug().Str("shard", newShard.ID).Msg("memqdb: alter shard options")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.State.Shards[newShard.ID] = newShard

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Shards, newShard.ID, newShard))
}

// TODO : unit tests
func (q *MemQDB) DropShard(_ context.Context, id string) error {
	spqrlog.Zero.Debug().Str("shard", id).Msg("memqdb: drop shard")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.State.Shards, id)
	return nil
}

// ==============================================================================
//                              REFERENCE RELATIONS
// ==============================================================================

// CreateReferenceRelation implements XQDB.
func (q *MemQDB) CreateReferenceRelation(_ context.Context, r *ReferenceRelation) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.State.ReferenceRelations[r.TableName] = r
	return nil
}

// GetReferenceRelation implements XQDB.
func (q *MemQDB) GetReferenceRelation(_ context.Context, relationFQN *rfqn.RelationFQN) (*ReferenceRelation, error) {
	tableName := relationFQN.RelationName

	spqrlog.Zero.Debug().Str("id", tableName).Msg("memqdb: get reference relation")
	q.mu.RLock()
	defer q.mu.RUnlock()

	if rr, ok := q.State.ReferenceRelations[tableName]; !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "reference relation \"%s\" not found", tableName)
	} else {
		return rr, nil
	}
}

// AlterReferenceRelationStorage implements XQDB.
func (q *MemQDB) AlterReferenceRelationStorage(_ context.Context, relationFQN *rfqn.RelationFQN, shs []string) error {
	tableName := relationFQN.RelationName
	q.mu.Lock()
	defer q.mu.Unlock()
	rel, ok := q.State.ReferenceRelations[tableName]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "reference relation \"%s\" not found", tableName)
	}
	rel.ShardIDs = shs
	rel.Version++
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.ReferenceRelations, tableName, rel))
}

// DropReferenceRelation implements XQDB.
func (q *MemQDB) DropReferenceRelation(_ context.Context, relationFQN *rfqn.RelationFQN) error {
	spqrlog.Zero.Debug().Str("rfqn", relationFQN.String()).Msg("memqdb: drop reference table")
	tableName := relationFQN.RelationName
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.State.ReferenceRelations[tableName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "reference relation \"%s\" not found", tableName)
	}
	delete(q.State.ReferenceRelations, tableName)
	return nil
}

// ListReferenceRelations implements XQDB.
func (q *MemQDB) ListReferenceRelations(_ context.Context) ([]*ReferenceRelation, error) {
	var rrs []*ReferenceRelation
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, r := range q.State.ReferenceRelations {
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
		q.State.RelationDistribution[r.Name] = distribution.ID
		if cmd, err := NewQdbStatementExt(CmdPut, r.Name, distribution.ID, MapRelationDistribution); err != nil {
			return nil, err
		} else {
			commands = append(commands, *cmd)
		}
	}

	if distributionJSON, err := json.Marshal(*distribution); err != nil {
		return nil, err
	} else {
		if cmd, err := NewQdbStatementExt(CmdPut, distribution.ID, string(distributionJSON), MapDistributions); err != nil {
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
	for _, v := range q.State.Distributions {
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

	if _, ok := q.State.Distributions[id]; !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}

	for t, ds := range q.State.RelationDistribution {
		if ds == id {
			if err := ExecuteCommands(q.DumpState, NewDeleteCommand(q.State.RelationDistribution, t)); err != nil {
				return err
			}
		}
	}

	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.State.Distributions, id))
}

// TODO : unit tests
func (q *MemQDB) AlterDistributionAttach(_ context.Context, id string, rels []*DistributedRelation) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: attach table to distribution")
	q.mu.Lock()
	defer q.mu.Unlock()

	if ds, ok := q.State.Distributions[id]; !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	} else {

		if ds.FQNRelations == nil {
			/* Initialize metadata from previous db version. */
			ds.FQNRelations = map[string]*DistributedRelation{}
		}

		ds.Version++

		for _, r := range rels {
			/* Do not use public iface function, because we already got lock. */
			if ds, err := q.relationDistributionInternal(r.QualifiedName()); err == nil {
				/* Well, okay. We already have distribution for relation with
				* this exact relname. What about schema?
				* If schema matches, throw error. Otherwise, try to simple place this
				* relation to fqn_relations. TODO: remove this nonsense after complete
				* SPQR 3.0.0 transition. */
				if dr := ds.Relations[r.QualifiedName().RelationName]; dr.SchemaName == r.SchemaName {
					return spqrerror.Newf(
						spqrerror.SPQR_INVALID_REQUEST,
						"relation \"%s\" is already attached", r.QualifiedName().String())
				} else {
					_, ok := ds.FQNRelations[r.QualifiedName().String()]
					if ok {
						/* error */
						return spqrerror.Newf(
							spqrerror.SPQR_INVALID_REQUEST,
							"relation \"%s\" is already attached", r.QualifiedName().String())

					} else {
						ds.FQNRelations[r.QualifiedName().String()] = r

						/* Note we do not store relation distribution index here. */
						return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Distributions, id, ds))
					}
				}
			}

			/* Now attach old-style. */
			ds.Relations[r.Name] = r
			q.State.RelationDistribution[r.Name] = id
			if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.RelationDistribution, r.Name, id)); err != nil {
				return err
			}
		}

		return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Distributions, id, ds))
	}
}

// TODO: unit tests
func (q *MemQDB) AlterDistributionDetach(ctx context.Context, id string, relationFQN *rfqn.RelationFQN) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: detach table from distribution")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.State.Distributions[id]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution \"%s\" not found", id)
	}

	ds.Version++

	if err := q.AlterSequenceDetachRelation(ctx, relationFQN); err != nil {
		return err
	}

	delete(ds.Relations, relationFQN.RelationName)
	if err := ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Distributions, id, ds)); err != nil {
		return err
	}

	err := ExecuteCommands(q.DumpState, NewDeleteCommand(q.State.RelationDistribution, relationFQN.RelationName))
	return err
}

// TODO : unit tests
func (q *MemQDB) AlterDistributedRelation(_ context.Context, id string, rel *DistributedRelation) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: alter distributed relation")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.State.Distributions[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}
	ds.Version++
	if dsID, ok := q.State.RelationDistribution[rel.Name]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", rel.Name)
	} else if dsID != id {
		return spqrerror.Newf(
			spqrerror.SPQR_INVALID_REQUEST,
			"relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"",
			rel.QualifiedName().String(), dsID, id)
	}

	rel.Version = ds.Relations[rel.Name].Version + 1
	ds.Relations[rel.Name] = rel

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Distributions, id, ds), NewUpdateCommand(q.State.RelationDistribution, rel.Name, id))
}

// TODO : unit tests
// TODO: explicitly pass version
func (q *MemQDB) AlterDistributedRelationSchema(_ context.Context, id string, relation *rfqn.RelationFQN, schemaName string) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: alter distributed relation schema")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.State.Distributions[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}
	ds.Version++
	if dsID, ok := q.State.RelationDistribution[relation.RelationName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relation.String())
	} else if dsID != id {
		return spqrerror.Newf(
			spqrerror.SPQR_INVALID_REQUEST,
			"relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"",
			relation.String(), dsID, id)
	}

	rel, ok := ds.Relations[relation.RelationName]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "relation \"%s\" is attached to distribution \"%s\", but distribution does not contain it", relation.String(), ds.ID)
	}
	rel.SchemaName = schemaName
	rel.Version++

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Distributions, id, ds), NewUpdateCommand(q.State.RelationDistribution, relation.RelationName, id))
}

// TODO : unit tests
func (q *MemQDB) AlterReplicatedRelationSchema(_ context.Context, id string, relation *rfqn.RelationFQN, schemaName string) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: alter distributed relation schema")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.State.Distributions[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}
	if dsID, ok := q.State.RelationDistribution[relation.RelationName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relation.String())
	} else if dsID != id {
		return spqrerror.Newf(
			spqrerror.SPQR_INVALID_REQUEST,
			"relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"",
			relation.String(), dsID, id)
	}

	rel, ok := q.State.ReferenceRelations[relation.RelationName]
	if !ok {
		return fmt.Errorf("reference relation \"%s\" not found", relation.String())
	}

	dsRel, ok := ds.Relations[relation.RelationName]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "relation \"%s\" is attached to distribution \"%s\", but distribution does not contain it", relation.String(), ds.ID)
	}
	dsRel.SchemaName = schemaName
	dsRel.Version++
	rel.SchemaName = schemaName
	rel.Version++

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Distributions, id, ds), NewUpdateCommand(q.State.ReferenceRelations, relation.RelationName, rel))
}

// TODO : unit tests
func (q *MemQDB) AlterDistributedRelationDistributionKey(_ context.Context, id string, relation *rfqn.RelationFQN, distributionKey []DistributionKeyEntry) error {
	spqrlog.Zero.Debug().Str("distribution", id).Msg("memqdb: alter distributed relation distribution key")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.State.Distributions[id]
	if !ok {
		return spqrerror.New(spqrerror.SPQR_OBJECT_NOT_EXIST, "no such distribution")
	}
	if dsID, ok := q.State.RelationDistribution[relation.RelationName]; !ok {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is not attached", relation.String())
	} else if dsID != id {
		return spqrerror.Newf(spqrerror.SPQR_INVALID_REQUEST, "relation \"%s\" is attached to distribution \"%s\", attempt to alter in distribution \"%s\"", relation.String(), dsID, id)
	}
	rel, ok := ds.Relations[relation.RelationName]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "relation \"%s\" is attached to distribution \"%s\", but distribution does not contain it", relation.String(), ds.ID)
	}

	ds.Version++
	rel.DistributionKey = distributionKey
	rel.Version++

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Distributions, id, ds), NewUpdateCommand(q.State.RelationDistribution, relation.RelationName, id))
}

// TODO : unit tests
func (q *MemQDB) GetDistribution(_ context.Context, id string) (*Distribution, error) {
	spqrlog.Zero.Debug().Str("id", id).Msg("memqdb: get distribution")
	q.mu.RLock()
	defer q.mu.RUnlock()

	if ds, ok := q.State.Distributions[id]; !ok {
		// DEPRECATE this
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution \"%s\" not found", id)
	} else {
		return ds.Copy(), nil
	}
}

// TODO : unit tests
func (q *MemQDB) CheckDistribution(_ context.Context, id string) (bool, error) {
	spqrlog.Zero.Debug().Str("id", id).Msg("memqdb: check distribution")
	q.mu.RLock()
	defer q.mu.RUnlock()

	_, ok := q.State.Distributions[id]
	return ok, nil
}

func (q *MemQDB) relationDistributionInternal(relation *rfqn.RelationFQN) (*Distribution, error) {
	if ds, ok := q.State.RelationDistribution[relation.RelationName]; !ok {
		return nil, spqrerror.Newf(spqrerror.SPQR_OBJECT_NOT_EXIST, "distribution for relation \"%s\" not found", relation)
	} else {
		// if there is no distr by key ds
		// then we have corruption
		return q.State.Distributions[ds].Copy(), nil
	}
}

func (q *MemQDB) GetRelationDistribution(_ context.Context, relation *rfqn.RelationFQN) (*Distribution, error) {
	spqrlog.Zero.Debug().Str("relation", relation.RelationName).Msg("memqdb: get distribution for table")
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.relationDistributionInternal(relation)
}

// ==============================================================================
//                               UNIQUE INDEXES
// ==============================================================================

func (q *MemQDB) ListUniqueIndexes(_ context.Context) (map[string]*UniqueIndex, error) {
	spqrlog.Zero.Debug().
		Msg("memqdb: list unique indexes")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.State.UniqueIndexes, nil
}

func (q *MemQDB) CreateUniqueIndex(_ context.Context, idx *UniqueIndex) error {
	spqrlog.Zero.Debug().
		Msg("memqdb: create unique index")
	q.mu.Lock()
	defer q.mu.Unlock()

	ds, ok := q.State.Distributions[idx.DistributionId]
	if !ok {
		return fmt.Errorf("cannot create unique index: distribution \"%s\" not found", idx.DistributionId)
	}
	ds.UniqueIndexes[idx.ID] = idx

	idxs, ok := q.State.UniqueIndexesByRel[idx.Relation.String()]
	if !ok {
		idxs = make(map[string]*UniqueIndex)
	}
	for _, col := range idx.ColumnNames {
		idxs[col] = idx
	}

	q.State.UniqueIndexes[idx.ID] = idx
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Distributions, ds.ID, ds), NewUpdateCommand(q.State.UniqueIndexes, idx.ID, idx), NewUpdateCommand(q.State.UniqueIndexesByRel, idx.Relation.String(), idxs))
}

func (q *MemQDB) DropUniqueIndex(_ context.Context, id string) error {
	spqrlog.Zero.Debug().
		Msg("memqdb: drop unique index")
	q.mu.Lock()
	defer q.mu.Unlock()

	idx, ok := q.State.UniqueIndexes[id]
	if !ok {
		return fmt.Errorf("unique index \"%s\" not found", id)
	}

	ds, ok := q.State.Distributions[idx.DistributionId]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "unique index \"%s\" belongs to nonexistent distribution \"%s\"", idx.ID, idx.DistributionId)
	}
	delete(ds.UniqueIndexes, idx.ID)

	idxs, ok := q.State.UniqueIndexesByRel[idx.Relation.String()]
	if !ok {
		return spqrerror.Newf(spqrerror.SPQR_METADATA_CORRUPTION, "unique index \"%s\" belongs to relation \"%s\", but index record not found", idx.ID, idx.Relation.String())
	}
	for _, col := range idx.ColumnNames {
		delete(idxs, col)
	}

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.Distributions, ds.ID, ds), NewUpdateCommand(q.State.UniqueIndexesByRel, idx.Relation.String(), idxs), NewDeleteCommand(q.State.UniqueIndexes, idx.ID))
}

func (q *MemQDB) ListRelationIndexes(_ context.Context, relationFQN *rfqn.RelationFQN) (map[string]*UniqueIndex, error) {

	q.mu.Lock()
	defer q.mu.Unlock()

	return q.State.UniqueIndexesByRel[relationFQN.String()], nil
}

// ==============================================================================
//                                   TASKS
// ==============================================================================

func (q *MemQDB) ListTaskGroups(_ context.Context) (map[string]*MoveTaskGroup, error) {
	spqrlog.Zero.Debug().
		Msg("memqdb: list task groups")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.State.MoveTaskGroups, nil
}

// TODO: unit tests
func (q *MemQDB) GetMoveTaskGroup(_ context.Context, id string) (*MoveTaskGroup, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: get task group")
	q.mu.RLock()
	defer q.mu.RUnlock()

	group, ok := q.State.MoveTaskGroups[id]
	if !ok {
		return nil, nil
	}
	return group, nil
}

// TODO: unit tests
func (q *MemQDB) WriteMoveTaskGroup(_ context.Context, id string, group *MoveTaskGroup, _ int64, _ *MoveTask) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: write task group")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.State.MoveTaskGroups[id]; ok {
		return fmt.Errorf("could not write move task group: task group with ID \"%s\" already exists", id)
	}

	q.State.MoveTaskGroups[id] = group
	if group.Issuer != nil && group.Issuer.Type == IssuerRedistributeTask {
		q.State.RedistributeTaskTaskGroupId[group.Issuer.Id] = id
	}
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.MoveTaskGroups, id, group))
}

// TODO: unit tests
func (q *MemQDB) DropMoveTaskGroup(_ context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: remove task group")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.State.MoveTaskGroups, id)
	delete(q.State.StopMoveTaskGroup, id)
	return nil
}

// TODO: unit tests
func (q *MemQDB) GetMoveTaskGroupTotalKeys(_ context.Context, id string) (int64, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: get task group total keys")
	q.mu.RLock()
	defer q.mu.RUnlock()

	val, ok := q.State.TotalKeys[id]
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

	q.State.TotalKeys[id] = totalKeys
	return nil
}

// TODO: unit tests
func (q *MemQDB) AddMoveTaskGroupStopFlag(_ context.Context, id string, immediate bool) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: put task group stop flag")
	q.mu.Lock()
	defer q.mu.Unlock()

	kind := StopTaskGroup
	if immediate {
		kind = StopTaskGroupimmediate
	}

	q.State.StopMoveTaskGroup[id] = kind
	return nil
}

// TODO: unit tests
func (q *MemQDB) CheckMoveTaskGroupStopFlag(_ context.Context, id string) (bool, bool, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: put task group stop flag")
	q.mu.RLock()
	defer q.mu.RUnlock()

	val, ok := q.State.StopMoveTaskGroup[id]
	if !ok {
		return false, false, nil
	}

	return true, val == StopTaskGroupimmediate, nil
}

func (q *MemQDB) GetMoveTaskByGroup(_ context.Context, taskGroupID string) (*MoveTask, error) {
	spqrlog.Zero.Debug().
		Str("task group id", taskGroupID).
		Msg("memqdb: get move task of a task group")
	q.mu.RLock()
	defer q.mu.RUnlock()

	id, ok := q.State.TaskGroupMoveTaskID[taskGroupID]
	if !ok {
		return nil, nil
	}
	task, ok := q.State.MoveTasks[id]
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

	return q.State.MoveTasks, nil
}

// TODO: unit tests
func (q *MemQDB) GetMoveTask(_ context.Context, id string) (*MoveTask, error) {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: get move task")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.State.MoveTasks[id], nil
}

// TODO: unit tests
func (q *MemQDB) WriteMoveTask(_ context.Context, task *MoveTask) error {
	spqrlog.Zero.Debug().
		Str("id", task.ID).
		Msg("memqdb: write move task")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.State.MoveTasks[task.ID]; ok {
		return fmt.Errorf("failed to write move task: another task already exists")
	}
	q.State.MoveTasks[task.ID] = task
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.MoveTasks, task.ID, task))
}

// TODO: unit tests
func (q *MemQDB) UpdateMoveTask(_ context.Context, task *MoveTask) error {
	spqrlog.Zero.Debug().
		Str("id", task.ID).
		Msg("memqdb: update move task")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.State.MoveTasks[task.ID]; !ok {
		return fmt.Errorf("failed to update move task: IDs differ")
	}

	q.State.MoveTasks[task.ID] = task
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.MoveTasks, task.ID, task))
}

// TODO: unit tests
func (q *MemQDB) DropMoveTask(_ context.Context, id string) error {
	spqrlog.Zero.Debug().
		Str("id", id).
		Msg("memqdb: remove move task")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.State.MoveTasks, id)
	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.State.MoveTasks, id))
}

// TODO: unit tests
func (q *MemQDB) ListRedistributeTasks(_ context.Context) ([]*RedistributeTask, error) {
	spqrlog.Zero.Debug().Msg("memqdb: list redistribute tasks")
	q.mu.RLock()
	defer q.mu.RUnlock()

	res := make([]*RedistributeTask, 0, len(q.State.RedistributeTasks))
	for _, task := range q.State.RedistributeTasks {
		res = append(res, task)
	}
	return res, nil
}

// TODO: unit tests
func (q *MemQDB) GetRedistributeTask(_ context.Context, id string) (*RedistributeTask, error) {
	spqrlog.Zero.Debug().Str("id", id).Msg("memqdb: get redistribute task")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.State.RedistributeTasks[id], nil
}

// TODO: unit tests
func (q *MemQDB) CreateRedistributeTask(_ context.Context, task *RedistributeTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("memqdb: create redistribute task")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.State.RedistributeTasks[task.ID]; ok {
		return fmt.Errorf("could not create redistribute task: redistribute task with ID \"%s\" already exists in QDB", task.ID)
	}
	if _, ok := q.State.KeyRangeRedistributeTasks[task.KeyRangeId]; ok {
		return fmt.Errorf("could not create redistribute task: task for key range \"%s\" already exists", task.KeyRangeId)
	}
	q.State.RedistributeTasks[task.ID] = task
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.RedistributeTasks, task.ID, task))
}

// TODO: unit tests
func (q *MemQDB) UpdateRedistributeTask(_ context.Context, task *RedistributeTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("memqdb: update redistribute task")
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.State.RedistributeTasks[task.ID]; !ok {
		return fmt.Errorf("could not update redistribute task: redistribute task with ID \"%s\" doesn't exist in QDB", task.ID)
	}
	q.State.RedistributeTasks[task.ID] = task
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.RedistributeTasks, task.ID, task))
}

// TODO: unit tests
func (q *MemQDB) DropRedistributeTask(_ context.Context, task *RedistributeTask) error {
	spqrlog.Zero.Debug().Str("id", task.ID).Msg("memqdb: remove redistribute task")
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.State.RedistributeTasks, task.ID)
	delete(q.State.KeyRangeRedistributeTasks, task.KeyRangeId)
	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.State.RedistributeTasks, task.ID), NewDeleteCommand(q.State.KeyRangeRedistributeTasks, task.KeyRangeId))
}

func (q *MemQDB) GetRedistributeTaskTaskGroupId(_ context.Context, id string) (string, error) {
	spqrlog.Zero.Debug().Str("id", id).Msg("memqdb: get redistribute task task group ID")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.State.RedistributeTaskTaskGroupId[id], nil
}

func (q *MemQDB) GetKeyRangeRedistributeTaskId(_ context.Context, _ string) (string, error) {
	return "", fmt.Errorf("not implemented")
}

// TODO: unit tests
func (q *MemQDB) GetBalancerTask(_ context.Context) (*BalancerTask, error) {
	spqrlog.Zero.Debug().Msg("memqdb: get balancer task")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.State.BalancerTask, nil
}

// TODO: unit tests
func (q *MemQDB) WriteBalancerTask(_ context.Context, task *BalancerTask) error {
	spqrlog.Zero.Debug().Msg("memqdb: write balancer task")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.State.BalancerTask = task
	return nil
}

// TODO: unit tests
func (q *MemQDB) DropBalancerTask(_ context.Context) error {
	spqrlog.Zero.Debug().Msg("memqdb: remove balancer task")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.State.BalancerTask = nil
	return nil
}

func (q *MemQDB) WriteTaskGroupStatus(_ context.Context, id string, status *TaskGroupStatus) error {
	spqrlog.Zero.Debug().
		Str("task group ID", id).
		Str("state", status.State).
		Str("msg", status.Message).
		Msg("memqdb: write task group status")
	q.mu.Lock()
	defer q.mu.Unlock()

	q.State.TaskGroupIDToStatus[id] = status
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.TaskGroupIDToStatus, id, status))
}

func (q *MemQDB) GetTaskGroupStatus(_ context.Context, id string) (*TaskGroupStatus, error) {
	spqrlog.Zero.Debug().
		Str("task group ID", id).
		Msg("memqdb: get task group status")
	q.mu.RLock()
	defer q.mu.RUnlock()

	status := q.State.TaskGroupIDToStatus[id]
	return status, nil
}

func (q *MemQDB) GetAllTaskGroupStatuses(_ context.Context) (map[string]*TaskGroupStatus, error) {
	spqrlog.Zero.Debug().
		Msg("memqdb: get task groups statuses")
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.State.TaskGroupIDToStatus, nil
}

// ==============================================================================
//                                 SEQUENCES
// ==============================================================================

func (q *MemQDB) createSequenceQdbStatements(seqName string, initialValue int64) ([]QdbStatement, error) {
	cmd1, err := NewQdbStatementExt(CmdPut, seqName, true, MapSequences)
	if err != nil {
		return nil, err
	}
	cmd2, err := NewQdbStatementExt(CmdPut, seqName, initialValue, MapSequenceToValues)
	if err != nil {
		return nil, err
	}
	return []QdbStatement{*cmd1, *cmd2}, nil
}

func (q *MemQDB) CreateSequence(_ context.Context, seqName string, initialValue int64) ([]QdbStatement, error) {
	spqrlog.Zero.Debug().
		Str("sequence", seqName).Msg("memqdb: alter sequence attach")
	return q.createSequenceQdbStatements(seqName, initialValue)
}

func (q *MemQDB) CheckSequence(_ context.Context, seqName string) (bool, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	_, ok := q.State.Sequences[seqName]
	return ok, nil
}

func (q *MemQDB) AlterSequenceAttach(_ context.Context, seqName string, relationFQN *rfqn.RelationFQN, colName string) error {
	spqrlog.Zero.Debug().
		Str("sequence", seqName).
		Str("relation", relationFQN.RelationName).
		Str("column", colName).Msg("memqdb: alter sequence attach")

	if _, ok := q.State.Sequences[seqName]; !ok {
		return fmt.Errorf("sequence %s does not exist", seqName)
	}

	key := fmt.Sprintf("%s_%s", relationFQN, colName)
	q.State.ColumnSequence[key] = seqName
	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.ColumnSequence, key, seqName))
}

func (q *MemQDB) AlterSequenceDetachRelation(_ context.Context, relationFQN *rfqn.RelationFQN) error {
	spqrlog.Zero.Debug().
		Str("relation", relationFQN.RelationName).
		Msg("memqdb: detach relation from sequence")

	for col := range q.State.ColumnSequence {
		rel := strings.Split(col, "_")[0]
		if rel == relationFQN.RelationName {
			if err := ExecuteCommands(q.DumpState, NewDeleteCommand(q.State.ColumnSequence, col)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (q *MemQDB) GetSequenceRelations(_ context.Context, seqName string) ([]*rfqn.RelationFQN, error) {
	rels := []*rfqn.RelationFQN{}
	for col, seq := range q.State.ColumnSequence {
		if seq == seqName {
			s := strings.Split(col, "_")
			relName := s[len(s)-2]
			rels = append(rels, &rfqn.RelationFQN{RelationName: relName})
		}
	}
	return rels, nil
}

func (q *MemQDB) DropSequence(_ context.Context, seqName string, force bool) error {
	for col, colSeq := range q.State.ColumnSequence {
		if colSeq == seqName && !force {
			data := strings.Split(col, "_")
			relName := data[0]
			colName := data[1]
			return spqrerror.Newf(spqrerror.SPQR_SEQUENCE_ERROR, "column %q is attached to sequence", fmt.Sprintf("%s.%s", relName, colName))
		}
	}

	if _, ok := q.State.Sequences[seqName]; !ok {
		return nil
	}

	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.State.Sequences, seqName),
		NewDeleteCommand(q.State.SequenceToValues, seqName))
}

func (q *MemQDB) GetRelationSequence(_ context.Context, relationFQN *rfqn.RelationFQN) (map[string]string, error) {
	spqrlog.Zero.Debug().
		Str("relation", relationFQN.RelationName).
		Interface("mapping", q.State.ColumnSequence).Msg("memqdb: get relation sequence")

	mapping := map[string]string{}
	for key, seqName := range q.State.ColumnSequence {
		data := strings.Split(key, "_")
		seqRelName := data[0]
		colName := data[1]

		if seqRelName == relationFQN.RelationName {
			mapping[colName] = seqName
		}
	}
	return mapping, nil
}

func (q *MemQDB) ListSequences(_ context.Context) ([]string, error) {
	seqNames := []string{}
	for seqName := range q.State.Sequences {
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

	nextval := q.State.SequenceToValues[seqName] + 1

	if idRange, err := NewRangeBySize(nextval, rangeSize); err != nil {
		return nil, fmt.Errorf("invalid id-range request: current=%d, request for=%d", nextval, rangeSize)
	} else {

		q.State.SequenceToValues[seqName] = idRange.Right
		if errDB := ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.SequenceToValues, seqName, idRange.Right)); errDB != nil {
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

	next := q.State.SequenceToValues[seqName]
	return next, nil
}

func (q *MemQDB) toRelationDistributionOperation(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CmdDelete:
		return NewDeleteCommand(q.State.RelationDistribution, stmt.Key), nil
	case CmdPut:
		val, ok := stmt.Value.(string)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for CMD_PUT, string is expected", stmt.Value)
		}
		return NewUpdateCommand(q.State.RelationDistribution, stmt.Key, val), nil
	default:
		return nil, fmt.Errorf("unsupported memqdb cmd %d (relation distribution)", stmt.CmdType)
	}
}
func (q *MemQDB) toDistributions(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CmdDelete:
		return NewDeleteCommand(q.State.Distributions, stmt.Key), nil
	case CmdPut:
		var distr Distribution
		val, ok := stmt.Value.(string)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for CMD_PUT, string is expected", stmt.Value)
		}
		if err := json.Unmarshal([]byte(val), &distr); err != nil {
			return nil, err
		} else {
			return NewUpdateCommand(q.State.Distributions, stmt.Key, &distr), nil
		}
	default:
		return nil, fmt.Errorf("unsupported memqdb cmd %d (distributions)", stmt.CmdType)
	}
}

func (q *MemQDB) toKeyRange(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CmdDelete:
		return NewDeleteCommand(q.State.Krs, stmt.Key), nil
	case CmdPut:
		val, ok := stmt.Value.(string)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for CMD_PUT, string is expected", stmt.Value)
		}
		var kr KeyRange
		if err := json.Unmarshal([]byte(val), &kr); err != nil {
			return nil, err
		}
		return NewUpdateCommand(q.State.Krs, stmt.Key, keyRangeToInternal(&kr)), nil
	default:
		return nil, fmt.Errorf("unsupported memDB cmd %d (key range)", stmt.CmdType)
	}
}

func (q *MemQDB) toFreq(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CmdDelete:
		return NewDeleteCommand(q.State.Freq, stmt.Key), nil
	case CmdPut:
		valFreq := true
		if stmt.Value == "false" {
			valFreq = false
		}
		return NewUpdateCommand(q.State.Freq, stmt.Key, valFreq), nil
	default:
		return nil, fmt.Errorf("unsupported memDB cmd %d (freq)", stmt.CmdType)
	}
}

func (q *MemQDB) toLock(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CmdDelete:
		return NewDeleteCommand(q.State.Locks, stmt.Key), nil
	case CmdPut:
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
		return NewUpdateCommand(q.State.Locks, stmt.Key, lock), nil
	default:
		return nil, fmt.Errorf("unsupported memDB cmd %d (lock)", stmt.CmdType)
	}
}

func (q *MemQDB) toKrVersion(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CmdDelete:
		return NewDeleteCommand(q.State.KrVersions, stmt.Key), nil
	case CmdPut:
		val, ok := stmt.Value.(int)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for MapKrVersions, int is expected", stmt.Value)
		}
		return NewUpdateCommand(q.State.KrVersions, stmt.Key, val), nil
	default:
		return nil, fmt.Errorf("unsupported memDB cmd %d (lock)", stmt.CmdType)
	}
}

func (q *MemQDB) toSequences(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CmdDelete:
		return NewDeleteCommand(q.State.Sequences, stmt.Key), nil
	case CmdPut:
		val, ok := stmt.Value.(bool)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for MapSequences, bool is expected", stmt.Value)
		}
		return NewUpdateCommand(q.State.Sequences, stmt.Key, val), nil
	default:
		return nil, fmt.Errorf("unsupported memDB cmd %d (Sequences)", stmt.CmdType)
	}
}

func (q *MemQDB) toSequenceToValues(stmt QdbStatement) (Command, error) {
	switch stmt.CmdType {
	case CmdDelete:
		return NewDeleteCommand(q.State.SequenceToValues, stmt.Key), nil
	case CmdPut:
		val, ok := stmt.Value.(int64)
		if !ok {
			return nil, fmt.Errorf("incorrect value type %T for MapSequenceToValues, int64 is expected", stmt.Value)
		}
		return NewUpdateCommand(q.State.SequenceToValues, stmt.Key, val), nil
	default:
		return nil, fmt.Errorf("unsupported memDB cmd %d (SequenceToValues)", stmt.CmdType)
	}
}

func (q *MemQDB) packMemqdbCommands(operations []QdbStatement) ([]Command, error) {
	memOperations := make([]Command, 0, len(operations))
	for _, stmt := range operations {
		var converterToCmd func(QdbStatement) (Command, error)
		switch stmt.Extension {
		case MapRelationDistribution:
			converterToCmd = q.toRelationDistributionOperation
		case MapDistributions:
			converterToCmd = q.toDistributions
		case MapKrs:
			converterToCmd = q.toKeyRange
		case MapFreq:
			converterToCmd = q.toFreq
		case MapLocks:
			converterToCmd = q.toLock
		case MapKrVersions:
			converterToCmd = q.toKrVersion
		case MapSequences:
			converterToCmd = q.toSequences
		case MapSequenceToValues:
			converterToCmd = q.toSequenceToValues
		default:
			return nil, fmt.Errorf("not implemented for transaction memqdb part %s", stmt.Extension)
		}
		operation, err := converterToCmd(stmt)
		if err != nil {
			return nil, err
		}
		memOperations = append(memOperations, operation)
	}
	return memOperations, nil
}

func (q *MemQDB) ExecNoTransaction(_ context.Context, operations []QdbStatement) error {
	spqrlog.Zero.Debug().Msg("memqdb: exec chunk commands without transaction")
	q.mu.Lock()
	defer q.mu.Unlock()
	if memOperations, err := q.packMemqdbCommands(operations); err != nil {
		return err
	} else {
		return ExecuteCommands(q.DumpState, memOperations...)
	}
}

func (q *MemQDB) CommitTransaction(_ context.Context, transaction *QdbTransaction) error {
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
func (q *MemQDB) ChangeTxStatus(_ context.Context, id string, state TwoPhaseTxState) error {
	spqrlog.Zero.Debug().Msg("memqdb: ChangeTxStatus")
	q.mu.Lock()
	defer q.mu.Unlock()

	/* XXX: validate state outer layers? */

	info := q.State.TwoPhaseTx[id]
	info.State = state

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.TwoPhaseTx, id, info))
}

func (q *MemQDB) AcquireTxOwnership(_ context.Context, id string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if info, ok := q.State.TwoPhaseTx[id]; ok {
		if info.Locked {
			return false, nil
		}
		info.Locked = true
		return true, nil
	}
	info := &TwoPCInfo{
		Gid:       id,
		ShardsIDs: nil,
		State:     TwoPhaseInitState,
		Locked:    true,
	}

	q.State.TwoPhaseTx[id] = info
	return true, nil
}

func (q *MemQDB) ReleaseTxOwnership(_ context.Context, gid string) error {
	spqrlog.Zero.Debug().Str("gid", gid).Msg("memqdb: ReleaseTxOwnership")
	q.mu.Lock()
	defer q.mu.Unlock()

	if info, ok := q.State.TwoPhaseTx[gid]; ok {
		info.Locked = false
	}
	return nil
}

// RecordTwoPhaseMembers implements DCStateKeeper.
// XXX: check that all members are valid spqr shards
func (q *MemQDB) RecordTwoPhaseMembers(_ context.Context, id string, shards []string) error {
	spqrlog.Zero.Debug().Msg("memqdb: RecordTwoPhaseMembers")
	q.mu.Lock()
	defer q.mu.Unlock()

	info := &TwoPCInfo{
		Gid:       id,
		ShardsIDs: shards,
		State:     TwoPhaseInitState,
		Locked:    true,
	}

	q.State.TwoPhaseTx[id] = info

	return ExecuteCommands(q.DumpState, NewUpdateCommand(q.State.TwoPhaseTx, id, info))
}

// TXCohortShards implements DCStateKeeper.
func (q *MemQDB) TXCohortShards(_ context.Context, gid string) ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if tx, ok := q.State.TwoPhaseTx[gid]; !ok {
		return nil, fmt.Errorf("could not get two-phase tx info: tx \"%s\" not found", gid)
	} else {
		return tx.ShardsIDs, nil
	}
}

// TXStatus implements DCStateKeeper.
func (q *MemQDB) TXStatus(_ context.Context, gid string) (TwoPhaseTxState, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if tx, ok := q.State.TwoPhaseTx[gid]; !ok {
		return "", fmt.Errorf("could not get two-phase tx info: tx \"%s\" not found", gid)
	} else {
		return tx.State, nil
	}
}

// ListTXNames implements [DCStateKeeper].
func (q *MemQDB) ListTXNames(_ context.Context) ([]string, error) {
	rt := []string{}

	for _, tx := range q.State.TwoPhaseTx {
		rt = append(rt, tx.Gid)
	}
	return rt, nil
}

func (q *MemQDB) GetTXs(_ context.Context) (map[string]*TwoPCInfo, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.State.TwoPhaseTx, nil
}

func (q *MemQDB) SetTxMetaStorage(context.Context, []string) error {
	return nil
}

func (q *MemQDB) GetTxMetaStorage(_ context.Context) ([]string, error) {
	return []string{"local"}, nil
}

func (q *MemQDB) RemoveTXData(_ context.Context, gid string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.State.TwoPhaseTx, gid)
	return ExecuteCommands(q.DumpState, NewDeleteCommand(q.State.TwoPhaseTx, gid))
}

func (q *MemQDB) ClearTxStatuses(_ context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	commands := make([]Command, 0, len(q.State.TwoPhaseTx))
	for gid := range q.State.TwoPhaseTx {
		commands = append(commands, NewDeleteCommand(q.State.TwoPhaseTx, gid))
	}
	q.State.TwoPhaseTx = map[string]*TwoPCInfo{}
	return ExecuteCommands(q.DumpState, commands...)
}

// ==============================================================================
//                               TASK GROUP STATE
// ==============================================================================

func (q *MemQDB) TryTaskGroupLock(_ context.Context, _ string, _ string) error {
	return fmt.Errorf("not implemented")
}

func (q *MemQDB) CheckTaskGroupLocked(_ context.Context, _ string) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (q *MemQDB) DropTaskGroupLock(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}

func (q *MemQDB) LockRedistributeTask(_ context.Context, _, _ string) error {
	return fmt.Errorf("not implemented")
}

func (q *MemQDB) DropRedistributeTaskLock(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}
