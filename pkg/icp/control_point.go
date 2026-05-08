package icp

import (
	"fmt"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

/* Known control point list */
const (
	TwoPhaseDecisionCP  = "2pc_decision_cp"
	TwoPhaseDecisionCP2 = "2pc_after_decision_cp"

	AfterRenameKeyRangeCP       = "after_rename_key_range_cp"
	AfterSplitKeyRangeCP        = "after_split_key_range_cp"
	AfterLockKeyRangeCP         = "after_lock_key_range_cp"
	CopyDataCP                  = "copy_data_cp"
	AfterCopyDataCP             = "after_copy_data_cp"
	AfterDeleteCP               = "after_delete_cp"
	AfterMoveKeysCP             = "after_move_keys_cp"
	AfterCoordUpdateKeyRangeCP  = "after_coordinator_update_key_range_cp"
	AfterRouterUpdateKeyRangeCP = "after_router_update_key_range_cp"
	AfterUnlockKeyRangeCP       = "after_unlock_key_range_cp"
	AfterMoveCP                 = "after_move_cp"
	AfterUniteKeyRangeCP        = "after_unite_key_range_cp"
	CopyReferenceRelationDataCP = "copy_reference_relation_data_cp"
)

type ICPContextHolder interface {
	Wait()
	Wake()

	CancelPID() uint32
}

/* XXX: store name -> action? */
var (
	/* Lets keep it simple - performance does not matter here */
	mu           sync.Mutex
	cpsMp        = map[string]func(ICPContextHolder){}
	cpsContextMp = map[string]ICPContextHolder{}
	cpsResetMp   = map[string]func(ICPContextHolder){}

	BlockedPIDs = map[uint32]struct{}{}
)

var (
	defaultPanicAction = func(ICPContextHolder) {
		panic("reached control point")
	}

	defaultSleepAction = func(ICPContextHolder) {
		time.Sleep(1 * time.Minute)
	}
)

func getAction(name string, a *spqrparser.ICPointAction) func(ICPContextHolder) {
	switch a.Act {
	case "panic":
		return defaultPanicAction
	case "sleep":
		if a.Timeout == time.Duration(0) {
			return defaultSleepAction
		}
		return func(ICPContextHolder) {
			time.Sleep(a.Timeout)
		}
	case "wait":
		return func(c ICPContextHolder) {
			cpsContextMp[name] = c
			// nil is ok.
			if c != nil {
				BlockedPIDs[c.CancelPID()] = struct{}{}
				c.Wait()
			}
		}
	default:
		return defaultPanicAction
	}
}

func getResetAction(a *spqrparser.ICPointAction) func(ICPContextHolder) {
	switch a.Act {
	case "wait":
		return func(c ICPContextHolder) {
			// nil is ok.
			if c != nil {
				c.Wake()

				delete(BlockedPIDs, c.CancelPID())
			}
		}
	default:
		return func(ICPContextHolder) {
			// noop
		}
	}
}

func DefineICP(name string, a *spqrparser.ICPointAction) error {
	mu.Lock()
	defer mu.Unlock()

	switch name {
	case TwoPhaseDecisionCP, TwoPhaseDecisionCP2, CopyDataCP,
		CopyReferenceRelationDataCP, AfterCopyDataCP, AfterDeleteCP,
		AfterLockKeyRangeCP, AfterMoveKeysCP, AfterCoordUpdateKeyRangeCP,
		AfterRouterUpdateKeyRangeCP, AfterUnlockKeyRangeCP, AfterRenameKeyRangeCP,
		AfterSplitKeyRangeCP, AfterMoveCP, AfterUniteKeyRangeCP:
		/* OK */
	default:
		return fmt.Errorf("unknown control point name %s", name)
	}

	/* OK */

	cpsMp[name] = getAction(name, a)
	cpsResetMp[name] = getResetAction(a)

	return nil
}

func ResetICP(name string) error {
	mu.Lock()
	defer mu.Unlock()

	switch name {
	case TwoPhaseDecisionCP, TwoPhaseDecisionCP2, CopyDataCP,
		CopyReferenceRelationDataCP, AfterCopyDataCP, AfterDeleteCP,
		AfterLockKeyRangeCP, AfterMoveKeysCP, AfterCoordUpdateKeyRangeCP,
		AfterRouterUpdateKeyRangeCP, AfterUnlockKeyRangeCP, AfterRenameKeyRangeCP,
		AfterSplitKeyRangeCP, AfterMoveCP, AfterUniteKeyRangeCP:
		/* OK */

		f, ok := cpsResetMp[name]

		if !ok {
			return fmt.Errorf("control point not attached: %s", name)
		}

		// nil is ok
		c := cpsContextMp[name]
		f(c)

		delete(cpsMp, name)
		delete(cpsResetMp, name)
		return nil
	}

	return fmt.Errorf("unknown control point name %s", name)
}

func CheckControlPoint(c ICPContextHolder, name string) error {
	mu.Lock()
	{
		/* XXX: support more behaviour modes */
		if act, ok := cpsMp[name]; ok {
			spqrlog.Zero.Debug().Str("name", name).Msg("reached control point")
			defer act(c)
		}
	}
	mu.Unlock()
	return nil
}
