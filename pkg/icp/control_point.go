package icp

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

/* Known control point list */
const (
	TwoPhaseBeforePrepareCP = "2pc_before_prepare_cp"
	TwoPhaseDecisionCP      = "2pc_decision_cp"
	TwoPhaseDecisionCP2     = "2pc_after_decision_cp"

	AfterRenameKeyRangeCP       = "after_rename_key_range_cp"
	AfterSplitKeyRangeCP        = "after_split_key_range_cp"
	AfterLockKeyRangeCP         = "after_lock_key_range_cp"
	AwaitPidCP                  = "await_pid_cp"
	CopyDataCP                  = "copy_data_cp"
	BeforeInsertCP              = "before_insert_cp"
	AfterInsertCP               = "after_insert_cp"
	AfterCopyDataCP             = "after_copy_data_cp"
	AfterDeleteQueryCP          = "after_delete_query_cp"
	AfterDeleteCP               = "after_delete_cp"
	AfterMoveKeysCP             = "after_move_keys_cp"
	AfterMoveKeysCP2            = "after_move_keys_before_update_kr_cp"
	AfterCoordUpdateKeyRangeCP  = "after_coordinator_update_key_range_cp"
	AfterRouterUpdateKeyRangeCP = "after_router_update_key_range_cp"
	AfterUnlockKeyRangeCP       = "after_unlock_key_range_cp"
	AfterMoveCP                 = "after_move_cp"
	AfterUniteKeyRangeCP        = "after_unite_key_range_cp"
	CopyReferenceRelationDataCP = "copy_reference_relation_data_cp"
	AfterGangDeployCP           = "after_gang_deploy_cp"
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
	cpsMp        = map[string]func(ICPContextHolder) error{}
	cpsContextMp = map[string]ICPContextHolder{}
	cpsResetMp   = map[string]func(ICPContextHolder){}

	BlockedPIDs = map[uint32]struct{}{}
)

var (
	defaultPanicAction = func(ICPContextHolder) {
		log.Fatal("reached control point")
	}

	defaultSleepAction = func(ICPContextHolder) {
		time.Sleep(1 * time.Minute)
	}
)

func getAction(name string, a *spqrparser.ICPointAction) func(ICPContextHolder) error {
	switch a.Act {
	case "panic":
		return func(c ICPContextHolder) error {
			defaultPanicAction(c)
			return nil
		}
	case "error":
		return func(ICPContextHolder) error {
			return fmt.Errorf("injected error at control point %q", name)
		}
	case "sleep":
		if a.Timeout == time.Duration(0) {
			return func(c ICPContextHolder) error {
				defaultSleepAction(c)
				return nil
			}
		}
		return func(ICPContextHolder) error {
			time.Sleep(a.Timeout)
			return nil
		}
	case "wait":
		return func(c ICPContextHolder) error {
			log.Printf("reached control point '%s'\n", name)
			mu.Lock()
			cpsContextMp[name] = c
			// nil is ok.
			if c != nil {
				BlockedPIDs[c.CancelPID()] = struct{}{}
			}
			mu.Unlock()

			wait := func() error {
				if c != nil {
					log.Println("waiting...")
					c.Wait()
				}
				return nil
			}
			return wait()
		}
	default:
		return func(c ICPContextHolder) error {
			defaultPanicAction(c)
			return nil
		}
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
	case TwoPhaseBeforePrepareCP, TwoPhaseDecisionCP, TwoPhaseDecisionCP2, CopyDataCP,
		CopyReferenceRelationDataCP, AfterCopyDataCP, AfterDeleteCP,
		AfterLockKeyRangeCP, AfterMoveKeysCP, AfterCoordUpdateKeyRangeCP,
		AfterRouterUpdateKeyRangeCP, AfterUnlockKeyRangeCP, AfterRenameKeyRangeCP,
		AfterSplitKeyRangeCP, AfterMoveCP, AfterUniteKeyRangeCP,
		BeforeInsertCP, AfterInsertCP, AfterDeleteQueryCP,
		AfterGangDeployCP, AwaitPidCP:
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

	log.Printf("reset icp '%s'\n", name)

	switch name {
	case TwoPhaseBeforePrepareCP, TwoPhaseDecisionCP, TwoPhaseDecisionCP2, CopyDataCP,
		CopyReferenceRelationDataCP, AfterCopyDataCP, AfterDeleteCP,
		AfterLockKeyRangeCP, AfterMoveKeysCP, AfterCoordUpdateKeyRangeCP,
		AfterRouterUpdateKeyRangeCP, AfterUnlockKeyRangeCP, AfterRenameKeyRangeCP,
		AfterSplitKeyRangeCP, AfterMoveCP, AfterUniteKeyRangeCP,
		BeforeInsertCP, AfterInsertCP, AfterDeleteQueryCP,
		AfterGangDeployCP, AwaitPidCP:
		/* OK */

		f, ok := cpsResetMp[name]

		if !ok {
			return fmt.Errorf("control point not attached: %s", name)
		}

		log.Printf("finish resetting icp '%s'\n", name)
		// nil is ok
		c := cpsContextMp[name]
		f(c)

		delete(cpsMp, name)
		delete(cpsResetMp, name)
		return nil
	}

	return fmt.Errorf("unknown control point name %s", name)
}

func IsPIDBlocked(pid uint32) bool {
	mu.Lock()
	defer mu.Unlock()
	_, ok := BlockedPIDs[pid]
	return ok
}

func CheckControlPoint(c ICPContextHolder, name string) error {
	mu.Lock()
	act := cpsMp[name]
	mu.Unlock()

	if act == nil {
		return nil
	}

	spqrlog.Zero.Debug().Str("name", name).Msg("reached control point")
	return act(c)
}
