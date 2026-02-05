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

	CopyDataCP = "copy_data_cp"
)

/* XXX: store name -> action? */
var (
	/* Lets keep it simple - performance does not matter here */
	mu    sync.Mutex
	cpsMp = map[string]func(){}
)

var (
	defaultPanicAction = func() {
		panic("reached control point")
	}

	defaultSleepAction = func() {
		time.Sleep(1 * time.Minute)
	}
)

func getAction(A *spqrparser.ICPointAction) func() {
	switch A.Act {
	case "panic":
		return defaultPanicAction
	case "sleep":
		if A.Timeout == time.Duration(0) {
			return defaultSleepAction
		}
		return func() {
			time.Sleep(A.Timeout)
		}
	default:
		return defaultPanicAction
	}
}

func DefineICP(name string, A *spqrparser.ICPointAction) error {
	mu.Lock()
	defer mu.Unlock()

	switch name {
	case TwoPhaseDecisionCP, TwoPhaseDecisionCP2, CopyDataCP:
		/* OK */
	default:
		return fmt.Errorf("unknown control point name %s", name)
	}

	/* OK */
	cpsMp[name] = getAction(A)

	return nil
}

func ResetICP(name string) error {
	mu.Lock()
	defer mu.Unlock()

	switch name {
	case TwoPhaseDecisionCP, TwoPhaseDecisionCP2, CopyDataCP:
		/* OK */
		delete(cpsMp, name)
		return nil
	}

	return fmt.Errorf("unknown control point name %s", name)
}

func CheckControlPoint(name string) error {
	mu.Lock()
	defer mu.Unlock()

	/* XXX: support more behaviour modes */
	if act, ok := cpsMp[name]; ok {
		spqrlog.Zero.Debug().Str("name", name).Msg("reached control point")
		act()
	}
	return nil
}
