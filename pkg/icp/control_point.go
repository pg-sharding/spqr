package icp

import (
	"fmt"
	"sync"
)

/* Known control point list */
const (
	TwoPhaseDecisionCP = "2pc_decision_cp"
)

/* XXX: store name -> action? */
var (
	/* Lets keep it simple - performance does not matter here */
	mu    sync.Mutex
	cpsMp = map[string]struct{}{}
)

func DefineICP(name string) error {
	mu.Lock()
	defer mu.Unlock()

	switch name {
	case TwoPhaseDecisionCP:
		/* OK */
		cpsMp[name] = struct{}{}
		return nil
	}

	return fmt.Errorf("unknown control point name %s", name)
}

func ResetICP(name string) error {
	mu.Lock()
	defer mu.Unlock()

	switch name {
	case TwoPhaseDecisionCP:
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
	if _, ok := cpsMp[name]; ok {
		panic(fmt.Sprintf("reached control point %s", name))
	}
	return nil
}
