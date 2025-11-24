package icp

import "fmt"

/* Known control point list */
const (
	TwoPhaseDecisionCP = "2pc_decision_cp"
)

/* XXX: store name -> action? */
var (
	cpsMp = map[string]struct{}{TwoPhaseDecisionCP: {}}
)

func DefineICP(name string) error {
	switch name {
	case TwoPhaseDecisionCP:
		/* OK */
		cpsMp[name] = struct{}{}
		return nil
	}

	return fmt.Errorf("unknown control point name %s", name)
}

func CheckControlPoint(name string) error {
	/* XXX: support more behaviour modes */
	if _, ok := cpsMp[name]; ok {
		panic(fmt.Sprintf("reached control point %s", name))
	}
	return nil
}
