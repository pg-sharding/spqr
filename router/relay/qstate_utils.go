package relay

import "strings"

func virtualParamTransformName(name string) string {
	retName := name
	if after, ok := strings.CutPrefix(retName, "__spqr__."); ok {
		retName = "__spqr__" + after
	}

	return retName
}
