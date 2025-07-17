package relay

import "strings"

func virtualParamTransformName(name string) string {
	retName := name
	if strings.HasPrefix(retName, "__spqr__.") {
		retName = "__spqr__" + strings.Trim(retName, "__spqr__.")
	}

	return retName
}
