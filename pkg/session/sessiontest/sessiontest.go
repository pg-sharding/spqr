package sessiontest

import "github.com/pg-sharding/spqr/pkg/session"

func MustFindBoolGUC(n string) session.BoolGUC {
	guc, err := session.FindBoolGUC(n)
	if err != nil {
		panic(err)
	}
	return guc
}

func MustFindStrGUC(n string) session.StrGUC {
	guc, err := session.FindStrGUC(n)
	if err != nil {
		panic(err)
	}
	return guc
}
