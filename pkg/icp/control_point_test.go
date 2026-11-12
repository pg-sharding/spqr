package icp

import (
	"testing"

	spqrparser "github.com/pg-sharding/spqr/yacc/console"
)

func TestErrorControlPoint(t *testing.T) {
	if err := DefineICP(AfterInsertCP, &spqrparser.ICPointAction{Act: "error"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ResetICP(AfterInsertCP); err != nil {
			t.Error(err)
		}
	})

	err := CheckControlPoint(nil, AfterInsertCP)
	if err == nil || err.Error() != `injected error at control point "after_insert_cp"` {
		t.Fatalf("unexpected control point error: %v", err)
	}
}
