package session

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleSet(t *testing.T) {
	cases := []struct {
		do             func(t SessionParamsHolder)
		expectedParams map[string]string
	}{
		{
			func(t SessionParamsHolder) {
				t.SetTsa(VirtualParamLevelLocal, "read-write")
				t.StartTx()
				t.SetTsa(VirtualParamLevelLocal, "read-only")
				t.CommitActiveSet()
			},
			map[string]string{
				"__spqr__distribution":            "default",
				"__spqr__default_route_behaviour": "",
				"__spqr__target_session_attrs":    "read-write",
			},
		},
		{
			func(t SessionParamsHolder) {
				t.SetParam("x", "1", false)
				t.StartTx()
				t.SetParam("x", "2", true)
				t.Rollback()
			},
			map[string]string{
				"__spqr__distribution":            "default",
				"__spqr__default_route_behaviour": "",
				"x":                               "1",
			},
		},
		{
			func(t SessionParamsHolder) {
				t.SetParam("x", "1", false)
				t.StartTx()
				t.SetParam("x", "2", true)
				t.Savepoint("sp1")
				t.SetParam("x", "3", true)
				t.RollbackToSP("sp1")
				t.CommitActiveSet()
			},
			map[string]string{
				"__spqr__distribution":            "default",
				"__spqr__default_route_behaviour": "",
				"x":                               "1",
			},
		},
		{
			func(t SessionParamsHolder) {
				t.StartTx()
				t.SetParam("x", "1", false)
				t.Rollback()
			},
			map[string]string{
				"__spqr__distribution":            "default",
				"__spqr__default_route_behaviour": "",
			},
		},
		{
			func(t SessionParamsHolder) {
				t.StartTx()
				t.SetTsa(VirtualParamLevelStatement, "read-write")
			},
			map[string]string{
				"__spqr__distribution":            "default",
				"__spqr__default_route_behaviour": "",
				"__spqr__target_session_attrs":    "read-write",
			},
		},
		{
			func(t SessionParamsHolder) {
				t.SetParam("x", "1", false)
				t.StartTx()
				t.ResetAll()
				t.Rollback()
			},
			map[string]string{
				"__spqr__distribution":            "default",
				"__spqr__default_route_behaviour": "",
				"x":                               "1",
			},
		},
	}

	for _, tc := range cases {
		h := NewSimpleHandler("any", false, "", "")
		tc.do(h)

		actualParams := h.Params()
		assert.Equal(t, tc.expectedParams, actualParams)
	}
}

func BenchmarkSimple(b *testing.B) {
	h := NewSimpleHandler("", false, "", "")
	for range b.N {
		h.Params()
		h.SetParam("x", "1", false)
		h.StartTx()
		h.SetParam("x", "2", true)
		h.ResetAll()
		h.CommitActiveSet()
		h.Params()
	}
}
