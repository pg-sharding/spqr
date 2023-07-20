package testutil

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
)

type ctxID string

const ctxIDKeyName ctxID = "context UUID"

type ctxMatcher struct {
	ctx context.Context
}

func (m *ctxMatcher) Matches(x interface{}) bool {
	newCtx, ok := x.(context.Context)
	if !ok {
		return false
	}
	oldVal := m.ctx.Value(ctxIDKeyName)
	newVal := newCtx.Value(ctxIDKeyName)
	return oldVal == newVal
}

func (m *ctxMatcher) String() string {
	return "context matcher"
}

//nolint:revive
func MatchContext(t *testing.T, ctx context.Context) (context.Context, gomock.Matcher) {
	ctx = context.WithValue(ctx, ctxIDKeyName, NewUUIDStr(t))
	return ctx, &ctxMatcher{
		ctx: ctx,
	}
}
