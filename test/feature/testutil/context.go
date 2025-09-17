package testutil

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type ctxID string

const ctxIDKeyName ctxID = "context UUID"

type ctxMatcher struct {
	ctx context.Context
}

func (m *ctxMatcher) Matches(x any) bool {
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

func MatchContext(t *testing.T, ctx context.Context) (context.Context, gomock.Matcher) {
	uuidVal, err := uuid.NewRandom()
	require.NoError(t, err)
	ctx = context.WithValue(ctx, ctxIDKeyName, uuidVal.String())
	return ctx, &ctxMatcher{
		ctx: ctx,
	}
}
