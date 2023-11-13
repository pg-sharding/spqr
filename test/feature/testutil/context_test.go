package testutil_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pg-sharding/spqr/test/feature/testutil"
)

type someKey string

const someKeyName someKey = "key"

func TestMatchContext(t *testing.T) {
	tcs := []struct {
		newCtx func(context.Context) context.Context
		exp    bool
	}{
		{
			newCtx: func(ctx context.Context) context.Context {
				return context.WithValue(ctx, someKeyName, "asd")
			},
			exp: true,
		},
		{
			newCtx: func(ctx context.Context) context.Context {
				return context.Background()
			},
			exp: false,
		},
		{
			newCtx: func(ctx context.Context) context.Context {
				ctx, cancel := context.WithCancel(ctx)
				cancel()
				return ctx
			},
			exp: true,
		},
	}

	for _, tc := range tcs {
		t.Run("", func(t *testing.T) {
			ctx, matcher := testutil.MatchContext(t, context.Background())
			require.Equal(t, tc.exp, matcher.Matches(tc.newCtx(ctx)))
		})
	}
}
