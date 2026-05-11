package session_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/stretchr/testify/assert"
)

func TestIsKnownSPQRHint_Known(t *testing.T) {
	is := assert.New(t)

	known := []string{
		session.SPQR_DISTRIBUTION,
		session.SPQR_DISTRIBUTED_RELATION,
		session.SPQR_DEFAULT_ROUTE_BEHAVIOUR,
		session.SPQR_AUTO_DISTRIBUTION,
		session.SPQR_DISTRIBUTION_KEY,
		session.SPQR_SHARDING_KEY,
		session.SPQR_PREFERRED_ENGINE,
		session.SPQR_COMMIT_STRATEGY,
		session.SPQR_TARGET_SESSION_ATTRS,
		session.SPQR_EXECUTE_ON,
		session.SPQR_SCATTER_QUERY,
		session.SPQR_REPLY_NOTICE,
		session.SPQR_MAINTAIN_PARAMS,
		session.SPQR_ENGINE_V2,
		session.SPQR_ALLOW_SPLIT_UPDATE,
		session.SPQR_ALLOW_POSTPROCESSING,
		session.SPQR_LINEARIZE_DISPATCH,
	}

	for _, n := range known {
		is.True(session.IsKnownSPQRHint(n), "expected %q to be known", n)
	}
}

func TestIsKnownSPQRHint_Unknown(t *testing.T) {
	is := assert.New(t)

	unknown := []string{
		"__spqr__no_such_hint",
		"__spqr__",
		"__spqr__distrib",      // typo of distribution
		"__spqr__execute_one",  // typo of execute_on
		"application_name",     // regular postgres GUC, not __spqr__
		"target_session_attrs", // TSA alias lives outside __spqr__ namespace
		"target-session-attrs", // same
		"",
	}

	for _, n := range unknown {
		is.False(session.IsKnownSPQRHint(n), "expected %q to be unknown", n)
	}
}
