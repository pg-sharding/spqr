package rmeta

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/router/rfqn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveRelationByAliasErrorIsolation(t *testing.T) {
	newContext := func() *RoutingMetadataContext {
		return &RoutingMetadataContext{
			RelationsByDistributionCol: map[string][]*rfqn.RelationFQN{
				"id": {{RelationName: "a"}, {RelationName: "b"}},
			},
		}
	}

	_, first := newContext().ResolveRelationByAlias("first", "id")
	var firstErr *spqrerror.SpqrError
	require.ErrorAs(t, first, &firstErr)

	_, second := newContext().ResolveRelationByAlias("second", "id")
	var secondErr *spqrerror.SpqrError
	require.ErrorAs(t, second, &secondErr)

	assert.Equal(t, "relation reference ambiguity by alias/colref: first/id", firstErr.ErrHint)
	assert.Equal(t, "relation reference ambiguity by alias/colref: second/id", secondErr.ErrHint)
}
