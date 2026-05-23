package spqrerror_test

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/stretchr/testify/assert"
)

func TestShardNotFound(t *testing.T) {
	err := spqrerror.ShardNotFound("shard2")

	assert.Equal(t, spqrerror.SPQR_NO_DATASHARD, err.ErrorCode)
	assert.Equal(t, "Shard \"shard2\" not found.", err.Error())
	assert.Equal(t, "Run 'SHOW shards' to see all configured shards.", err.ErrHint)
}

func TestCleanGrpcErrorRestoresSpqrErrorMetadata(t *testing.T) {
	original := spqrerror.ShardNotFound("shard2").
		Detail("shard lookup failed").
		Context("key range validation").
		Pos(42).
		Query("CREATE KEY RANGE krid2")

	cleanErr := spqrerror.CleanGrpcError(spqrerror.ToGrpcError(original))

	spErr, ok := cleanErr.(*spqrerror.SpqrError)
	assert.True(t, ok)
	assert.Equal(t, original.ErrorCode, spErr.ErrorCode)
	assert.Equal(t, original.Error(), spErr.Error())
	assert.Equal(t, original.ErrHint, spErr.ErrHint)
	assert.Equal(t, original.ErrDetail, spErr.ErrDetail)
	assert.Equal(t, original.ErrContext, spErr.ErrContext)
	assert.Equal(t, original.Position, spErr.Position)
	assert.Equal(t, original.InternalQuery, spErr.InternalQuery)
}
