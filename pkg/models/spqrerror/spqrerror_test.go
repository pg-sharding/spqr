package spqrerror_test

import (
	"fmt"
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	for _, tt := range []struct {
		input error
		code  string
	}{
		{original, spqrerror.SPQR_NO_DATASHARD},
		{fmt.Errorf("split failed: %w", original), spqrerror.SPQR_NO_DATASHARD},
		{spqrerror.Wrap(original, spqrerror.SPQR_KEYRANGE_ERROR, "split failed"), spqrerror.SPQR_KEYRANGE_ERROR},
	} {
		assert.ErrorIs(t, tt.input, original)
		cleanErr := spqrerror.CleanGrpcError(spqrerror.ToGrpcError(tt.input))

		var spErr *spqrerror.SpqrError
		if assert.ErrorAs(t, cleanErr, &spErr) {
			assert.Equal(t, tt.code, spErr.ErrorCode)
			assert.Equal(t, tt.input.Error(), spErr.Error())
			assert.Equal(t, original.ErrHint, spErr.ErrHint)
			assert.Equal(t, original.ErrDetail, spErr.ErrDetail)
			assert.Equal(t, original.ErrContext, spErr.ErrContext)
			assert.Equal(t, original.Position, spErr.Position)
			assert.Equal(t, original.InternalQuery, spErr.InternalQuery)
		}
	}
	assert.Equal(t, spqrerror.SPQR_NO_DATASHARD, original.ErrorCode)
}

func TestCleanGrpcErrorPreservesSpqrErrorWithGrpcCause(t *testing.T) {
	cause := status.Error(codes.Unavailable, "etcd unavailable")
	original := spqrerror.Wrap(cause, spqrerror.SPQR_KEYRANGE_ERROR, "failed to commit a new key range")
	for _, input := range []error{original, fmt.Errorf("split failed: %w", original)} {
		cleanErr := spqrerror.CleanGrpcError(input)
		assert.Same(t, input, cleanErr)
		assert.ErrorIs(t, cleanErr, cause)
	}
}

func TestMetadataErrors(t *testing.T) {
	for _, tt := range []struct {
		name                string
		err                 *spqrerror.SpqrError
		code, message, hint string
	}{
		{"distribution", spqrerror.DistributionNotFound("ds"), spqrerror.SPQR_OBJECT_NOT_EXIST, `distribution "ds" not found`, "Run 'SHOW distributions' to see all configured distributions."},
		{"relation", spqrerror.RelationNotFound("sales.orders", "ds"), spqrerror.SPQR_OBJECT_NOT_EXIST, `relation "sales.orders" not found in distribution "ds"`, "Run 'SHOW relations' to see the attached relations and their distributions."},
		{"reference relation", spqrerror.ReferenceRelationNotFound("sales.countries"), spqrerror.SPQR_OBJECT_NOT_EXIST, `reference relation "sales.countries" not found`, "Run 'SHOW reference_relations' to see all configured reference relations."},
		{"index", spqrerror.UniqueIndexNotFound("idx"), spqrerror.SPQR_OBJECT_NOT_EXIST, `unique index "idx" not found`, "Run 'SHOW unique_indexes' to see all configured unique indexes."},
		{"task", spqrerror.TaskNotFound("move task", "task1"), spqrerror.SPQR_OBJECT_NOT_EXIST, `move task "task1" not found`, ""},
		{"duplicate", spqrerror.ObjectAlreadyExists("shard", "sh1"), spqrerror.SPQR_INVALID_REQUEST, `shard "sh1" already exists`, "Choose a different name or use the existing object."},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.code, tt.err.ErrorCode)
			assert.Equal(t, tt.message, tt.err.Error())
			assert.Equal(t, tt.hint, tt.err.ErrHint)
		})
	}
}
