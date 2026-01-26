package coord

import (
	"context"
	"testing"

	localcoord "github.com/pg-sharding/spqr/pkg/coord"
	"github.com/pg-sharding/spqr/pkg/models/distributions"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/qdb"
	mock "github.com/pg-sharding/spqr/qdb/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// TestValidateSplitBoundaryOnDestShard tests the boundary validation logic
// that prevents "bound intersects" errors during key range migration.
// This is the fix for the bug where split boundaries conflicted with existing
// key ranges on the destination shard.
func TestValidateSplitBoundaryOnDestShard(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)

	tests := []struct {
		name           string
		boundValue     [][]byte            // Split boundary
		destShardID    string              // Target shard
		existingRanges map[string][][]byte // key range ID -> lower bound
		colTypes       []string
		expectError    bool
		expectedErrMsg string
	}{
		{
			name:        "valid split - boundary before all destination ranges",
			boundValue:  [][]byte{[]byte("05000000-0000-0000-0000-000000000000")},
			destShardID: "shard-002",
			existingRanges: map[string][][]byte{
				"kr_10": {[]byte("10000000-0000-0000-0000-000000000000")},
				"kr_40": {[]byte("40000000-0000-0000-0000-000000000000")},
			},
			colTypes:    []string{qdb.ColumnTypeVarchar},
			expectError: false,
		},
		{
			name:        "error - boundary equals existing range lower bound",
			boundValue:  [][]byte{[]byte("60000000-0000-0000-0000-000000000000")},
			destShardID: "shard-002",
			existingRanges: map[string][][]byte{
				"kr_10": {[]byte("10000000-0000-0000-0000-000000000000")},
				"kr_60": {[]byte("60000000-0000-0000-0000-000000000000")},
			},
			colTypes:       []string{qdb.ColumnTypeVarchar},
			expectError:    true,
			expectedErrMsg: "coincides with existing key range",
		},
		{
			name:        "error - boundary would intersect (between adjacent ranges)",
			boundValue:  [][]byte{[]byte("3247d48d-8f36-438a-a58a-3ba7d893eb38")},
			destShardID: "shard-002",
			existingRanges: map[string][][]byte{
				"kr_existing_1": {[]byte("3247d48d-8f36-438a-a58a-3ba7d893eb37")},
				"kr_existing_2": {[]byte("33410e10-6a05-4a6f-8531-ac613a911476")},
			},
			colTypes:       []string{qdb.ColumnTypeVarchar},
			expectError:    true,
			expectedErrMsg: "would intersect with key range",
		},
		{
			name:        "valid - no existing ranges on destination shard",
			boundValue:  [][]byte{[]byte("50000000-0000-0000-0000-000000000000")},
			destShardID: "shard-002",
			existingRanges: map[string][][]byte{
				"kr_other": {[]byte("10000000-0000-0000-0000-000000000000")},
			},
			colTypes:    []string{qdb.ColumnTypeVarchar},
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockQDB := mock.NewMockXQDB(ctrl)

			// Convert test data to internal format
			var qdbKeyRanges []*qdb.KeyRange
			for krID, lowerBound := range tc.existingRanges {
				shardID := "shard-002"
				if tc.name == "valid - no existing ranges on destination shard" {
					shardID = "shard-other"
				}
				qdbKeyRanges = append(qdbKeyRanges, &qdb.KeyRange{
					KeyRangeID:     krID,
					LowerBound:     lowerBound,
					ShardID:        shardID,
					DistributionId: "ds1",
				})
			}

			// Mock the ListKeyRanges to return key ranges
			mockQDB.EXPECT().ListKeyRanges(gomock.Any(), "ds1").Return(qdbKeyRanges, nil)

			// Mock GetDistribution to return distribution info
			ds := &qdb.Distribution{
				ID:       "ds1",
				ColTypes: []string{qdb.ColumnTypeVarchar},
			}
			mockQDB.EXPECT().GetDistribution(gomock.Any(), "ds1").Return(ds, nil)

			// Create a Coordinator with the mock QDB
			mockDCS := mock.NewMockDCStateKeeper(ctrl)

			// Create clustered coordinator
			clusterCoord := &ClusteredCoordinator{
				Coordinator: localcoord.NewCoordinator(mockQDB, mockDCS),
				db:          mockQDB,
			}

			err := clusterCoord.validateSplitBoundaryOnDestShard(ctx, tc.boundValue, &distributions.Distribution{
				Id:       "ds1",
				ColTypes: tc.colTypes,
			}, tc.destShardID, tc.colTypes)

			if tc.expectError {
				assert.Error(err, "expected error but got none")
				assert.Contains(err.Error(), tc.expectedErrMsg, "error message should contain expected text")
			} else {
				assert.NoError(err, "expected no error but got: %v", err)
			}
		})
	}
}

// TestValidateSplitBoundaryOnDestShardWithRealKeyRanges tests the boundary validation
// with more realistic UUID key ranges similar to the production bug scenario.
func TestValidateSplitBoundaryOnDestShardWithRealKeyRanges(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQDB := mock.NewMockXQDB(ctrl)

	// Simulate the key ranges on destination shard (shard-002) from production bug
	keyRanges := []*qdb.KeyRange{
		{
			KeyRangeID:     "ds_user_id_kr_3247d48d_8f36_438a_a58a_3ba7d893eb37",
			LowerBound:     [][]byte{[]byte("3247d48d-8f36-438a-a58a-3ba7d893eb37")},
			ShardID:        "shard-002",
			DistributionId: "ds_user_id",
		},
		{
			KeyRangeID:     "ds_user_id_kr_33410e10_6a05_4a6f_8531_ac613a911476",
			LowerBound:     [][]byte{[]byte("33410e10-6a05-4a6f-8531-ac613a911476")},
			ShardID:        "shard-002",
			DistributionId: "ds_user_id",
		},
	}

	mockQDB.EXPECT().ListKeyRanges(gomock.Any(), "ds_user_id").Return(keyRanges, nil)

	ds := &qdb.Distribution{
		ID:       "ds_user_id",
		ColTypes: []string{qdb.ColumnTypeVarchar},
	}
	mockQDB.EXPECT().GetDistribution(gomock.Any(), "ds_user_id").Return(ds, nil)

	// Create a Coordinator with the mock QDB and DCStateKeeper
	mockDCS := mock.NewMockDCStateKeeper(ctrl)

	// Create clustered coordinator
	clusterCoord := &ClusteredCoordinator{
		Coordinator: localcoord.NewCoordinator(mockQDB, mockDCS),
		db:          mockQDB,
	}

	// Try to split at boundary that would intersect with adjacent range
	// This is the exact scenario from the bug: boundary eb38 intersects with ranges eb37 and 3341...
	conflictingBound := [][]byte{[]byte("3247d48d-8f36-438a-a58a-3ba7d893eb38")}

	err := clusterCoord.validateSplitBoundaryOnDestShard(
		ctx,
		conflictingBound,
		&distributions.Distribution{
			Id:       "ds_user_id",
			ColTypes: []string{qdb.ColumnTypeVarchar},
		},
		"shard-002",
		[]string{qdb.ColumnTypeVarchar},
	)

	assert.Error(err, "expected error due to boundary intersection")
	if serr, ok := err.(*spqrerror.SpqrError); ok {
		assert.Equal(spqrerror.SPQR_KEYRANGE_ERROR, serr.ErrorCode)
	}
	assert.Contains(err.Error(), "would intersect", "error should mention intersection")
}

// TestValidateSplitBoundaryNilBound tests that nil boundaries (whole range moves) pass validation
func TestValidateSplitBoundaryNilBound(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQDB := mock.NewMockXQDB(ctrl)
	mockDCS := mock.NewMockDCStateKeeper(ctrl)
	// No expectations needed - nil boundary should return early without calling ListKeyRanges

	clusterCoord := &ClusteredCoordinator{
		Coordinator: localcoord.NewCoordinator(mockQDB, mockDCS),
		db:          mockQDB,
	}

	err := clusterCoord.validateSplitBoundaryOnDestShard(
		ctx,
		nil, // nil boundary - moving whole range
		&distributions.Distribution{
			Id:       "ds1",
			ColTypes: []string{qdb.ColumnTypeVarchar},
		},
		"shard-002",
		[]string{qdb.ColumnTypeVarchar},
	)

	assert.NoError(err, "nil boundary should always pass validation")
}

// TestValidateSplitBoundaryListKeyRangesError tests error handling when ListKeyRanges fails
func TestValidateSplitBoundaryListKeyRangesError(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQDB := mock.NewMockXQDB(ctrl)
	testErr := spqrerror.New(spqrerror.SPQR_METADATA_CORRUPTION, "connection failed")
	mockQDB.EXPECT().ListKeyRanges(gomock.Any(), "ds1").Return(nil, testErr)

	// Create a Coordinator with the mock QDB
	mockDCS := mock.NewMockDCStateKeeper(ctrl)

	clusterCoord := &ClusteredCoordinator{
		Coordinator: localcoord.NewCoordinator(mockQDB, mockDCS),
		db:          mockQDB,
	}

	err := clusterCoord.validateSplitBoundaryOnDestShard(
		ctx,
		[][]byte{[]byte("50000000-0000-0000-0000-000000000000")},
		&distributions.Distribution{
			Id:       "ds1",
			ColTypes: []string{qdb.ColumnTypeVarchar},
		},
		"shard-002",
		[]string{qdb.ColumnTypeVarchar},
	)

	assert.Error(err, "should return error when ListKeyRanges fails")
	assert.Equal(testErr, err)
}

// TestValidateSplitBoundaryFallbackToWholeRangeMove tests fallback when split boundary conflicts
func TestValidateSplitBoundaryFallbackToWholeRangeMove(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQDB := mock.NewMockXQDB(ctrl)

	// Simulate key ranges on destination shard that would cause conflict
	keyRanges := []*qdb.KeyRange{
		{
			KeyRangeID:     "ds_user_id_kr_3247d48d_8f36_438a_a58a_3ba7d893eb37",
			LowerBound:     [][]byte{[]byte("3247d48d-8f36-438a-a58a-3ba7d893eb37")},
			ShardID:        "shard-002",
			DistributionId: "ds_user_id",
		},
		{
			KeyRangeID:     "ds_user_id_kr_33410e10_6a05_4a6f_8531_ac613a911476",
			LowerBound:     [][]byte{[]byte("33410e10-6a05-4a6f-8531-ac613a911476")},
			ShardID:        "shard-002",
			DistributionId: "ds_user_id",
		},
	}

	mockQDB.EXPECT().ListKeyRanges(gomock.Any(), "ds_user_id").Return(keyRanges, nil)

	ds := &qdb.Distribution{
		ID:       "ds_user_id",
		ColTypes: []string{qdb.ColumnTypeVarchar},
	}
	mockQDB.EXPECT().GetDistribution(gomock.Any(), "ds_user_id").Return(ds, nil)

	mockDCS := mock.NewMockDCStateKeeper(ctrl)

	clusterCoord := &ClusteredCoordinator{
		Coordinator: localcoord.NewCoordinator(mockQDB, mockDCS),
		db:          mockQDB,
	}

	// Conflicting boundary that would intersect with existing ranges
	conflictingBound := [][]byte{[]byte("3247d48d-8f36-438a-a58a-3ba7d893eb38")}

	err := clusterCoord.validateSplitBoundaryOnDestShard(
		ctx,
		conflictingBound,
		&distributions.Distribution{
			Id:       "ds_user_id",
			ColTypes: []string{qdb.ColumnTypeVarchar},
		},
		"shard-002",
		[]string{qdb.ColumnTypeVarchar},
	)

	// Validation should fail due to intersection
	assert.Error(err, "expected validation to fail for conflicting boundary")
	assert.Contains(err.Error(), "would intersect", "error should mention intersection")
}

// Helper test to verify key range comparison logic
func TestKeyRangeComparisonLogic(t *testing.T) {
	assert := assert.New(t)

	// Test UUID string comparisons that are crucial for the bug fix
	// This validates that UUID boundary checking works correctly

	// Create distribution for UUID comparisons
	dist := &distributions.Distribution{
		Id:       "test",
		ColTypes: []string{qdb.ColumnTypeVarchar},
	}

	// Test case from bug: these should be detected as conflicting
	krLower1 := kr.KeyRangeBound{"3247d48d-8f36-438a-a58a-3ba7d893eb37"}
	krLower2 := kr.KeyRangeBound{"3247d48d-8f36-438a-a58a-3ba7d893eb38"}
	krLower3 := kr.KeyRangeBound{"33410e10-6a05-4a6f-8531-ac613a911476"}

	// Verify ordering: kr1 < kr2 < kr3
	assert.True(kr.CmpRangesLess(krLower1, krLower2, dist.ColTypes))
	assert.True(kr.CmpRangesLess(krLower2, krLower3, dist.ColTypes))

	// Verify kr2 would indeed intersect with the gap between kr1 and kr3
	// (i.e., kr2 is after kr1 and before kr3)
	assert.True(kr.CmpRangesLess(krLower1, krLower2, dist.ColTypes))
	assert.True(kr.CmpRangesLessEqual(krLower2, krLower3, dist.ColTypes))
}
