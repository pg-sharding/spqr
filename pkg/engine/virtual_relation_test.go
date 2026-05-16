package engine

import (
	"math"
	"testing"
	"time"

	"github.com/pg-sharding/spqr/pkg/models/tasks"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateCoverage(t *testing.T) {
	tests := []struct {
		name     string
		lower    any
		upper    any
		colType  string
		expected string
	}{
		// Integer type tests
		{
			name:     "Integer: small range in middle",
			lower:    int64(30),
			upper:    int64(40),
			colType:  qdb.ColumnTypeInteger,
			expected: "0.00%",
		},
		{
			name:     "Integer: inverse range (lower > upper)",
			lower:    int64(20),
			upper:    int64(10),
			colType:  qdb.ColumnTypeInteger,
			expected: "0.00%",
		},
		{
			name:     "Integer: very large range (half of MaxInt64)",
			lower:    int64(0),
			upper:    int64(9223372036854775807 / 2),
			colType:  qdb.ColumnTypeInteger,
			expected: "25.00%",
		},
		// Unsigned integer tests
		{
			name:     "Uinteger: small range",
			lower:    uint64(1),
			upper:    uint64(11),
			colType:  qdb.ColumnTypeUinteger,
			expected: "0.00%",
		},
		{
			name:     "Uinteger: large range (quarter of MaxUint64)",
			lower:    uint64(0),
			upper:    uint64(18446744073709551615 / 4),
			colType:  qdb.ColumnTypeUinteger,
			expected: "25.00%",
		},
		// Unsupported types - should return N/A
		{
			name:     "Varchar: N/A",
			lower:    "abc",
			upper:    "xyz",
			colType:  qdb.ColumnTypeVarchar,
			expected: "N/A",
		},
		{
			name:     "UUID: almost in half",
			lower:    "00000000-0000-0000-0000-000000000000",
			upper:    "88888888-8888-8888-8888-888888888888",
			colType:  qdb.ColumnTypeUUID,
			expected: "53.33%",
		},
		{
			name:     "UUID: invalid format",
			lower:    "not-a-uuid",
			upper:    "also-not-a-uuid",
			colType:  qdb.ColumnTypeUUID,
			expected: "N/A",
		},
		{
			name:     "Unknown type: N/A",
			lower:    int64(10),
			upper:    int64(20),
			colType:  "unknown_type",
			expected: "N/A",
		},
		{
			name:     "integer - from 0 to MaxInt64",
			lower:    int64(0),
			upper:    int64(math.MaxInt64),
			colType:  qdb.ColumnTypeInteger,
			expected: "50.00%",
		},
		{
			name:     "integer - from MinInt64/2 to MaxInt64/2",
			lower:    int64(math.MinInt64) / 2,
			upper:    int64(math.MaxInt64) / 2,
			colType:  qdb.ColumnTypeInteger,
			expected: "50.00%",
		},
		{
			name:     "uinteger - from 0 to MaxUint64",
			lower:    uint64(0),
			upper:    uint64(math.MaxUint64),
			colType:  qdb.ColumnTypeUinteger,
			expected: "100.00%",
		},
		{
			name:     "uuid - from 00000... to ffffff...",
			lower:    "00000000-0000-0000-0000-000000000000",
			upper:    "ffffffff-ffff-ffff-ffff-ffffffffffff",
			colType:  qdb.ColumnTypeUUID,
			expected: "100.00%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateCoverage(tt.lower, tt.upper, tt.colType)
			if result != tt.expected {
				t.Errorf("calculateCoverage(%v, %v, %q) = %q, expected %q",
					tt.lower, tt.upper, tt.colType, result, tt.expected)
			}
		})
	}
}

func TestRedistributeStatusVirtualRelationScan_EmptyInput(t *testing.T) {
	groups := make(map[string]*tasks.MoveTaskGroup)
	statuses := make(map[string]*tasks.MoveTaskGroupStatus)
	rtByTaskGroup := make(map[string]*tasks.RedistributeTask)

	tts := RedistributeStatusVirtualRelationScan(groups, statuses, rtByTaskGroup)

	require.NotNil(t, tts)
	assert.Equal(t, 0, len(tts.Raw))
	assert.Equal(t, 16, len(tts.Desc))
}

func TestRedistributeStatusVirtualRelationScan_SingleRedistributeTask(t *testing.T) {
	groupID := "tg-1"
	taskID := "rt-1"
	now := time.Now()

	groups := map[string]*tasks.MoveTaskGroup{
		groupID: {
			ID:        groupID,
			KridFrom:  "kr-1",
			KridTo:    "kr-2",
			ShardToID: "shard-2",
			BatchSize: 100,
			TotalKeys: 500,
			CreatedAt: now,
			Issuer: &tasks.MoveTaskGroupIssuer{
				Type: tasks.IssuerRedistributeTask,
				ID:   taskID,
			},
		},
	}

	statuses := map[string]*tasks.MoveTaskGroupStatus{
		groupID: {
			State:           tasks.TaskGroupRunning,
			Message:         "running",
			UpdatedAt:       now,
			Stage:           "MOVE_DATA",
			Detail:          "executing by localhost:5432",
			ProgressPercent: "50.0",
			KeysProcessed:   250,
			BatchPosition:   "2/4",
			MoveTaskState:   "SPLIT",
		},
	}

	rtByTaskGroup := map[string]*tasks.RedistributeTask{
		groupID: {
			ID:          taskID,
			TaskGroupID: groupID,
		},
	}

	tts := RedistributeStatusVirtualRelationScan(groups, statuses, rtByTaskGroup)

	require.NotNil(t, tts)
	require.Equal(t, 1, len(tts.Raw))
	assert.Equal(t, 16, len(tts.Desc))

	row := tts.Raw[0]
	require.Equal(t, 16, len(row))
	assert.Equal(t, groupID, string(row[0]))
	assert.Equal(t, taskID, string(row[1]))
	assert.Equal(t, "kr-1", string(row[2]))
	assert.Equal(t, "shard-2", string(row[3]))
}

func TestRedistributeStatusVirtualRelationScan_IgnoresNonRedistributeTasks(t *testing.T) {
	groupID := "tg-1"

	groups := map[string]*tasks.MoveTaskGroup{
		groupID: {
			ID:        groupID,
			KridFrom:  "kr-1",
			KridTo:    "kr-2",
			ShardToID: "shard-2",
			BatchSize: 100,
			Issuer: &tasks.MoveTaskGroupIssuer{
				Type: tasks.IssuerBalancerTask,
			},
		},
	}

	statuses := make(map[string]*tasks.MoveTaskGroupStatus)
	rtByTaskGroup := make(map[string]*tasks.RedistributeTask)

	tts := RedistributeStatusVirtualRelationScan(groups, statuses, rtByTaskGroup)

	assert.Equal(t, 0, len(tts.Raw))
}

func TestRedistributeStatusVirtualRelationScan_DefaultStatusWhenNotFound(t *testing.T) {
	groupID := "tg-1"
	taskID := "rt-1"

	groups := map[string]*tasks.MoveTaskGroup{
		groupID: {
			ID:        groupID,
			KridFrom:  "kr-1",
			KridTo:    "kr-2",
			ShardToID: "shard-2",
			BatchSize: 100,
			TotalKeys: 500,
			Issuer: &tasks.MoveTaskGroupIssuer{
				Type: tasks.IssuerRedistributeTask,
				ID:   taskID,
			},
		},
	}

	statuses := make(map[string]*tasks.MoveTaskGroupStatus)
	rtByTaskGroup := map[string]*tasks.RedistributeTask{
		groupID: {
			ID: taskID,
		},
	}

	tts := RedistributeStatusVirtualRelationScan(groups, statuses, rtByTaskGroup)

	require.Equal(t, 1, len(tts.Raw))

	row := tts.Raw[0]
	assert.Equal(t, "PLANNED", string(row[6]))
}

func TestRedistributeStatusVirtualRelationScan_ZeroKeysProcessedUsesGroupTotal(t *testing.T) {
	groupID := "tg-1"

	groups := map[string]*tasks.MoveTaskGroup{
		groupID: {
			ID:        groupID,
			KridFrom:  "kr-1",
			KridTo:    "kr-2",
			ShardToID: "shard-2",
			BatchSize: 100,
			TotalKeys: 500,
			Issuer: &tasks.MoveTaskGroupIssuer{
				Type: tasks.IssuerRedistributeTask,
			},
		},
	}

	statuses := map[string]*tasks.MoveTaskGroupStatus{
		groupID: {
			State:         tasks.TaskGroupRunning,
			KeysProcessed: 0,
		},
	}

	rtByTaskGroup := make(map[string]*tasks.RedistributeTask)

	tts := RedistributeStatusVirtualRelationScan(groups, statuses, rtByTaskGroup)

	require.Equal(t, 1, len(tts.Raw))

	row := tts.Raw[0]
	assert.Equal(t, "500", string(row[10]))
}
