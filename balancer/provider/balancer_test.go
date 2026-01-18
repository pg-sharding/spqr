package provider

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/stretchr/testify/assert"
)

func createTestBalancer() *BalancerImpl {
	return &BalancerImpl{
		threshold:         []float64{100.0, 1000.0, 100.0, 1000.0},
		maxFitCoefficient: 0.8,
		dsToKeyRanges: map[string][]*kr.KeyRange{
			"ds1": {
				{ID: "kr1", ShardID: "sh1"},
				{ID: "kr2", ShardID: "sh2"},
				{ID: "kr3", ShardID: "sh3"},
			},
		},
		dsToKrIdx: map[string]map[string]int{
			"ds1": {
				"kr1": 0,
				"kr2": 1,
				"kr3": 2,
			},
		},
		krToDs: map[string]string{
			"kr1": "ds1",
			"kr2": "ds1",
			"kr3": "ds1",
		},
	}
}

func TestFitsOnShard(t *testing.T) {
	b := createTestBalancer()

	tests := []struct {
		name           string
		krMetrics      []float64
		keyCountToMove int
		krKeyCount     int
		shard          *ShardMetrics
		expected       bool
	}{
		{
			name:           "keys fit on shard - low load",
			krMetrics:      []float64{50.0, 500.0, 50.0, 500.0},
			keyCountToMove: 10,
			krKeyCount:     100,
			shard: &ShardMetrics{
				ShardId:      "sh1",
				MetricsTotal: []float64{20.0, 200.0, 20.0, 200.0},
			},
			expected: true,
		},
		{
			name:           "keys do not fit - would exceed CPU threshold",
			krMetrics:      []float64{100.0, 500.0, 100.0, 500.0},
			keyCountToMove: 50,
			krKeyCount:     100,
			shard: &ShardMetrics{
				ShardId:      "sh2",
				MetricsTotal: []float64{60.0, 200.0, 60.0, 200.0},
			},
			expected: false,
		},
		{
			name:           "keys do not fit - would exceed space threshold",
			krMetrics:      []float64{50.0, 1000.0, 50.0, 1000.0},
			keyCountToMove: 20,
			krKeyCount:     100,
			shard: &ShardMetrics{
				ShardId:      "sh3",
				MetricsTotal: []float64{30.0, 900.0, 30.0, 900.0},
			},
			expected: false,
		},
		{
			name:           "empty shard can accept keys",
			krMetrics:      []float64{80.0, 800.0, 80.0, 800.0},
			keyCountToMove: 10,
			krKeyCount:     100,
			shard: &ShardMetrics{
				ShardId:      "sh4",
				MetricsTotal: []float64{0.0, 0.0, 0.0, 0.0},
			},
			expected: true,
		},
		{
			name:           "keys fit exactly at threshold",
			krMetrics:      []float64{100.0, 1000.0, 100.0, 1000.0},
			keyCountToMove: 10,
			krKeyCount:     100,
			shard: &ShardMetrics{
				ShardId:      "sh5",
				MetricsTotal: []float64{90.0, 900.0, 90.0, 900.0},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := b.fitsOnShard(tt.krMetrics, tt.keyCountToMove, tt.krKeyCount, tt.shard)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMaxFitOnShard(t *testing.T) {
	b := createTestBalancer()
	b.threshold = []float64{100.0, 1000.0, 100.0, 1000.0}

	tests := []struct {
		name         string
		krMetrics    []float64
		krKeyCount   int64
		shard        *ShardMetrics
		expectedMin  int
		expectedMax  int
		checkExact   bool
		expectedKeys int
	}{
		{
			name:       "shard can fit some keys based on CPU",
			krMetrics:  []float64{50.0, 500.0, 50.0, 500.0},
			krKeyCount: 100,
			shard: &ShardMetrics{
				ShardId:      "sh1",
				MetricsTotal: []float64{20.0, 200.0, 20.0, 200.0},
			},

			expectedMin: 128,
			expectedMax: 128,
		},
		{
			name:       "space is limiting factor",
			krMetrics:  []float64{10.0, 900.0, 10.0, 900.0},
			krKeyCount: 100,
			shard: &ShardMetrics{
				ShardId:      "sh2",
				MetricsTotal: []float64{10.0, 100.0, 10.0, 100.0},
			},

			expectedMin: 720,
			expectedMax: 720,
		},
		{
			name:       "shard is full",
			krMetrics:  []float64{100.0, 1000.0, 100.0, 1000.0},
			krKeyCount: 100,
			shard: &ShardMetrics{
				ShardId:      "sh3",
				MetricsTotal: []float64{100.0, 1000.0, 100.0, 1000.0},
			},
			checkExact:   true,
			expectedKeys: 0,
		},
		{
			name:       "empty shard with low per-key metrics",
			krMetrics:  []float64{10.0, 100.0, 10.0, 100.0},
			krKeyCount: 1000,
			shard: &ShardMetrics{
				ShardId:      "sh4",
				MetricsTotal: []float64{0.0, 0.0, 0.0, 0.0},
			},

			expectedMin: 8000,
			expectedMax: 8000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := b.maxFitOnShard(tt.krMetrics, tt.krKeyCount, tt.shard)
			if tt.checkExact {
				assert.Equal(t, tt.expectedKeys, result)
			} else {
				assert.GreaterOrEqual(t, result, tt.expectedMin)
				assert.LessOrEqual(t, result, tt.expectedMax)
			}
		})
	}
}

func TestGetAdjacentShards(t *testing.T) {
	b := createTestBalancer()

	tests := []struct {
		name     string
		krId     string
		expected map[string]struct{}
	}{
		{
			name: "first key range has one adjacent",
			krId: "kr1",
			expected: map[string]struct{}{
				"sh2": {},
			},
		},
		{
			name: "middle key range has two adjacent",
			krId: "kr2",
			expected: map[string]struct{}{
				"sh1": {},
				"sh3": {},
			},
		},
		{
			name: "last key range has one adjacent",
			krId: "kr3",
			expected: map[string]struct{}{
				"sh2": {},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := b.getAdjacentShards(tt.krId)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetShardToMoveTo(t *testing.T) {
	b := createTestBalancer()

	tests := []struct {
		name             string
		shardMetrics     []*ShardMetrics
		shardIdToMetrics map[string]*ShardMetrics
		krId             string
		krShardId        string
		keyCountToMove   int
		expectedShard    string
		expectedOk       bool
	}{
		{
			name: "move to adjacent shard with capacity",
			shardMetrics: []*ShardMetrics{
				{ShardId: "sh3", MetricsTotal: []float64{10.0, 100.0, 10.0, 100.0}},
				{ShardId: "sh2", MetricsTotal: []float64{20.0, 200.0, 20.0, 200.0}},
				{ShardId: "sh1", MetricsTotal: []float64{80.0, 800.0, 80.0, 800.0}},
			},
			shardIdToMetrics: map[string]*ShardMetrics{
				"sh1": {
					ShardId:      "sh1",
					MetricsTotal: []float64{80.0, 800.0, 80.0, 800.0},
					MetricsKR:    map[string][]float64{"kr1": {50.0, 500.0, 50.0, 500.0}},
					KeyCountKR:   map[string]int64{"kr1": 100},
				},
				"sh2": {
					ShardId:      "sh2",
					MetricsTotal: []float64{20.0, 200.0, 20.0, 200.0},
					MetricsKR:    map[string][]float64{},
					KeyCountKR:   map[string]int64{},
				},
				"sh3": {
					ShardId:      "sh3",
					MetricsTotal: []float64{10.0, 100.0, 10.0, 100.0},
					MetricsKR:    map[string][]float64{},
					KeyCountKR:   map[string]int64{},
				},
			},
			krId:           "kr1",
			krShardId:      "sh1",
			keyCountToMove: 10,
			expectedShard:  "sh2",
			expectedOk:     true,
		},
		{
			name: "no shard with enough capacity",
			shardMetrics: []*ShardMetrics{
				{ShardId: "sh2", MetricsTotal: []float64{95.0, 950.0, 95.0, 950.0}},
				{ShardId: "sh3", MetricsTotal: []float64{95.0, 950.0, 95.0, 950.0}},
				{ShardId: "sh1", MetricsTotal: []float64{99.0, 990.0, 99.0, 990.0}},
			},
			shardIdToMetrics: map[string]*ShardMetrics{
				"sh1": {
					ShardId:      "sh1",
					MetricsTotal: []float64{99.0, 990.0, 99.0, 990.0},
					MetricsKR:    map[string][]float64{"kr1": {100.0, 1000.0, 100.0, 1000.0}},
					KeyCountKR:   map[string]int64{"kr1": 100},
				},
				"sh2": {
					ShardId:      "sh2",
					MetricsTotal: []float64{95.0, 950.0, 95.0, 950.0},
					MetricsKR:    map[string][]float64{},
					KeyCountKR:   map[string]int64{},
				},
				"sh3": {
					ShardId:      "sh3",
					MetricsTotal: []float64{95.0, 950.0, 95.0, 950.0},
					MetricsKR:    map[string][]float64{},
					KeyCountKR:   map[string]int64{},
				},
			},
			krId:           "kr1",
			krShardId:      "sh1",
			keyCountToMove: 50,
			expectedShard:  "",
			expectedOk:     false,
		},
		{
			name: "prefer non-adjacent shard with more capacity",
			shardMetrics: []*ShardMetrics{
				{ShardId: "sh3", MetricsTotal: []float64{5.0, 50.0, 5.0, 50.0}},
				{ShardId: "sh2", MetricsTotal: []float64{85.0, 850.0, 85.0, 850.0}},
				{ShardId: "sh1", MetricsTotal: []float64{80.0, 800.0, 80.0, 800.0}},
			},
			shardIdToMetrics: map[string]*ShardMetrics{
				"sh1": {
					ShardId:      "sh1",
					MetricsTotal: []float64{80.0, 800.0, 80.0, 800.0},
					MetricsKR:    map[string][]float64{"kr1": {40.0, 400.0, 40.0, 400.0}},
					KeyCountKR:   map[string]int64{"kr1": 100},
				},
				"sh2": {
					ShardId:      "sh2",
					MetricsTotal: []float64{85.0, 850.0, 85.0, 850.0},
					MetricsKR:    map[string][]float64{},
					KeyCountKR:   map[string]int64{},
				},
				"sh3": {
					ShardId:      "sh3",
					MetricsTotal: []float64{5.0, 50.0, 5.0, 50.0},
					MetricsKR:    map[string][]float64{},
					KeyCountKR:   map[string]int64{},
				},
			},
			krId:           "kr1",
			krShardId:      "sh1",
			keyCountToMove: 15,
			expectedShard:  "sh2",
			expectedOk:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shard, ok := b.getShardToMoveTo(tt.shardMetrics, tt.shardIdToMetrics, tt.krId, tt.krShardId, tt.keyCountToMove)
			assert.Equal(t, tt.expectedOk, ok)
			if tt.expectedOk {
				assert.Equal(t, tt.expectedShard, shard)
			}
		})
	}
}

func TestMoveMaxPossible(t *testing.T) {
	b := createTestBalancer()

	tests := []struct {
		name             string
		shardMetrics     []*ShardMetrics
		shardIdToMetrics map[string]*ShardMetrics
		krId             string
		krShardId        string
		expectedShard    string
		minKeyCount      int
	}{
		{
			name: "find shard with maximum capacity",
			shardMetrics: []*ShardMetrics{
				{ShardId: "sh3", MetricsTotal: []float64{5.0, 50.0, 5.0, 50.0}},
				{ShardId: "sh2", MetricsTotal: []float64{50.0, 500.0, 50.0, 500.0}},
				{ShardId: "sh1", MetricsTotal: []float64{80.0, 800.0, 80.0, 800.0}},
			},
			shardIdToMetrics: map[string]*ShardMetrics{
				"sh1": {
					ShardId:      "sh1",
					MetricsTotal: []float64{80.0, 800.0, 80.0, 800.0},
					MetricsKR:    map[string][]float64{"kr1": {50.0, 500.0, 50.0, 500.0}},
					KeyCountKR:   map[string]int64{"kr1": 100},
				},
				"sh2": {
					ShardId:      "sh2",
					MetricsTotal: []float64{50.0, 500.0, 50.0, 500.0},
					MetricsKR:    map[string][]float64{},
					KeyCountKR:   map[string]int64{},
				},
				"sh3": {
					ShardId:      "sh3",
					MetricsTotal: []float64{5.0, 50.0, 5.0, 50.0},
					MetricsKR:    map[string][]float64{},
					KeyCountKR:   map[string]int64{},
				},
			},
			krId:          "kr1",
			krShardId:     "sh1",
			expectedShard: "sh3",
			minKeyCount:   1,
		},
		{
			name: "all shards full - return 0",
			shardMetrics: []*ShardMetrics{
				{ShardId: "sh2", MetricsTotal: []float64{100.0, 1000.0, 100.0, 1000.0}},
				{ShardId: "sh3", MetricsTotal: []float64{100.0, 1000.0, 100.0, 1000.0}},
				{ShardId: "sh1", MetricsTotal: []float64{100.0, 1000.0, 100.0, 1000.0}},
			},
			shardIdToMetrics: map[string]*ShardMetrics{
				"sh1": {
					ShardId:      "sh1",
					MetricsTotal: []float64{100.0, 1000.0, 100.0, 1000.0},
					MetricsKR:    map[string][]float64{"kr1": {100.0, 1000.0, 100.0, 1000.0}},
					KeyCountKR:   map[string]int64{"kr1": 100},
				},
				"sh2": {
					ShardId:      "sh2",
					MetricsTotal: []float64{100.0, 1000.0, 100.0, 1000.0},
					MetricsKR:    map[string][]float64{},
					KeyCountKR:   map[string]int64{},
				},
				"sh3": {
					ShardId:      "sh3",
					MetricsTotal: []float64{100.0, 1000.0, 100.0, 1000.0},
					MetricsKR:    map[string][]float64{},
					KeyCountKR:   map[string]int64{},
				},
			},
			krId:          "kr1",
			krShardId:     "sh1",
			expectedShard: "",
			minKeyCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shard, keyCount := b.moveMaxPossible(tt.shardMetrics, tt.shardIdToMetrics, tt.krId, tt.krShardId)
			assert.GreaterOrEqual(t, keyCount, tt.minKeyCount)
			if tt.expectedShard != "" {
				assert.Equal(t, tt.expectedShard, shard)
			}
		})
	}
}
