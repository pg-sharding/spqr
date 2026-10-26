package server

import (
	"testing"

	mockshard "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func newMockShardWithName(ctrl *gomock.Controller, name string) *mockshard.MockShardHostInstance {
	sh := mockshard.NewMockShardHostInstance(ctrl)
	sh.EXPECT().Name().Return(name).AnyTimes()
	return sh
}

func shardNames(t *testing.T, shards []shard.ShardHostInstance) []string {
	t.Helper()
	names := make([]string, 0, len(shards))
	for _, sh := range shards {
		names = append(names, sh.Name())
	}
	return names
}

func TestMultiShardServer_UnRouteShard(t *testing.T) {
	type tcase struct {
		name     string
		active   []string
		unroute  string
		expected []string
		err      bool
	}

	for _, tt := range []tcase{
		{
			name:     "single shard is removed",
			active:   []string{"sh1"},
			unroute:  "sh1",
			expected: []string{},
		},
		{
			name:     "first shard is removed",
			active:   []string{"sh1", "sh2", "sh3"},
			unroute:  "sh1",
			expected: []string{"sh2", "sh3"},
		},
		{
			name:     "middle shard is removed",
			active:   []string{"sh1", "sh2", "sh3"},
			unroute:  "sh2",
			expected: []string{"sh1", "sh3"},
		},
		{
			name:     "last shard is removed",
			active:   []string{"sh1", "sh2", "sh3"},
			unroute:  "sh3",
			expected: []string{"sh1", "sh2"},
		},
		{
			name:     "unknown shard leaves active shards untouched",
			active:   []string{"sh1", "sh2"},
			unroute:  "sh3",
			expected: []string{"sh1", "sh2"},
			err:      true,
		},
		{
			name:     "no active shards",
			active:   []string{},
			unroute:  "sh1",
			expected: []string{},
			err:      true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			active := make([]shard.ShardHostInstance, 0, len(tt.active))
			for _, name := range tt.active {
				active = append(active, newMockShardWithName(ctrl, name))
			}

			m := &MultiShardServer{activeShards: active}

			unrouted, err := m.UnRouteShard(kr.ShardKey{Name: tt.unroute})

			if tt.err {
				assert.Error(t, err)
				assert.Nil(t, unrouted)
			} else {
				require.NoError(t, err)
				require.NotNil(t, unrouted)
				assert.Equal(t, tt.unroute, unrouted.Name())
			}

			assert.Equal(t, tt.expected, shardNames(t, m.Datashards()))
		})
	}
}

func TestMultiShardServer_DatashardsSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	first := newMockShardWithName(ctrl, "sh1")
	second := newMockShardWithName(ctrl, "sh2")
	m := &MultiShardServer{activeShards: []shard.ShardHostInstance{first, second}}

	snapshot := m.Datashards()
	_, err := m.UnRouteShard(kr.ShardKey{Name: "sh1"})
	require.NoError(t, err)
	require.Len(t, snapshot, 2)
	assert.Same(t, first, snapshot[0])
	assert.Same(t, second, snapshot[1])

	current := m.Datashards()
	require.Len(t, current, 1)
	current[0] = first
	assert.Equal(t, []shard.ShardHostInstance{second}, m.Datashards())
}
