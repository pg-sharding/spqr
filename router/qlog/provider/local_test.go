package qlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDumpQueryFileExists(t *testing.T) {
	assert := assert.New(t)

	tmpFile, err := os.CreateTemp("", "dump-test")
	assert.NoError(err)
	defer func() {
		err = os.Remove(tmpFile.Name())
		assert.NoError(err)
	}()

	testStr := "ogima testing localQLog"
	localQLog := NewLocalQlog()
	err = localQLog.DumpQuery(context.TODO(), tmpFile.Name(), testStr)

	assert.NoError(err)

	data, err := os.ReadFile(tmpFile.Name())
	assert.NoError(err)

	assert.Equal(fmt.Sprintf("%s\n", testStr), string(data))
}

func TestDumpQueryFileNotExist(t *testing.T) {
	assert := assert.New(t)

	testStr := "ogima testing localQLog"
	localQLog := NewLocalQlog()
	err := localQLog.DumpQuery(context.TODO(), "test", testStr)

	defer func() {
		err = os.Remove("test")
		assert.NoError(err)
	}()

	assert.NoError(err)

	data, err := os.ReadFile("test")
	assert.NoError(err)

	assert.Equal(fmt.Sprintf("%s\n", testStr), string(data))
}

func TestDumpQueryReadonlyFile(t *testing.T) {
	assert := assert.New(t)

	tmpFile, err := os.CreateTemp("", "dump-test")
	assert.NoError(err)
	defer func() {
		err = os.Remove(tmpFile.Name())
		assert.NoError(err)
	}()
	err = os.Chmod(tmpFile.Name(), os.FileMode(4))
	assert.NoError(err)

	testStr := "ogima testing localQLog"
	localQLog := NewLocalQlog()
	err = localQLog.DumpQuery(context.TODO(), tmpFile.Name(), testStr)

	assert.Error(err)
	assert.ErrorIs(err, os.ErrPermission)
}

func TestRecoverFileNotFound(t *testing.T) {
	assert := assert.New(t)
	localQLog := NewLocalQlog()

	queries, err := localQLog.Recover(context.Background(), "non_existed_file")

	assert.Error(err)
	assert.Empty(queries)
}

func TestRecover(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected []string
	}{
		{
			name:     "empty file",
			content:  "",
			expected: nil,
		},
		{
			name:     "simple query",
			content:  "SELECT 1;",
			expected: []string{" SELECT 1;"},
		},
		{
			name:     "query without semicolon",
			content:  "SELECT 1",
			expected: []string{" SELECT 1"},
		},
		{
			name:     "multiple queries",
			content:  "SELECT 1;\nSELECT 2;",
			expected: []string{" SELECT 1;", " SELECT 2;"},
		},
		{
			name:     "multiple queries without last semicolon",
			content:  "SELECT 1;\nSELECT 2",
			expected: []string{" SELECT 1;", " SELECT 2"},
		},
		{
			name:     "query with comment",
			content:  "-- comment\nSELECT 1;",
			expected: []string{" SELECT 1;"},
		},
		{
			name: "multiline query",
			content: `CREATE KEY RANGE uid_range_1
FROM 00000000-0000-0000-0000-000000000000
ROUTE TO shard1
FOR DISTRIBUTION uid_ds;`,
			expected: []string{" CREATE KEY RANGE uid_range_1 FROM 00000000-0000-0000-0000-000000000000 ROUTE TO shard1 FOR DISTRIBUTION uid_ds;"},
		},
		{
			name: "multiline query without semicolon",
			content: `CREATE KEY RANGE uid_range_1
FROM 00000000-0000-0000-0000-000000000000
ROUTE TO shard1
FOR DISTRIBUTION uid_ds`,
			expected: []string{" CREATE KEY RANGE uid_range_1 FROM 00000000-0000-0000-0000-000000000000 ROUTE TO shard1 FOR DISTRIBUTION uid_ds"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localQLog := NewLocalQlog()
			tmpFilePath := filepath.Join(t.TempDir(), "recover_test")
			err := os.WriteFile(tmpFilePath, []byte(tt.content), 0644)
			require.NoError(t, err)

			queries, err := localQLog.Recover(context.Background(), tmpFilePath)

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, queries)
		})
	}
}
