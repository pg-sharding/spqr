package qlog

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
