package auth_test

import (
	"bytes"
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/pg-sharding/spqr/pkg/auth"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/spqr/pkg/config"
)

const shardName1 = "sh1"
const shardName2 = "sh2"

var buf bytes.Buffer

func MockBakendRule() *config.BackendRule {
	auth1 := &config.AuthCfg{
		Method:   "md5",
		Password: "123",
	}
	auth2 := &config.AuthCfg{
		Method:   "md5",
		Password: "321",
	}

	authRules := map[string]*config.AuthCfg{shardName1: auth1, shardName2: auth2}

	br := &config.BackendRule{
		Usr:             "vasya",
		DB:              "random",
		AuthRules:       authRules,
		ConnectionLimit: 42,
	}
	return br
}

func MockShard(name string) conn.DBInstance {
	front := pgproto3.NewFrontend(nil, &buf)

	instance := &conn.PostgreSQLInstance{}
	instance.SetShardName(name)
	instance.SetFrontend(front)
	return instance
}

func TestDifferentPasswordsForDifferentShards(t *testing.T) {
	assert := assert.New(t)

	br := MockBakendRule()

	shard1 := MockShard(shardName1)
	shard2 := MockShard(shardName2)

	message := &pgproto3.AuthenticationMD5Password{}

	err := auth.AuthBackend(shard1, br, message)
	assert.NoError(err)

	md1 := buf.String()
	buf.Reset()

	err = auth.AuthBackend(shard2, br, message)

	assert.NoError(err)
	assert.NotEqual(md1, buf.String(), "Passwords` hashes must be different")

	buf.Reset()
}

func TestSamePasswordForOneShard(t *testing.T) {
	assert := assert.New(t)

	br := MockBakendRule()

	shard1 := MockShard(shardName1)

	message := &pgproto3.AuthenticationMD5Password{}

	err := auth.AuthBackend(shard1, br, message)
	assert.NoError(err)

	md1 := buf.String()
	buf.Reset()

	err = auth.AuthBackend(shard1, br, message)

	assert.NoError(err)
	assert.Equal(md1, buf.String(), "Passwords` hashes must match")

	buf.Reset()
}

func TestErrorWhenNoPasswordForShard(t *testing.T) {
	assert := assert.New(t)

	br := MockBakendRule()
	message := &pgproto3.AuthenticationMD5Password{}
	shard := MockShard("unexisting")

	err := auth.AuthBackend(shard, br, message)

	assert.Error(err, "Can`t connect to the shard without password")

	buf.Reset()
}
