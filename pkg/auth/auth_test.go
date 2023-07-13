package auth_test

import (
	"bytes"
	"testing"

	"github.com/pg-sharding/spqr/pkg/auth"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/pg-sharding/spqr/pkg/pgproto3/v2"
	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/spqr/pkg/config"
)

const shardName1 = "sh1"
const shardName2 = "sh2"

var buf bytes.Buffer

func MockBakendRule(method string) *config.BackendRule {
	auth1 := &config.AuthCfg{
		Method:   config.AuthMethod(method),
		Password: "123",
	}
	auth2 := &config.AuthCfg{
		Method:   config.AuthMethod(method),
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
	//init test data
	assert := assert.New(t)

	br_md5 := MockBakendRule("md5")
	message_md5 := &pgproto3.AuthenticationMD5Password{}

	br_clear := MockBakendRule("clear_text")
	message_clear := &pgproto3.AuthenticationCleartextPassword{}

	shard1 := MockShard(shardName1)
	shard2 := MockShard(shardName2)

	//check md5
	err := auth.AuthBackend(shard1, br_md5, message_md5)
	assert.NoError(err)

	md1 := buf.String()
	buf.Reset()

	err = auth.AuthBackend(shard2, br_md5, message_md5)

	assert.NoError(err)
	assert.NotEqual(md1, buf.String(), "Passwords` hashes must be different")
	buf.Reset()

	//check clear text
	err = auth.AuthBackend(shard1, br_clear, message_clear)
	assert.NoError(err)

	md1 = buf.String()
	buf.Reset()

	err = auth.AuthBackend(shard2, br_clear, message_clear)

	assert.NoError(err)
	assert.NotEqual(md1, buf.String(), "Passwords must be different")

	//clean test data
	buf.Reset()
}

func TestSamePasswordForOneShard(t *testing.T) {
	//init test data
	assert := assert.New(t)

	br_md5 := MockBakendRule("md5")
	message_md5 := &pgproto3.AuthenticationMD5Password{}

	br_clear := MockBakendRule("clear_text")
	message_clear := &pgproto3.AuthenticationCleartextPassword{}

	shard1 := MockShard(shardName1)

	//check md5
	err := auth.AuthBackend(shard1, br_md5, message_md5)
	assert.NoError(err)

	md1 := buf.String()
	buf.Reset()

	err = auth.AuthBackend(shard1, br_md5, message_md5)

	assert.NoError(err)
	assert.Equal(md1, buf.String(), "Passwords` hashes must match")
	buf.Reset()

	//check clear text
	err = auth.AuthBackend(shard1, br_clear, message_clear)
	assert.NoError(err)

	md1 = buf.String()
	buf.Reset()

	err = auth.AuthBackend(shard1, br_clear, message_clear)

	assert.NoError(err)
	assert.Equal(md1, buf.String(), "Passwords must match")

	//clean test data
	buf.Reset()
}

func TestErrorWhenNoPasswordForShard(t *testing.T) {
	//init test data
	assert := assert.New(t)

	br_md5 := MockBakendRule("md5")
	message_md5 := &pgproto3.AuthenticationMD5Password{}

	br_clear := MockBakendRule("clear_text")
	message_clear := &pgproto3.AuthenticationCleartextPassword{}

	shard := MockShard("unexisting")

	//check md5
	err := auth.AuthBackend(shard, br_md5, message_md5)
	assert.Error(err, "Can`t connect to the shard without password")

	//check clear text
	err = auth.AuthBackend(shard, br_clear, message_clear)
	assert.Error(err, "Can`t connect to the shard without password")

	//clean test data
	buf.Reset()
}

func TestCanConnectWithDefaultRule(t *testing.T) {
	//init test data
	authRule_md5 := &config.AuthCfg{
		Method:   "md5",
		Password: "12345",
	}

	br_md5 := &config.BackendRule{
		Usr:             "vasya",
		DB:              "random",
		DefaultAuthRule: authRule_md5,
		ConnectionLimit: 42,
	}
	message_md5 := &pgproto3.AuthenticationMD5Password{}

	authRule_clear := &config.AuthCfg{
		Method:   "clear_text",
		Password: "12345",
	}

	br_clear := &config.BackendRule{
		Usr:             "vasya",
		DB:              "random",
		DefaultAuthRule: authRule_clear,
		ConnectionLimit: 42,
	}
	message_clear := &pgproto3.AuthenticationCleartextPassword{}

	assert := assert.New(t)

	shard := MockShard("unexisting")

	//check md5
	err := auth.AuthBackend(shard, br_md5, message_md5)
	assert.NoError(err, "Couldn`t connect to the shard with default rule")

	//check clear text
	err = auth.AuthBackend(shard, br_clear, message_clear)
	assert.NoError(err, "Couldn`t connect to the shard with default rule")

	//clean test data
	buf.Reset()
}

func TestDifferentPasswordsForRuleAndDefault(t *testing.T) {
	//init test data
	br_md5 := MockBakendRule("md5")
	br_md5.DefaultAuthRule = &config.AuthCfg{
		Method:   "md5",
		Password: "12345",
	}
	message_md5 := &pgproto3.AuthenticationMD5Password{}

	br_clear := MockBakendRule("clear_text")
	br_clear.DefaultAuthRule = &config.AuthCfg{
		Method:   "clear_text",
		Password: "12345",
	}
	message_clear := &pgproto3.AuthenticationCleartextPassword{}

	assert := assert.New(t)

	shard1 := MockShard(shardName1)
	shard2 := MockShard("unexisting")

	//check md5
	err := auth.AuthBackend(shard1, br_md5, message_md5)

	assert.NoError(err)
	md1 := buf.String()
	buf.Reset()

	err = auth.AuthBackend(shard2, br_md5, message_md5)

	assert.NoError(err)
	assert.NotEqual(md1, buf.String(), "Passwords` hashes must be different")
	buf.Reset()

	//check clear text
	err = auth.AuthBackend(shard1, br_clear, message_clear)

	assert.NoError(err)
	md1 = buf.String()
	buf.Reset()

	err = auth.AuthBackend(shard2, br_clear, message_clear)

	assert.NoError(err)
	assert.NotEqual(md1, buf.String(), "Passwords must be different")

	//clean test data
	buf.Reset()
}
