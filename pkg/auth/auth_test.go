package auth_test

import (
	"bytes"
	"testing"

	"github.com/golang/mock/gomock"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/auth"
	"github.com/pg-sharding/spqr/pkg/conn"
	"github.com/stretchr/testify/assert"

	"github.com/pg-sharding/spqr/pkg/config"
)

const shardName1 = "sh1"
const shardName2 = "sh2"

var buf bytes.Buffer

// MockBakendRule generates a mock BackendRule for testing purposes.
//
// It takes a string parameter `method` which represents the authentication method.
// The function creates two AuthCfg objects, `auth1` and `auth2`, with the given method and different passwords.
// It then creates a map `authRules` with shard names as keys and the corresponding AuthCfg objects as values.
// Finally, it creates a BackendRule object `br` with the provided username, database name, `authRules`, and a connection limit.
// The function returns the generated BackendRule object.
//
// Parameters:
// - method (string): The authentication method.
//
// Returns:
// - *config.BackendRule: The generated BackendRule object.
func MockBakendRule() *config.BackendRule {
	auth1 := &config.AuthBackendCfg{
		Password: "123",
	}
	auth2 := &config.AuthBackendCfg{
		Password: "321",
	}

	authRules := map[string]*config.AuthBackendCfg{shardName1: auth1, shardName2: auth2}

	br := &config.BackendRule{
		Usr:             "vasya",
		DB:              "random",
		AuthRules:       authRules,
		ConnectionLimit: 42,
	}
	return br
}

// MockShard creates a mock DBInstance for testing purposes.
//
// It takes a string parameter `name` which represents the shard name.
// The function creates a new `pgproto3.Frontend` with `nil` backend and a pointer to a `bytes.Buffer` named `buf`.
// It then creates a new `conn.PostgreSQLInstance` and sets the shard name and frontend using the provided `name` and `front` respectively.
// Finally, it returns the created `conn.DBInstance`.
//
// Parameters:
// - name (string): The shard name.
//
// Returns:
// - conn.DBInstance: The mock DBInstance.
func MockShard(name string) conn.DBInstance {
	front := pgproto3.NewFrontend(nil, &buf)

	instance := &conn.PostgreSQLInstance{}
	instance.SetShardName(name)
	instance.SetFrontend(front)
	return instance
}

// TestDifferentPasswordsForDifferentShards tests the behavior of the AuthBackend function when different passwords are provided for different shards.
//
// This function initializes test data by creating mock backend rules and authentication messages for both MD5 and clear text authentication methods.
// It also creates mock shards with specific names.
// The function then calls the AuthBackend function with the shards, backend rules, and authentication messages.
// It asserts that there are no errors returned from the AuthBackend function.
// It also checks that the generated password hashes for the shards are different.
// The function resets the buffer used for storing the generated password hashes.
// The function repeats the process for the clear text authentication method.
// Finally, it resets the buffer used for storing the generated password hashes.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestDifferentPasswordsForDifferentShards(t *testing.T) {
	//init test data
	assert := assert.New(t)

	br_md5 := MockBakendRule()
	message_md5 := &pgproto3.AuthenticationMD5Password{}

	br_clear := MockBakendRule()
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

// TestSamePasswordForOneShard is a test function that verifies the behavior of the AuthBackend function when the same password is used for one shard.
//
// This function initializes test data, including assertions using the "assert" package. It creates mock backend rules and authentication messages for MD5 and clear text authentication methods.
// It also creates a mock shard with the shard name "sh1".
// The function then calls the AuthBackend function with the shard, backend rule, and authentication message for MD5 authentication.
// It checks if the error returned by AuthBackend is nil and compares the generated password hash with the expected value.
// The function then resets the buffer and calls AuthBackend again with the same shard, backend rule, and authentication message for MD5 authentication.
// It checks if the error returned by AuthBackend is nil and compares the generated password hash with the previous value to ensure it matches.
// The function then resets the buffer and calls AuthBackend again with the same shard, backend rule, and authentication message for clear text authentication.
// It checks if the error returned by AuthBackend is nil and compares the generated password hash with the expected value.
// The function then resets the buffer and calls AuthBackend again with the same shard, backend rule, and authentication message for clear text authentication.
// It checks if the error returned by AuthBackend is nil and compares the generated password hash with the previous value to ensure it matches.
// Finally, the function resets the buffer to clean up the test data.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestSamePasswordForOneShard(t *testing.T) {
	//init test data
	assert := assert.New(t)

	br_md5 := MockBakendRule()
	message_md5 := &pgproto3.AuthenticationMD5Password{}

	br_clear := MockBakendRule()
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

// TestErrorWhenNoPasswordForShard tests the error case when trying to authenticate a shard without a password.
//
// This function initializes test data, creates a mock backend rule for MD5 authentication and a mock backend rule for clear text authentication.
// It then creates a mock shard with the name "unexisting".
// The function checks the authentication for both MD5 and clear text authentication methods using the AuthBackend function.
// It asserts that an error is returned for both authentication methods.
// Finally, it resets the buffer used for testing.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestErrorWhenNoPasswordForShard(t *testing.T) {
	//init test data
	assert := assert.New(t)

	br_md5 := MockBakendRule()
	message_md5 := &pgproto3.AuthenticationMD5Password{}

	br_clear := MockBakendRule()
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

// TestCanConnectWithDefaultRule tests the ability to connect with the default authentication rule.
//
// It initializes test data for MD5 and clear text authentication methods, creates backend rules for each method,
// and attempts to connect to a shard with these rules using the AuthBackend function.
// It asserts that the connection is successful for both MD5 and clear text authentication methods.
// Finally, it resets the test data buffer.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestCanConnectWithDefaultRule(t *testing.T) {
	//init test data
	authRule_md5 := &config.AuthBackendCfg{
		Password: "12345",
	}

	br_md5 := &config.BackendRule{
		Usr:             "vasya",
		DB:              "random",
		DefaultAuthRule: authRule_md5,
		ConnectionLimit: 42,
	}
	message_md5 := &pgproto3.AuthenticationMD5Password{}

	authRule_clear := &config.AuthBackendCfg{
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

// TestDifferentPasswordsForRuleAndDefault tests the scenario where different passwords are used for a rule and the default authentication.
//
// This function initializes test data by creating mock backend rules for MD5 and clear text authentication.
// The MD5 rule has a default authentication rule with a password of "12345", and the clear text rule also has a default authentication rule with the same password.
// The function then creates mock shards for testing.
// The function checks the authentication for both MD5 and clear text authentication methods using the AuthBackend function.
// It asserts that there are no errors during authentication for both shards.
// It also asserts that the hashes of the passwords for the two shards are different.
// Finally, it resets the buffer used for testing.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestDifferentPasswordsForRuleAndDefault(t *testing.T) {
	//init test data
	br_md5 := MockBakendRule()
	br_md5.DefaultAuthRule = &config.AuthBackendCfg{
		Password: "12345",
	}
	message_md5 := &pgproto3.AuthenticationMD5Password{}

	br_clear := MockBakendRule()
	br_clear.DefaultAuthRule = &config.AuthBackendCfg{
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

// MockFrontendRule generates a mock FrontendRule for testing purposes.
//
// It takes a string parameter `method` which represents the authentication method.
// The function creates an AuthCfg object `auth1` with the given method and password "123".
// It also takes a pointer to a LDAPCfg object `ldapConfig` which is assigned to the `LDAPConfig` field of `auth1`.
// The function then creates a FrontendRule object `fr` with the username "vasya", the database name "random", and `auth1` as the authentication rule.
// The function returns the generated FrontendRule object.
//
// Parameters:
// - method (string): The authentication method.
// - ldapConfig (*config.LDAPCfg): The LDAP configuration.
//
// Returns:
// - *config.FrontendRule: The generated FrontendRule object.
func MockFrontendRule(method string, ldapConfig *config.LDAPCfg) *config.FrontendRule {
	auth1 := &config.AuthCfg{
		Method:     config.AuthMethod(method),
		Password:   "123",
		LDAPConfig: ldapConfig,
	}

	fr := &config.FrontendRule{
		Usr:      "vasya",
		DB:       "random",
		AuthRule: auth1,
	}
	return fr
}

// TestAuthFrontend is a unit test function that tests the AuthFrontend function in the auth package.
//
// It sets up a mock client, defines the LDAP configuration for testing on an external LDAP server,
// and tests the AuthFrontend function with both search+bind and simple-bind modes.
// The function asserts that there are no errors returned by the AuthFrontend function.
//
// Parameters:
// - t (*testing.T): The testing object used for assertions.
//
// Returns:
// - None.
func TestAuthFrontend(t *testing.T) {
	assert := assert.New(t)
	ctrl := gomock.NewController(t)
	cl := mockcl.NewMockRouterClient(ctrl)
	cl.EXPECT().Usr().AnyTimes().Return("gauss")
	cl.EXPECT().PasswordCT().AnyTimes().Return("password", nil)

	// Testing on external open ldap server
	// Test search+bind mode
	ldapConfig := config.LDAPCfg{
		AuthMode: "search_and_bind",
		ConnConfig: &config.LDAPConnCfg{
			ConnMode: "unencrypted",
			Scheme:   "ldap",
			Port:     "389",
		},
		Servers: []string{"ldap.forumsys.com"},

		BindDN:          "cn=read-only-admin,dc=example,dc=com",
		BindPassword:    "password",
		BaseDN:          "dc=example,dc=com",
		SearchAttribute: "uid",
	}
	authRule := MockFrontendRule("ldap", &ldapConfig)
	err := auth.AuthFrontend(cl, authRule)
	assert.NoError(err)

	// Test simple-bind mode
	ldapConfig = config.LDAPCfg{
		AuthMode: "simple_bind",
		ConnConfig: &config.LDAPConnCfg{
			ConnMode: "unencrypted",
			Scheme:   "ldap",
			Port:     "389",
		},
		Servers: []string{"ldap.forumsys.com"},

		Prefix: "uid=",
		Suffix: ",dc=example,dc=com",
	}
	err = auth.AuthFrontend(cl, authRule)
	assert.NoError(err)
}
