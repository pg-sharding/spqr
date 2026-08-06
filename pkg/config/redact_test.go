package config

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalRedactedJSON(t *testing.T) {
	input := struct {
		Auth    []*AuthCfg                 `json:"auth"`
		Backend map[string]*AuthBackendCfg `json:"backend"`
		Shards  map[string]*ShardConnect   `json:"shards"`
	}{
		Auth: []*AuthCfg{{
			Password: "frontend-secret",
			LDAPConfig: &LDAPCfg{
				BindPassword: "ldap-secret",
			},
		}},
		Backend: map[string]*AuthBackendCfg{
			"user": {Password: "backend-secret"},
		},
		Shards: map[string]*ShardConnect{
			"shard": {Password: "shard-secret"},
		},
	}

	before, err := json.Marshal(input)
	require.NoError(t, err)
	redacted, err := MarshalRedactedJSON(input)
	require.NoError(t, err)

	secrets := []string{"frontend-secret", "ldap-secret", "backend-secret", "shard-secret"}
	for _, secret := range secrets {
		assert.NotContains(t, string(redacted), secret)
	}
	assert.Equal(t, len(secrets), strings.Count(string(redacted), RedactedValue))

	after, err := json.Marshal(input)
	require.NoError(t, err)
	assert.Equal(t, before, after, "MarshalRedactedJSON mutated its input")
}

func TestMarshalRedactedJSONNil(t *testing.T) {
	data, err := MarshalRedactedJSON(nil)
	require.NoError(t, err)
	assert.Equal(t, "null", string(data))
}
