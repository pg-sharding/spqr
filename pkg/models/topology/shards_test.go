package topology_test

import (
	"encoding/json"
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	proto "github.com/pg-sharding/spqr/pkg/protos"
	"github.com/pg-sharding/spqr/qdb"
	"github.com/stretchr/testify/assert"
)

// tlsTestCase describes a single TLS configuration variant for table-driven tests.
type tlsTestCase struct {
	name      string
	configTLS *config.TLSConfig
	qdbTLS    *qdb.TLSConfig
	protoTLS  *proto.TLSConfig
}

// tlsTestCases covers realistic TLS configurations:
//   - full mutual TLS (verify-full)
//   - require with root CA only
//   - allow mode with no certs
//   - disable (explicit)
//   - sslmode-only (no files)
//   - nil (no TLS at all)
var tlsTestCases = []tlsTestCase{
	{
		name: "verify-full with mutual TLS",
		configTLS: &config.TLSConfig{
			SslMode:      "verify-full",
			CertFile:     "/etc/spqr/certs/client.crt",
			KeyFile:      "/etc/spqr/certs/client.key",
			RootCertFile: "/etc/spqr/certs/ca.crt",
		},
		qdbTLS: &qdb.TLSConfig{
			SslMode:      "verify-full",
			CertFile:     "/etc/spqr/certs/client.crt",
			KeyFile:      "/etc/spqr/certs/client.key",
			RootCertFile: "/etc/spqr/certs/ca.crt",
		},
		protoTLS: &proto.TLSConfig{
			Sslmode:      "verify-full",
			CertFile:     "/etc/spqr/certs/client.crt",
			KeyFile:      "/etc/spqr/certs/client.key",
			RootCertFile: "/etc/spqr/certs/ca.crt",
		},
	},
	{
		name: "require with root CA only",
		configTLS: &config.TLSConfig{
			SslMode:      "require",
			RootCertFile: "/etc/spqr/certs/ca.crt",
		},
		qdbTLS: &qdb.TLSConfig{
			SslMode:      "require",
			RootCertFile: "/etc/spqr/certs/ca.crt",
		},
		protoTLS: &proto.TLSConfig{
			Sslmode:      "require",
			RootCertFile: "/etc/spqr/certs/ca.crt",
		},
	},
	{
		name: "allow with no certificates",
		configTLS: &config.TLSConfig{
			SslMode: "allow",
		},
		qdbTLS: &qdb.TLSConfig{
			SslMode: "allow",
		},
		protoTLS: &proto.TLSConfig{
			Sslmode: "allow",
		},
	},
	{
		name: "disable explicit",
		configTLS: &config.TLSConfig{
			SslMode: "disable",
		},
		qdbTLS: &qdb.TLSConfig{
			SslMode: "disable",
		},
		protoTLS: &proto.TLSConfig{
			Sslmode: "disable",
		},
	},
	{
		name: "verify-ca with cert pair",
		configTLS: &config.TLSConfig{
			SslMode:      "verify-ca",
			CertFile:     "/tls/server.crt",
			KeyFile:      "/tls/server.key",
			RootCertFile: "/tls/root.crt",
		},
		qdbTLS: &qdb.TLSConfig{
			SslMode:      "verify-ca",
			CertFile:     "/tls/server.crt",
			KeyFile:      "/tls/server.key",
			RootCertFile: "/tls/root.crt",
		},
		protoTLS: &proto.TLSConfig{
			Sslmode:      "verify-ca",
			CertFile:     "/tls/server.crt",
			KeyFile:      "/tls/server.key",
			RootCertFile: "/tls/root.crt",
		},
	},
	{
		name:      "nil TLS",
		configTLS: nil,
		qdbTLS:    nil,
		protoTLS:  nil,
	},
}

func TestDataShardFromDB_TLSVariants(t *testing.T) {
	for _, tc := range tlsTestCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			dbShard := &qdb.Shard{
				ID:       "sh1",
				RawHosts: []string{"host1:5432"},
				TLS:      tc.qdbTLS,
			}

			ds := topology.DataShardFromDB(dbShard)

			assert.Equal("sh1", ds.ID)
			assert.Equal([]string{"host1:5432"}, ds.Cfg.RawHosts)
			assert.Equal(tc.configTLS, ds.Cfg.TLS)
		})
	}
}

func TestDataShardToDB_TLSVariants(t *testing.T) {
	for _, tc := range tlsTestCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			ds := topology.NewDataShard("sh1", &config.Shard{
				RawHosts: []string{"host1:5432"},
				TLS:      tc.configTLS,
			})

			dbShard := topology.DataShardToDB(ds)

			assert.Equal("sh1", dbShard.ID)
			assert.Equal([]string{"host1:5432"}, dbShard.RawHosts)
			if tc.qdbTLS == nil {
				assert.Nil(dbShard.TLS)
			} else {
				assert.NotNil(dbShard.TLS)
				assert.Equal(tc.qdbTLS.SslMode, dbShard.TLS.SslMode)
				assert.Equal(tc.qdbTLS.CertFile, dbShard.TLS.CertFile)
				assert.Equal(tc.qdbTLS.KeyFile, dbShard.TLS.KeyFile)
				assert.Equal(tc.qdbTLS.RootCertFile, dbShard.TLS.RootCertFile)
			}
		})
	}
}

func TestDataShardToProto_TLSVariants(t *testing.T) {
	for _, tc := range tlsTestCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			ds := topology.NewDataShard("sh1", &config.Shard{
				RawHosts: []string{"host1:5432"},
				TLS:      tc.configTLS,
			})

			ps := topology.DataShardToProto(ds)

			assert.Equal("sh1", ps.Id)
			if tc.protoTLS == nil {
				assert.Nil(ps.Tls)
			} else {
				assert.NotNil(ps.Tls)
				assert.Equal(tc.protoTLS.Sslmode, ps.Tls.Sslmode)
				assert.Equal(tc.protoTLS.CertFile, ps.Tls.CertFile)
				assert.Equal(tc.protoTLS.KeyFile, ps.Tls.KeyFile)
				assert.Equal(tc.protoTLS.RootCertFile, ps.Tls.RootCertFile)
			}
		})
	}
}

func TestDataShardFromProto_TLSVariants(t *testing.T) {
	for _, tc := range tlsTestCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			ps := &proto.Shard{
				Id:    "sh1",
				Hosts: []string{"host1:5432"},
				Tls:   tc.protoTLS,
			}

			ds := topology.DataShardFromProto(ps)

			assert.Equal("sh1", ds.ID)
			assert.Equal([]string{"host1:5432"}, ds.Cfg.RawHosts)
			assert.Equal(tc.configTLS, ds.Cfg.TLS)
		})
	}
}

func TestDataShardDBRoundTrip_TLSVariants(t *testing.T) {
	for _, tc := range tlsTestCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			original := topology.NewDataShard("sh1", &config.Shard{
				RawHosts: []string{"host1:5432", "host2:5432"},
				TLS:      tc.configTLS,
			})

			dbShard := topology.DataShardToDB(original)
			restored := topology.DataShardFromDB(dbShard)

			assert.Equal(original.ID, restored.ID)
			assert.Equal(original.Cfg.RawHosts, restored.Cfg.RawHosts)
			assert.Equal(original.Cfg.TLS, restored.Cfg.TLS)
		})
	}
}

func TestDataShardProtoRoundTrip_TLSVariants(t *testing.T) {
	for _, tc := range tlsTestCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			original := topology.NewDataShard("sh1", &config.Shard{
				RawHosts: []string{"host1:5432"},
				TLS:      tc.configTLS,
			})

			protoShard := topology.DataShardToProto(original)
			restored := topology.DataShardFromProto(protoShard)

			assert.Equal(original.ID, restored.ID)
			assert.Equal(original.Cfg.TLS, restored.Cfg.TLS)
		})
	}
}

// TestQDBShardJSONRoundTrip verifies that qdb.Shard with TLS survives
// JSON marshal/unmarshal — the same path used by etcd and memqdb persistence.
func TestQDBShardJSONRoundTrip(t *testing.T) {
	for _, tc := range tlsTestCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			original := &qdb.Shard{
				ID:       "sh1",
				RawHosts: []string{"host1:5432"},
				TLS:      tc.qdbTLS,
			}

			data, err := json.Marshal(original)
			assert.NoError(err)

			var restored qdb.Shard
			err = json.Unmarshal(data, &restored)
			assert.NoError(err)

			assert.Equal(original.ID, restored.ID)
			assert.Equal(original.RawHosts, restored.RawHosts)
			assert.Equal(original.TLS, restored.TLS)
		})
	}
}

// TestQDBShardJSON_BackwardCompatibility ensures that JSON without a "tls"
// field (data written before this change) deserializes with TLS == nil.
func TestQDBShardJSON_BackwardCompatibility(t *testing.T) {
	assert := assert.New(t)

	oldJSON := `{"id":"sh1","hosts":["host1:5432"]}`

	var shard qdb.Shard
	err := json.Unmarshal([]byte(oldJSON), &shard)
	assert.NoError(err)
	assert.Equal("sh1", shard.ID)
	assert.Equal([]string{"host1:5432"}, shard.RawHosts)
	assert.Nil(shard.TLS)
}

// TestQDBShardJSON_OmitsNilTLS ensures that marshalling a shard without TLS
// does not emit a "tls" key (omitempty behavior).
func TestQDBShardJSON_OmitsNilTLS(t *testing.T) {
	assert := assert.New(t)

	shard := &qdb.Shard{
		ID:       "sh1",
		RawHosts: []string{"host1:5432"},
	}

	data, err := json.Marshal(shard)
	assert.NoError(err)

	var raw map[string]json.RawMessage
	err = json.Unmarshal(data, &raw)
	assert.NoError(err)
	_, hasTLS := raw["tls"]
	assert.False(hasTLS, "JSON should not contain 'tls' key when TLS is nil")
}
