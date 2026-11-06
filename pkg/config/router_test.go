package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouterGrpcListenHost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, config, wantHost, wantGrpcAPIHost, wantGrpcListenHost string
	}{
		{"empty config preserves all-interface bind", "log_level: info\n", "", "", ""},
		{"explicit empty host preserves all-interface bind", "log_level: info\n" + "host: \"\"\n", "", "", ""},
		{"grpc API inherits host", "log_level: info\n" + "host: \"10.0.0.5\"\n", "10.0.0.5", "", "10.0.0.5"},
		{"dedicated grpc API host overrides empty host", "log_level: info\n" + "grpc_api_host: \"0.0.0.0\"\n", "", "0.0.0.0", "0.0.0.0"},
		{"dedicated grpc API host takes precedence", "log_level: info\n" + "host: \"10.0.0.5\"\n" + "grpc_api_host: \"0.0.0.0\"\n", "10.0.0.5", "0.0.0.0", "0.0.0.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "router.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tt.config), 0o600))
			r := &Router{}
			_, err := LoadConfig(path, r)
			require.NoError(t, err)
			assert.Equal(t, tt.wantHost, r.Host)
			assert.Equal(t, tt.wantGrpcAPIHost, r.GrpcAPIHost)
			assert.Equal(t, tt.wantGrpcListenHost, r.GrpcListenHost())
			assert.Empty(t, r.RouterPort)
			assert.Empty(t, r.AdminConsolePort)
			assert.Empty(t, r.GrpcAPIPort)
			assert.Empty(t, r.RouterROPort)
		})
	}
}
