package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouterPortDefaults(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "router.yaml")

	err := os.WriteFile(path, []byte("log_level: info\n"), 0o600)
	require.NoError(t, err)

	r := &Router{}
	_, err = LoadConfig(path, r)
	require.NoError(t, err)

	assert.Equal(t, DefaultRouterPort, r.RouterPort)
	assert.Equal(t, DefaultAdminConsolePort, r.AdminConsolePort)
	assert.Equal(t, DefaultRouterGrpcAPIPort, r.GrpcAPIPort)
	assert.Empty(t, r.RouterROPort)
}

func TestRouterPortDefaultsOverriddenByConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "router.yaml")

	cfg := "" +
		"log_level: info\n" +
		"router_port: \"15432\"\n" +
		"admin_console_port: \"17432\"\n" +
		"grpc_api_port: \"17000\"\n"
	err := os.WriteFile(path, []byte(cfg), 0o600)
	require.NoError(t, err)

	r := &Router{}
	_, err = LoadConfig(path, r)
	require.NoError(t, err)

	assert.Equal(t, "15432", r.RouterPort)
	assert.Equal(t, "17432", r.AdminConsolePort)
	assert.Equal(t, "17000", r.GrpcAPIPort)
}

func TestRouterGrpcListenHost(t *testing.T) {
	t.Parallel()

	assert.Equal(t, DefaultRouterGrpcAPIHost, (&Router{}).GrpcListenHost())
	assert.Equal(t, "10.0.0.5", (&Router{Host: "10.0.0.5"}).GrpcListenHost())
	assert.Equal(t, "0.0.0.0", (&Router{GrpcAPIHost: "0.0.0.0"}).GrpcListenHost())
	assert.Equal(t, "0.0.0.0", (&Router{Host: "10.0.0.5", GrpcAPIHost: "0.0.0.0"}).GrpcListenHost())
}

func TestRouterGrpcListenHostDefaultWhenHostEmpty(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "router.yaml")

	err := os.WriteFile(path, []byte("log_level: info\n"+"host: \"\"\n"), 0o600)
	require.NoError(t, err)

	r := &Router{}
	_, err = LoadConfig(path, r)
	require.NoError(t, err)

	assert.Empty(t, r.Host)
	assert.Equal(t, DefaultRouterPort, r.RouterPort)
	assert.Equal(t, DefaultRouterGrpcAPIPort, r.GrpcAPIPort)
	assert.Equal(t, DefaultRouterGrpcAPIHost, r.GrpcListenHost())
}
