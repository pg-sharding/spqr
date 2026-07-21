package grpccreds

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		server  *config.GRPCServerTLSConfig
		client  *config.GRPCClientTLSConfig
		wantErr bool
	}{
		{name: "nil configs"},
		{name: "disabled server with files", server: &config.GRPCServerTLSConfig{CertFile: "cert"}, wantErr: true},
		{name: "tls server without key", server: &config.GRPCServerTLSConfig{Mode: config.GRPCTLSTLS, CertFile: "cert"}, wantErr: true},
		{name: "tls server with client CA", server: &config.GRPCServerTLSConfig{Mode: config.GRPCTLSTLS, CertFile: "cert", KeyFile: "key", ClientCAFile: "ca"}, wantErr: true},
		{name: "mtls server without client CA", server: &config.GRPCServerTLSConfig{Mode: config.GRPCTLSMTLS, CertFile: "cert", KeyFile: "key"}, wantErr: true},
		{name: "invalid server mode", server: &config.GRPCServerTLSConfig{Mode: "require"}, wantErr: true},
		{name: "disabled client with root CA", client: &config.GRPCClientTLSConfig{RootCAFile: "ca"}, wantErr: true},
		{name: "tls client with identity", client: &config.GRPCClientTLSConfig{Mode: config.GRPCTLSTLS, CertFile: "cert", KeyFile: "key"}, wantErr: true},
		{name: "mtls client without key", client: &config.GRPCClientTLSConfig{Mode: config.GRPCTLSMTLS, CertFile: "cert"}, wantErr: true},
		{name: "invalid client mode", client: &config.GRPCClientTLSConfig{Mode: "prefer"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			if tt.server != nil {
				err = tt.server.Validate()
			} else if tt.client != nil {
				err = tt.client.Validate()
			}
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCredentialFilesFailDuringPreflight(t *testing.T) {
	err := ValidateServer(&config.GRPCServerTLSConfig{
		Mode: config.GRPCTLSTLS, CertFile: "missing-cert", KeyFile: "missing-key",
	})
	require.ErrorContains(t, err, "load gRPC server certificate")

	err = ValidateClient(&config.GRPCClientTLSConfig{
		Mode: config.GRPCTLSTLS, RootCAFile: "missing-ca",
	})
	require.ErrorContains(t, err, "read gRPC root CA")
}

func TestCLITLSFlagsPreserveInvalidDisabledOptionsForValidation(t *testing.T) {
	flags := CLITLSFlags{Mode: string(config.GRPCTLSDisabled), RootCAFile: "unexpected-ca"}
	cfg := flags.ToClientTLSConfig()
	require.NotNil(t, cfg)
	require.Error(t, cfg.Validate())
}
