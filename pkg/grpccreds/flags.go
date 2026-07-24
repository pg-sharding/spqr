package grpccreds

import (
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/spf13/cobra"
)

// CLITLSFlags holds TLS-related command-line flags for CLI tools
// that connect to SPQR components via gRPC.
type CLITLSFlags struct {
	Mode       string
	RootCAFile string
	CertFile   string
	KeyFile    string
	ServerName string
}

// RegisterFlags adds gRPC TLS flags to a cobra command.
func (f *CLITLSFlags) RegisterFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&f.Mode, "grpc-tls-mode", string(config.GRPCTLSDisabled), "gRPC TLS mode (disabled, tls, mtls)")
	cmd.PersistentFlags().StringVar(&f.RootCAFile, "grpc-root-ca-file", "", "path to the CA used to verify the gRPC server")
	cmd.PersistentFlags().StringVar(&f.CertFile, "grpc-cert-file", "", "path to the gRPC client certificate (mtls mode)")
	cmd.PersistentFlags().StringVar(&f.KeyFile, "grpc-key-file", "", "path to the gRPC client private key (mtls mode)")
	cmd.PersistentFlags().StringVar(&f.ServerName, "grpc-server-name", "", "certificate server name override; defaults to the target authority")
}

// ToClientTLSConfig converts the CLI flags into a client TLS config.
// Returns nil if TLS is disabled.
func (f *CLITLSFlags) ToClientTLSConfig() *config.GRPCClientTLSConfig {
	if (f.Mode == "" || f.Mode == string(config.GRPCTLSDisabled)) &&
		f.RootCAFile == "" && f.CertFile == "" && f.KeyFile == "" && f.ServerName == "" {
		return nil
	}
	return &config.GRPCClientTLSConfig{
		Mode:       config.GRPCTLSMode(f.Mode),
		RootCAFile: f.RootCAFile,
		CertFile:   f.CertFile,
		KeyFile:    f.KeyFile,
		ServerName: f.ServerName,
	}
}
