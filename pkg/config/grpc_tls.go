package config

import "fmt"

// GRPCTLSMode controls transport security for an SPQR gRPC endpoint.
type GRPCTLSMode string

const (
	GRPCTLSDisabled GRPCTLSMode = "disabled"
	GRPCTLSTLS      GRPCTLSMode = "tls"
	GRPCTLSMTLS     GRPCTLSMode = "mtls"
)

// GRPCServerTLSConfig configures credentials presented by an SPQR gRPC server.
type GRPCServerTLSConfig struct {
	Mode         GRPCTLSMode `json:"mode" yaml:"mode" toml:"mode"`
	CertFile     string      `json:"cert_file" yaml:"cert_file" toml:"cert_file"`
	KeyFile      string      `json:"key_file" yaml:"key_file" toml:"key_file"`
	ClientCAFile string      `json:"client_ca_file" yaml:"client_ca_file" toml:"client_ca_file"`
}

// EffectiveMode returns the configured mode, defaulting to disabled.
func (c *GRPCServerTLSConfig) EffectiveMode() GRPCTLSMode {
	if c == nil || c.Mode == "" {
		return GRPCTLSDisabled
	}
	return c.Mode
}

// Validate checks the structural requirements of a gRPC server TLS config.
func (c *GRPCServerTLSConfig) Validate() error {
	mode := c.EffectiveMode()
	if c == nil {
		return nil
	}

	switch mode {
	case GRPCTLSDisabled:
		if c.CertFile != "" || c.KeyFile != "" || c.ClientCAFile != "" {
			return fmt.Errorf("gRPC server TLS files require mode tls or mtls")
		}
		return nil
	case GRPCTLSTLS:
		if c.CertFile == "" || c.KeyFile == "" {
			return fmt.Errorf("gRPC server TLS mode %q requires both cert_file and key_file", mode)
		}
		if c.ClientCAFile != "" {
			return fmt.Errorf("gRPC server client_ca_file requires mode mtls")
		}
		return nil
	case GRPCTLSMTLS:
		if c.CertFile == "" || c.KeyFile == "" {
			return fmt.Errorf("gRPC server TLS mode %q requires both cert_file and key_file", mode)
		}
		if c.ClientCAFile == "" {
			return fmt.Errorf("gRPC server TLS mode %q requires client_ca_file", mode)
		}
		return nil
	default:
		return fmt.Errorf("invalid gRPC server TLS mode %q; use disabled, tls, or mtls", mode)
	}
}

// GRPCClientTLSConfig configures verification and optional client identity for
// an SPQR gRPC client.
type GRPCClientTLSConfig struct {
	Mode       GRPCTLSMode `json:"mode" yaml:"mode" toml:"mode"`
	RootCAFile string      `json:"root_ca_file" yaml:"root_ca_file" toml:"root_ca_file"`
	CertFile   string      `json:"cert_file" yaml:"cert_file" toml:"cert_file"`
	KeyFile    string      `json:"key_file" yaml:"key_file" toml:"key_file"`
	ServerName string      `json:"server_name" yaml:"server_name" toml:"server_name"`
}

// EffectiveMode returns the configured mode, defaulting to disabled.
func (c *GRPCClientTLSConfig) EffectiveMode() GRPCTLSMode {
	if c == nil || c.Mode == "" {
		return GRPCTLSDisabled
	}
	return c.Mode
}

// Validate checks the structural requirements of a gRPC client TLS config.
func (c *GRPCClientTLSConfig) Validate() error {
	mode := c.EffectiveMode()
	if c == nil {
		return nil
	}

	switch mode {
	case GRPCTLSDisabled:
		if c.RootCAFile != "" || c.CertFile != "" || c.KeyFile != "" || c.ServerName != "" {
			return fmt.Errorf("gRPC client TLS options require mode tls or mtls")
		}
		return nil
	case GRPCTLSTLS:
		if c.CertFile != "" || c.KeyFile != "" {
			return fmt.Errorf("gRPC client cert_file and key_file require mode mtls")
		}
		return nil
	case GRPCTLSMTLS:
		if c.CertFile == "" || c.KeyFile == "" {
			return fmt.Errorf("gRPC client TLS mode %q requires both cert_file and key_file", mode)
		}
		return nil
	default:
		return fmt.Errorf("invalid gRPC client TLS mode %q; use disabled, tls, or mtls", mode)
	}
}
