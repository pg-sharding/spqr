package grpccreds

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/pg-sharding/spqr/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ServerOptions returns the gRPC options needed by a server endpoint. An empty
// slice means TLS is disabled.
func ServerOptions(cfg *config.GRPCServerTLSConfig) ([]grpc.ServerOption, error) {
	tlsConfig, err := serverTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	if tlsConfig == nil {
		return nil, nil
	}
	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsConfig))}, nil
}

// DialOption returns transport credentials for a gRPC client. When ServerName
// is empty, grpc-go derives the verified name from the target authority.
func DialOption(cfg *config.GRPCClientTLSConfig) (grpc.DialOption, error) {
	tlsConfig, err := clientTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	if tlsConfig == nil {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), nil
}

// ValidateServer loads and validates all credentials needed by a gRPC server.
func ValidateServer(cfg *config.GRPCServerTLSConfig) error {
	_, err := serverTLSConfig(cfg)
	return err
}

// ValidateClient loads and validates all credentials needed by a gRPC client.
func ValidateClient(cfg *config.GRPCClientTLSConfig) error {
	_, err := clientTLSConfig(cfg)
	return err
}

func serverTLSConfig(cfg *config.GRPCServerTLSConfig) (*tls.Config, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.EffectiveMode() == config.GRPCTLSDisabled {
		return nil, nil
	}

	certificate, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load gRPC server certificate %q and key %q: %w", cfg.CertFile, cfg.KeyFile, err)
	}

	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.NoClientCert,
	}

	if cfg.EffectiveMode() == config.GRPCTLSMTLS {
		clientCAs, err := loadCertPool(cfg.ClientCAFile, "gRPC client CA")
		if err != nil {
			return nil, err
		}
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		tlsConfig.ClientCAs = clientCAs
	}

	return tlsConfig, nil
}

func clientTLSConfig(cfg *config.GRPCClientTLSConfig) (*tls.Config, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.EffectiveMode() == config.GRPCTLSDisabled {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: cfg.ServerName,
	}

	if cfg.RootCAFile != "" {
		rootCAs, err := loadCertPool(cfg.RootCAFile, "gRPC root CA")
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = rootCAs
	}

	if cfg.EffectiveMode() == config.GRPCTLSMTLS {
		certificate, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load gRPC client certificate %q and key %q: %w", cfg.CertFile, cfg.KeyFile, err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	return tlsConfig, nil
}

func loadCertPool(path string, description string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s file %q: %w", description, path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("parse %s file %q: no certificates found", description, path)
	}
	return pool, nil
}
