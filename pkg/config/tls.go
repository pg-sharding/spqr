package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/pg-sharding/spqr/pkg/spqrlog"
	"github.com/pkg/errors"
)

// https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION
type TLSConfig struct {
	SslMode      string `json:"sslmode" toml:"sslmode" yaml:"sslmode"`
	KeyFile      string `json:"key_file" toml:"key_file" yaml:"key_file"`
	CertFile     string `json:"cert_file" toml:"cert_file" yaml:"cert_file"`
	RootCertFile string `json:"root_cert_file" toml:"root_cert_file" yaml:"root_cert_file"`
}

// ConfigTLS creates tls.Init from SPQR config.
// Almost full copy of https://github.com/jackc/pgx/blob/a968ce3437eefc4168b39bbc4b1ea685f4c8ae66/pgconn/config.go#L610.

// Init initializes the TLS configuration based on the provided host and TLSConfig struct.
//
// Parameters:
// - host (string): The host to connect to.
// - c (*TLSConfig): A pointer to the TLSConfig struct.
//
// Returns:
// - (*tls.Config, error): The initialized TLS configuration and an error if any occurred.
func (c *TLSConfig) Init(host string) (*tls.Config, error) {
	// Match libpq default behavior
	if c == nil || c.SslMode == "" {
		c = &TLSConfig{SslMode: "disable"}
	}

	if (c.CertFile != "" && c.KeyFile == "") || (c.CertFile == "" && c.KeyFile != "") {
		return nil, fmt.Errorf(`SPQR: both "cert_file" and "key_file" are required`)
	}

	tlsConfig := &tls.Config{}

	switch c.SslMode {
	case "disable":
		return nil, nil
	case "allow", "prefer":
		// We use InsecureSkipVerify here because https://github.com/jackc/pgx/blob/a968ce3437eefc4168b39bbc4b1ea685f4c8ae66/pgconn/config.go#L633
		// codeql[go/disabled-certificate-verification]
		tlsConfig.InsecureSkipVerify = true
	case "require":
		// According to PostgreSQL documentation, if a root CA file exists,
		// the behavior of sslmode=require should be the same as that of verify-ca
		//
		// See https://www.postgresql.org/docs/12/libpq-ssl.html
		if c.RootCertFile != "" {
			goto nextCase
		}
		// We use InsecureSkipVerify here because https://github.com/jackc/pgx/blob/a968ce3437eefc4168b39bbc4b1ea685f4c8ae66/pgconn/config.go#L642
		// codeql[go/disabled-certificate-verification]
		tlsConfig.InsecureSkipVerify = true
		break
	nextCase:
		fallthrough
	case "verify-ca":
		// Don't perform the default certificate verification because it
		// will verify the hostname. Instead, verify the server's
		// certificate chain ourselves in VerifyPeerCertificate and
		// ignore the server name. This emulates libpq's verify-ca
		// behavior.
		//
		// See https://github.com/golang/go/issues/21971#issuecomment-332693931
		// and https://pkg.go.dev/crypto/tls?tab=doc#example-Config-VerifyPeerCertificate
		// for more info.
		// We use InsecureSkipVerify here because https://github.com/jackc/pgx/blob/a968ce3437eefc4168b39bbc4b1ea685f4c8ae66/pgconn/config.go#L656
		// codeql[go/disabled-certificate-verification]
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.VerifyPeerCertificate = func(certificates [][]byte, _ [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, len(certificates))
			for i, asn1Data := range certificates {
				cert, err := x509.ParseCertificate(asn1Data)
				if err != nil {
					return errors.Wrap(err, "failed to parse certificate from server: ")
				}
				certs[i] = cert
			}

			// Leave DNSName empty to skip hostname verification.
			opts := x509.VerifyOptions{
				Roots:         tlsConfig.RootCAs,
				Intermediates: x509.NewCertPool(),
			}
			// Skip the first cert because it's the leaf. All others
			// are intermediates.
			for _, cert := range certs[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := certs[0].Verify(opts)
			return err
		}
	case "verify-full":
		tlsConfig.ServerName = host
	default:
		return nil, fmt.Errorf("SPQR: sslmode is invalid")
	}

	if c.RootCertFile != "" {
		caCertPool, err := c.validateRootCA()
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = caCertPool
		tlsConfig.ClientCAs = caCertPool
	}

	if c.CertFile != "" && c.KeyFile != "" {
		spqrlog.Zero.Debug().
			Str("cert_file", c.CertFile).
			Str("key_file", c.KeyFile).
			Msg("loading TLS certificates")
		cert, err := c.validateCertificates()
		if err != nil {
			return nil, err
		}
		if cert != nil {
			tlsConfig.Certificates = []tls.Certificate{*cert}
		}
	}

	return tlsConfig, nil
}

// validateCertificates validates certificate and key pair, returns the certificate
func (c *TLSConfig) validateCertificates() (*tls.Certificate, error) {
	if c.CertFile == "" || c.KeyFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("SPQR: unable to load X509 key pair: %w", err)
	}
	return &cert, nil
}

// validateRootCA validates root CA certificate, returns the cert pool
func (c *TLSConfig) validateRootCA() (*x509.CertPool, error) {
	if c.RootCertFile == "" {
		return nil, nil
	}

	caCert, err := os.ReadFile(c.RootCertFile)
	if err != nil {
		return nil, fmt.Errorf("SPQR: unable to read CA file: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("SPQR: unable to add CA to cert pool")
	}

	return caCertPool, nil
}

// ReloadCertificates validates new certificates without breaking existing connections
func (c *TLSConfig) ReloadCertificates() error {
	if c == nil || (c.CertFile == "" && c.KeyFile == "") {
		return nil
	}

	// Validate certificates using shared logic
	if _, err := c.validateCertificates(); err != nil {
		spqrlog.Zero.Error().Err(err).Str("cert_file", c.CertFile).Str("key_file", c.KeyFile).
			Msg("TLS certificate reload failed - keeping existing certificates")
		return fmt.Errorf("SPQR: %w", err)
	}

	// Validate root CA using shared logic
	if _, err := c.validateRootCA(); err != nil {
		spqrlog.Zero.Error().Err(err).Str("root_cert_file", c.RootCertFile).
			Msg("Root CA reload failed - keeping existing CA")
		return fmt.Errorf("SPQR: %w", err)
	}

	spqrlog.Zero.Info().Str("cert_file", c.CertFile).Str("key_file", c.KeyFile).
		Str("root_cert_file", c.RootCertFile).Msg("TLS certificates reloaded successfully")
	return nil
}