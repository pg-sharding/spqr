package grpccreds

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type testPKI struct {
	caFile          string
	serverCertFile  string
	serverKeyFile   string
	clientCertFile  string
	clientKeyFile   string
	foreignCertFile string
	foreignKeyFile  string
}

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

func TestTLSConfigConstruction(t *testing.T) {
	pki := newTestPKI(t)

	serverConfig, err := serverTLSConfig(&config.GRPCServerTLSConfig{
		Mode:     config.GRPCTLSTLS,
		CertFile: pki.serverCertFile,
		KeyFile:  pki.serverKeyFile,
	})
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS12), serverConfig.MinVersion)
	require.Equal(t, tls.NoClientCert, serverConfig.ClientAuth)

	clientConfig, err := clientTLSConfig(&config.GRPCClientTLSConfig{
		Mode:       config.GRPCTLSTLS,
		RootCAFile: pki.caFile,
		ServerName: "server.test",
	})
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS12), clientConfig.MinVersion)
	require.Equal(t, "server.test", clientConfig.ServerName)
	require.NotNil(t, clientConfig.RootCAs)

	mtlsServerConfig, err := serverTLSConfig(&config.GRPCServerTLSConfig{
		Mode:         config.GRPCTLSMTLS,
		CertFile:     pki.serverCertFile,
		KeyFile:      pki.serverKeyFile,
		ClientCAFile: pki.caFile,
	})
	require.NoError(t, err)
	require.Equal(t, tls.RequireAndVerifyClientCert, mtlsServerConfig.ClientAuth)
	require.NotNil(t, mtlsServerConfig.ClientCAs)

	mtlsClientConfig, err := clientTLSConfig(&config.GRPCClientTLSConfig{
		Mode:       config.GRPCTLSMTLS,
		RootCAFile: pki.caFile,
		CertFile:   pki.clientCertFile,
		KeyFile:    pki.clientKeyFile,
	})
	require.NoError(t, err)
	require.Len(t, mtlsClientConfig.Certificates, 1)
}

func TestGRPCTransportMatrix(t *testing.T) {
	pki := newTestPKI(t)
	tlsServer := &config.GRPCServerTLSConfig{
		Mode:     config.GRPCTLSTLS,
		CertFile: pki.serverCertFile,
		KeyFile:  pki.serverKeyFile,
	}
	tlsClient := &config.GRPCClientTLSConfig{
		Mode:       config.GRPCTLSTLS,
		RootCAFile: pki.caFile,
		ServerName: "server.test",
	}
	mtlsServer := &config.GRPCServerTLSConfig{
		Mode:         config.GRPCTLSMTLS,
		CertFile:     pki.serverCertFile,
		KeyFile:      pki.serverKeyFile,
		ClientCAFile: pki.caFile,
	}
	mtlsClient := &config.GRPCClientTLSConfig{
		Mode:       config.GRPCTLSMTLS,
		RootCAFile: pki.caFile,
		CertFile:   pki.clientCertFile,
		KeyFile:    pki.clientKeyFile,
		ServerName: "server.test",
	}

	tests := []struct {
		name    string
		server  *config.GRPCServerTLSConfig
		client  *config.GRPCClientTLSConfig
		wantErr bool
	}{
		{name: "plaintext compatibility"},
		{name: "verified TLS", server: tlsServer, client: tlsClient},
		{name: "target authority identity", server: tlsServer, client: &config.GRPCClientTLSConfig{
			Mode: config.GRPCTLSTLS, RootCAFile: pki.caFile,
		}},
		{name: "verified mTLS", server: mtlsServer, client: mtlsClient},
		{name: "TLS client to plaintext server", client: tlsClient, wantErr: true},
		{name: "plaintext client to TLS server", server: tlsServer, wantErr: true},
		{name: "wrong server name", server: tlsServer, client: &config.GRPCClientTLSConfig{
			Mode: config.GRPCTLSTLS, RootCAFile: pki.caFile, ServerName: "wrong.test",
		}, wantErr: true},
		{name: "untrusted server CA", server: tlsServer, client: &config.GRPCClientTLSConfig{
			Mode: config.GRPCTLSTLS, RootCAFile: pki.foreignCertFile, ServerName: "server.test",
		}, wantErr: true},
		{name: "mTLS server without client identity", server: mtlsServer, client: tlsClient, wantErr: true},
		{name: "mTLS server with untrusted client", server: mtlsServer, client: &config.GRPCClientTLSConfig{
			Mode: config.GRPCTLSMTLS, RootCAFile: pki.caFile, CertFile: pki.foreignCertFile,
			KeyFile: pki.foreignKeyFile, ServerName: "server.test",
		}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, stop := startHealthServer(t, tt.server)
			defer stop()

			err := checkHealth(addr, tt.client)
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

func startHealthServer(t *testing.T, cfg *config.GRPCServerTLSConfig) (string, func()) {
	t.Helper()
	options, err := ServerOptions(cfg)
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(options...)
	server.RegisterService(&pingServiceDescription, pingService{})
	go func() {
		_ = server.Serve(listener)
	}()

	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}

func checkHealth(addr string, cfg *config.GRPCClientTLSConfig) error {
	dialOption, err := DialOption(cfg)
	if err != nil {
		return err
	}
	connection, err := grpc.NewClient(addr, dialOption)
	if err != nil {
		return err
	}
	defer func() {
		_ = connection.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return connection.Invoke(ctx, "/test.Ping/Ping", &emptypb.Empty{}, &emptypb.Empty{})
}

type pingServiceServer interface {
	Ping(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
}

type pingService struct{}

func (pingService) Ping(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func pingHandler(
	server any,
	ctx context.Context,
	decoder func(any) error,
	interceptor grpc.UnaryServerInterceptor,
) (any, error) {
	request := &emptypb.Empty{}
	if err := decoder(request); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return server.(pingServiceServer).Ping(ctx, request)
	}
	info := &grpc.UnaryServerInfo{Server: server, FullMethod: "/test.Ping/Ping"}
	handler := func(ctx context.Context, request any) (any, error) {
		return server.(pingServiceServer).Ping(ctx, request.(*emptypb.Empty))
	}
	return interceptor(ctx, request, info, handler)
}

var pingServiceDescription = grpc.ServiceDesc{
	ServiceName: "test.Ping",
	HandlerType: (*pingServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Ping", Handler: pingHandler},
	},
}

func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	directory := t.TempDir()
	caCertificate, caKey, caFile := newCA(t, directory, "ca")
	foreignCA, foreignCAKey, _ := newCA(t, directory, "foreign-ca")

	serverCert, serverKey := issueCertificate(t, directory, "server", caCertificate, caKey,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, []string{"server.test"}, []net.IP{net.ParseIP("127.0.0.1")})
	clientCert, clientKey := issueCertificate(t, directory, "client", caCertificate, caKey,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, nil)
	foreignCert, foreignKey := issueCertificate(t, directory, "foreign-client", foreignCA, foreignCAKey,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, nil)

	return testPKI{
		caFile:          caFile,
		serverCertFile:  serverCert,
		serverKeyFile:   serverKey,
		clientCertFile:  clientCert,
		clientKeyFile:   clientKey,
		foreignCertFile: foreignCert,
		foreignKeyFile:  foreignKey,
	}
}

func newCA(t *testing.T, directory string, name string) (*x509.Certificate, *ecdsa.PrivateKey, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          randomSerial(t),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	certPath := filepath.Join(directory, name+".crt")
	writePEM(t, certPath, "CERTIFICATE", der)
	return certificate, key, certPath
}

func issueCertificate(
	t *testing.T,
	directory string,
	name string,
	ca *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	extKeyUsages []x509.ExtKeyUsage,
	dnsNames []string,
	ipAddresses []net.IP,
) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: randomSerial(t),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  extKeyUsages,
		DNSNames:     dnsNames,
		IPAddresses:  ipAddresses,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certPath := filepath.Join(directory, name+".crt")
	keyPath := filepath.Join(directory, name+".key")
	writePEM(t, certPath, "CERTIFICATE", der)
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)
	return certPath, keyPath
}

func randomSerial(t *testing.T) *big.Int {
	t.Helper()
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	require.NoError(t, err)
	return serial
}

func writePEM(t *testing.T, path string, blockType string, der []byte) {
	t.Helper()
	err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0600)
	require.NoError(t, err)
}
