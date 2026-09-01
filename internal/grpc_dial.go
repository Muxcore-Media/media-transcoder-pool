package internal

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func meshInsecureAllowed() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("MUXCORE_INSECURE_DISABLE_TLS")))
	if v == "true" || v == "1" {
		return true
	}
	v = strings.ToLower(strings.TrimSpace(os.Getenv("MUXCORE_GRPC_INSECURE")))
	return v == "true" || v == "1"
}

func meshGRPCDialOptions() ([]grpc.DialOption, error) {
	if meshInsecureAllowed() {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}
	certFile := strings.TrimSpace(os.Getenv("MUXCORE_TLS_CERT"))
	keyFile := strings.TrimSpace(os.Getenv("MUXCORE_TLS_KEY"))
	caFile := strings.TrimSpace(os.Getenv("MUXCORE_TLS_CA"))
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("TLS required for gRPC dials — set MUXCORE_TLS_CERT and MUXCORE_TLS_KEY, or MUXCORE_INSECURE_DISABLE_TLS=true for dev")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load client TLS cert/key: %w", err)
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if caFile != "" {
		pemBytes, err := os.ReadFile(caFile) //nolint:gosec // operator-configured CA path
		if err != nil {
			return nil, fmt.Errorf("read TLS CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("parse TLS CA from %q", caFile)
		}
		tlsConfig.RootCAs = pool
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}, nil
}
