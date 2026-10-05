package internal_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/Muxcore-Media/core/sdk/go/module/meshtls"
	"github.com/Muxcore-Media/media-transcoder-pool/internal"
)

// writeMeshIdentity generates a test CA and a leaf (server+client) cert, writes
// them to a temp dir and points MUXCORE_TLS_* at them.
func writeMeshIdentity(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "module"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(leafKey)
	write := func(name, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	t.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "")
	t.Setenv("MUXCORE_DEV_TLS_SKIP", "")
	t.Setenv("MUXCORE_GRPC_INSECURE", "")
	t.Setenv("MUXCORE_TLS_CA", write("ca.pem", "CERTIFICATE", caDER))
	t.Setenv("MUXCORE_TLS_CERT", write("leaf.pem", "CERTIFICATE", leafDER))
	t.Setenv("MUXCORE_TLS_KEY", write("leaf.key", "EC PRIVATE KEY", keyDER))
}

// probeCall makes a call to a method nobody serves: a completed handshake yields
// Unimplemented, a failed transport yields Unavailable.
func probeCall(t *testing.T, addr string, opt grpc.DialOption) codes.Code {
	t.Helper()
	conn, err := grpc.NewClient(addr, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = conn.Invoke(ctx, "/muxcore.test.Probe/Nope", &emptypb.Empty{}, &emptypb.Empty{})
	return status.Code(err)
}

// assertMeshTLS checks the server at addr speaks mesh TLS and refuses plaintext.
func assertMeshTLS(t *testing.T, addr string) {
	t.Helper()
	opt, err := meshtls.DialOption("localhost")
	if err != nil {
		t.Fatal(err)
	}
	// A completed handshake surfaces as Unimplemented, never Unavailable.
	if c := probeCall(t, addr, opt); c == codes.Unavailable {
		t.Fatalf("TLS client with cert was rejected: %v", c)
	}
	if c := probeCall(t, addr, grpc.WithTransportCredentials(insecure.NewCredentials())); c != codes.Unavailable {
		t.Fatalf("plaintext client got %v, want Unavailable", c)
	}
}

func TestMain(m *testing.M) {
	// Existing tests use plaintext loopback gRPC; the TLS wiring test below
	// clears this flag explicitly.
	_ = os.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "true")
	os.Exit(m.Run())
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := l.Addr().String()
	_ = l.Close()
	return a
}

func TestGRPCServerUsesMeshTLS(t *testing.T) {
	writeMeshIdentity(t)
	grpcAddr := freeAddr(t)
	ctx := context.Background()
	m := internal.NewModule(internal.Config{
		DBPath:          filepath.Join(t.TempDir(), "pool.db"),
		GRPCAddr:        grpcAddr,
		HTTPAddr:        "127.0.0.1:0",
		DisableDispatch: true,
	})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })
	assertMeshTLS(t, grpcAddr)
}

func TestOfflineTranscoderUsesMeshTLS(t *testing.T) {
	writeMeshIdentity(t)
	w, err := internal.StartOfflineTranscoder("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	assertMeshTLS(t, w.Addr())
}
