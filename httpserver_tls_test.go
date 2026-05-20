package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"
)

// generateSelfSignedCert returns a fresh self-signed ECDSA leaf certificate
// usable for localhost TLS in tests.
func generateSelfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func startTLSServer(t *testing.T, addr string) {
	t.Helper()
	cert := generateSelfSignedCert(t)
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h2", "http/1.1"},
		MinVersion:   tls.VersionTLS12,
	}
	srv := NewHTTPServer().
		HTTP1(defaultHTTP1Handler).
		HTTP2(defaultHTTP2Handler)
	go func() { _ = srv.ListenTLSConfig(addr, cfg) }()

	// Wait for TLS listener by attempting a real TLS dial.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := tls.Dial("tcp", "127.0.0.1"+addr, &tls.Config{InsecureSkipVerify: true})
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("TLS server never started on %s", addr)
}

func TestHTTPServer_H2_TLS(t *testing.T) {
	const addr = ":17985"
	startTLSServer(t, addr)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"h2", "http/1.1"},
		},
		ForceAttemptHTTP2: true,
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	resp, err := client.Get("https://127.0.0.1" + addr + "/tlspath")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.ProtoMajor != 2 {
		t.Fatalf("expected HTTP/2, got %s", resp.Proto)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "/tlspath") {
		t.Fatalf("body missing path: %s", body)
	}
	if !strings.Contains(string(body), "Protocol: HTTP/2") {
		t.Fatalf("body missing protocol marker: %s", body)
	}
}

func TestHTTPServer_H1_TLS_ALPNFallback(t *testing.T) {
	const addr = ":17984"
	startTLSServer(t, addr)

	// Force the client to advertise only http/1.1 so the server selects it via ALPN.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"http/1.1"},
		},
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	resp, err := client.Get("https://127.0.0.1" + addr + "/h1tls")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 1 {
		t.Fatalf("expected HTTP/1.x, got %s", resp.Proto)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "/h1tls") {
		t.Fatalf("body missing path: %s", body)
	}
	if !strings.Contains(string(body), "Protocol: HTTP/1.x") {
		t.Fatalf("body missing protocol marker: %s", body)
	}
}
