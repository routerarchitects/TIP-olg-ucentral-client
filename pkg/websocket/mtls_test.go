package websocket

import (
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
)

func generateMTLSCerts(t *testing.T) (caFile, serverCertFile, serverKeyFile, clientCertFile, clientKeyFile string) {
	tempDir := t.TempDir()

	// CA
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDER)

	caFile = filepath.Join(tempDir, "ca.crt")
	caOut, _ := os.Create(caFile)
	pem.Encode(caOut, &pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caOut.Close()

	// Server
	serverKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	serverDER, _ := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)

	serverCertFile = filepath.Join(tempDir, "server.crt")
	scOut, _ := os.Create(serverCertFile)
	pem.Encode(scOut, &pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	scOut.Close()

	serverKeyFile = filepath.Join(tempDir, "server.key")
	skOut, _ := os.Create(serverKeyFile)
	skBytes, _ := x509.MarshalECPrivateKey(serverKey)
	pem.Encode(skOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: skBytes})
	skOut.Close()

	// Client
	clientKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "Test Client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	clientDER, _ := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)

	clientCertFile = filepath.Join(tempDir, "client.crt")
	ccOut, _ := os.Create(clientCertFile)
	pem.Encode(ccOut, &pem.Block{Type: "CERTIFICATE", Bytes: clientDER})
	ccOut.Close()

	clientKeyFile = filepath.Join(tempDir, "client.key")
	ckOut, _ := os.Create(clientKeyFile)
	ckBytes, _ := x509.MarshalECPrivateKey(clientKey)
	pem.Encode(ckOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: ckBytes})
	ckOut.Close()

	return
}
