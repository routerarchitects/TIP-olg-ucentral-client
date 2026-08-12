package config

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func generateTestCertAndKey(t *testing.T, certPath, keyPath string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("Failed to create certificate: %v", err)
	}

	certOut, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("Failed to open cert file for writing: %v", err)
	}
	defer certOut.Close()
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	keyOut, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("Failed to open key file for writing: %v", err)
	}
	defer keyOut.Close()
	pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
}

func TestConfig_Validation(t *testing.T) {
	tempDir := t.TempDir()

	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")
	caFile := filepath.Join(tempDir, "ca.pem")
	caKeyFile := filepath.Join(tempDir, "cakey.pem")
	credsFile := filepath.Join(tempDir, "user.creds")

	generateTestCertAndKey(t, certFile, keyFile)
	generateTestCertAndKey(t, caFile, caKeyFile)
	os.WriteFile(credsFile, []byte("fake-creds"), 0644)

	t.Run("Valid Config with derived Target", func(t *testing.T) {
		cfg := Config{
			Serial: "derived-router",
			Cloud: CloudConfig{
				URL:                           "wss://example.com",
				ConnectTimeoutSeconds:         1,
				WriteTimeoutSeconds:           1,
				PingIntervalSeconds:           1,
				PongTimeoutSeconds:            1,
				StableSessionThresholdSeconds: 1,
				CompressionThresholdBytes:     1,
				MaxFrameSizeBytes:             1,
				TLS: CloudTLSConfig{
					CAFile:         caFile,
					ClientCertFile: certFile,
					ClientKeyFile:  keyFile,
				},
			},
			NATS: NATSConfig{
				Servers:         []string{"tls://127.0.0.1:4222"},
				CredentialsFile: credsFile,
				CAFile:          caFile,
			},
			Queues: QueueConfig{
				WSWriterCapacity:      1,
				EmergencyCapacity:     1,
				NATSPublishCapacity:   1,
				CommandResultCapacity: 1,
				TelemetryCapacity:     1,
			},
		}

		err := cfg.Validate()
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if cfg.NATS.Target != "derived-router" {
			t.Errorf("expected target to be derived from serial 'derived-router', got %q", cfg.NATS.Target)
		}
	})

	t.Run("Invalid NATS Target with wildcards", func(t *testing.T) {
		cfg := Config{
			Serial: "router1",
			NATS: NATSConfig{
				Target: "router.*",
			},
		}
		err := cfg.Validate()
		if err == nil {
			t.Errorf("expected error for invalid target with wildcard, got nil")
		}
	})
}
