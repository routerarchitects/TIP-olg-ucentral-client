package config

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

func checkFile(path, name string) error {
	if path == "" {
		return fmt.Errorf("%s is required", name)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s is not accessible: %w", name, err)
	}
	if info.IsDir() {
		return fmt.Errorf("%s is a directory", name)
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%s cannot be opened: %w", name, err)
	}
	defer f.Close()
	return nil
}

type CloudTLSConfig struct {
	CAFile         string `json:"ca_file"`
	ClientCertFile string `json:"client_cert_file"`
	ClientKeyFile  string `json:"client_key_file"`
	ServerName     string `json:"server_name,omitempty"`
}

type CloudConfig struct {
	URL                           string         `json:"url"`
	ConnectTimeoutSeconds         int            `json:"connect_timeout_seconds"`
	WriteTimeoutSeconds           int            `json:"write_timeout_seconds"`
	PingIntervalSeconds           int            `json:"ping_interval_seconds"`
	PongTimeoutSeconds            int            `json:"pong_timeout_seconds"`
	StableSessionThresholdSeconds int            `json:"stable_session_threshold_seconds"`
	CompressionThresholdBytes     int            `json:"compression_threshold_bytes"` // Defines compression threshold mapped to permessage-deflate behavior
	TLS                           CloudTLSConfig `json:"tls"`
}

type NATSConfig struct {
	Servers         []string `json:"servers"`
	CredentialsFile string   `json:"credentials_file"`
	CAFile          string   `json:"ca_file"`
}

type QueueConfig struct {
	WSWriterCapacity      int `json:"ws_writer_capacity"`
	EmergencyCapacity     int `json:"emergency_capacity"`
	NATSPublishCapacity   int `json:"nats_publish_capacity"`
	CommandResultCapacity int `json:"command_result_capacity"`
	TelemetryCapacity     int `json:"telemetry_capacity"`
}

type Config struct {
	Serial string      `json:"serial"`
	Cloud  CloudConfig `json:"cloud"`
	NATS   NATSConfig  `json:"nats"`
	Queues QueueConfig `json:"queues"`
}

func (c *CloudTLSConfig) Validate() error {
	if err := checkFile(c.CAFile, "tls ca_file"); err != nil {
		return err
	}
	if err := checkFile(c.ClientCertFile, "tls client_cert_file"); err != nil {
		return err
	}
	if err := checkFile(c.ClientKeyFile, "tls client_key_file"); err != nil {
		return err
	}

	// Deep validation of TLS configuration
	if _, err := tls.LoadX509KeyPair(c.ClientCertFile, c.ClientKeyFile); err != nil {
		return fmt.Errorf("invalid tls client certificate or key: %w", err)
	}

	caCert, err := os.ReadFile(c.CAFile)
	if err != nil {
		return fmt.Errorf("failed to read tls ca_file: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return fmt.Errorf("failed to parse tls ca_file as a valid PEM CA bundle")
	}

	return nil
}

func (c *CloudConfig) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("cloud url is required")
	}
	u, err := url.ParseRequestURI(c.URL)
	if err != nil || u.Scheme != "wss" || u.Host == "" {
		return fmt.Errorf("cloud url must be a valid wss URL")
	}
	if c.ConnectTimeoutSeconds <= 0 {
		return fmt.Errorf("cloud connect_timeout_seconds must be positive")
	}
	if c.WriteTimeoutSeconds <= 0 {
		return fmt.Errorf("cloud write_timeout_seconds must be positive")
	}
	if c.PingIntervalSeconds <= 0 {
		return fmt.Errorf("cloud ping_interval_seconds must be positive")
	}
	if c.PongTimeoutSeconds <= 0 {
		return fmt.Errorf("cloud pong_timeout_seconds must be positive")
	}
	if c.StableSessionThresholdSeconds <= 0 {
		return fmt.Errorf("cloud stable_session_threshold_seconds must be positive")
	}
	if c.CompressionThresholdBytes < 0 {
		return fmt.Errorf("cloud compression_threshold_bytes must be zero or positive")
	}
	if err := c.TLS.Validate(); err != nil {
		return err
	}
	return nil
}

func (n *NATSConfig) Validate() error {
	if len(n.Servers) == 0 {
		return fmt.Errorf("nats servers are required")
	}
	for _, srv := range n.Servers {
		u, err := url.ParseRequestURI(srv)
		if err != nil || u.Scheme != "tls" || u.Host == "" {
			return fmt.Errorf("nats server must be a valid tls URL")
		}
	}
	if err := checkFile(n.CredentialsFile, "nats credentials_file"); err != nil {
		return err
	}
	if err := checkFile(n.CAFile, "nats ca_file"); err != nil {
		return err
	}
	
	natsCaCert, err := os.ReadFile(n.CAFile)
	if err != nil {
		return fmt.Errorf("failed to read nats ca_file: %w", err)
	}
	natsCaCertPool := x509.NewCertPool()
	if ok := natsCaCertPool.AppendCertsFromPEM(natsCaCert); !ok {
		return fmt.Errorf("failed to parse nats ca_file as a valid PEM CA bundle")
	}
	
	natsCreds, err := os.ReadFile(n.CredentialsFile)
	if err != nil {
		return fmt.Errorf("failed to read nats credentials_file: %w", err)
	}
	if !bytes.Contains(natsCreds, []byte("-----BEGIN NATS USER JWT-----")) {
		return fmt.Errorf("nats credentials_file does not contain a valid NATS USER JWT structure")
	}
	
	return nil
}

func (q *QueueConfig) Validate() error {
	if q.WSWriterCapacity <= 0 {
		return fmt.Errorf("ws_writer_capacity must be positive")
	}
	if q.EmergencyCapacity <= 0 {
		return fmt.Errorf("emergency_capacity must be positive")
	}
	if q.NATSPublishCapacity <= 0 {
		return fmt.Errorf("nats_publish_capacity must be positive")
	}
	if q.CommandResultCapacity <= 0 {
		return fmt.Errorf("command_result_capacity must be positive")
	}
	if q.TelemetryCapacity <= 0 {
		return fmt.Errorf("telemetry_capacity must be positive")
	}
	return nil
}

func (c *Config) Validate() error {
	if c.Serial == "" {
		return fmt.Errorf("serial is required")
	}
	if err := c.Cloud.Validate(); err != nil {
		return err
	}
	if err := c.NATS.Validate(); err != nil {
		return err
	}
	if err := c.Queues.Validate(); err != nil {
		return err
	}
	return nil
}

type CacheTTLConfig struct {
	Configure    int
	LEDs         int
	Reboot       int
	RemoteAccess int
	Factory      int
	Upgrade      int
	Certupdate   int
	Reenroll     int
	Script       int
	Default      int
}

func parseEnvDuration(envKey string, defaultDuration time.Duration) (int, error) {
	val := os.Getenv(envKey)
	if val == "" {
		return int(defaultDuration.Seconds()), nil
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %s: %w", envKey, err)
	}
	if d < time.Second {
		return 0, fmt.Errorf("%s must be at least 1s, got %v", envKey, d)
	}
	return int(d.Seconds()), nil
}

// LoadCacheTTLConfigFromEnv parses the OLG_CACHE_TTL_* environment variables as Go durations,
// applies the documented defaults if unset, and rejects malformed or negative durations.
func LoadCacheTTLConfigFromEnv() (CacheTTLConfig, error) {
	cfg := CacheTTLConfig{}
	var err error

	if cfg.Configure, err = parseEnvDuration("OLG_CACHE_TTL_CONFIGURE", 5*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.LEDs, err = parseEnvDuration("OLG_CACHE_TTL_LEDS", 5*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.Reboot, err = parseEnvDuration("OLG_CACHE_TTL_REBOOT", 10*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.RemoteAccess, err = parseEnvDuration("OLG_CACHE_TTL_REMOTE_ACCESS", 10*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.Factory, err = parseEnvDuration("OLG_CACHE_TTL_FACTORY", 30*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.Upgrade, err = parseEnvDuration("OLG_CACHE_TTL_UPGRADE", 60*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.Certupdate, err = parseEnvDuration("OLG_CACHE_TTL_CERTUPDATE", 30*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.Reenroll, err = parseEnvDuration("OLG_CACHE_TTL_REENROLL", 30*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.Script, err = parseEnvDuration("OLG_CACHE_TTL_SCRIPT", 30*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.Default, err = parseEnvDuration("OLG_CACHE_TTL_DEFAULT", 2*time.Minute); err != nil {
		return cfg, err
	}

	return cfg, nil
}

// TTLForMethod returns the configured TTL for a specific JSON-RPC method in seconds.
func (c CacheTTLConfig) TTLForMethod(method string) int {
	switch strings.ToLower(method) {
	case "configure":
		return c.Configure
	case "leds":
		return c.LEDs
	case "reboot":
		return c.Reboot
	case "remoteaccess", "remote_access":
		return c.RemoteAccess
	case "factory":
		return c.Factory
	case "upgrade":
		return c.Upgrade
	case "certupdate":
		return c.Certupdate
	case "reenroll":
		return c.Reenroll
	case "script":
		return c.Script
	default:
		return c.Default
	}
}
