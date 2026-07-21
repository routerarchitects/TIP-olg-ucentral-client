package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

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
	if d <= 0 {
		return 0, fmt.Errorf("%s must be > 0, got %v", envKey, d)
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

// TTLForMethod returns the configured TTL in seconds for a specific JSON-RPC method.
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
