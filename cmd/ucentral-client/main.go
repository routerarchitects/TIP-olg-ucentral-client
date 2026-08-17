package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/config"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/nats"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/reqmgr"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/websocket"
)

// connectMetadataProvider maps CapabilityCache and Serial to the websocket interface
type connectMetadataProvider struct {
	cache  *nats.CapabilityCache
	serial string
}

func (m *connectMetadataProvider) ConnectParams(ctx context.Context) (websocket.CloudConnectParams, error) {
	rawCaps, err := m.cache.GetCapabilities()
	if err != nil {
		return websocket.CloudConnectParams{}, err
	}
	fw, err := m.cache.GetFirmware()
	if err != nil {
		return websocket.CloudConnectParams{}, err
	}
	var caps map[string]any
	if err := json.Unmarshal(rawCaps, &caps); err != nil {
		return websocket.CloudConnectParams{}, err
	}
	return websocket.CloudConnectParams{
		Serial:       m.serial,
		UUID:         0,
		Firmware:     fw,
		Capabilities: caps,
	}, nil
}

// frameHandler routes inbound websocket frames to NATS via the RequestManager
type frameHandler struct {
	reqMgr *reqmgr.DefaultRequestManager
}

func (h *frameHandler) HandleFrame(ctx context.Context, frame websocket.InboundFrame) (websocket.FrameDisposition, error) {
	// For production: the main loop frame handler processes incoming frames from the cloud.
	// In the real system, it would call h.reqMgr.HandleRequest(...) to start transactions.
	log.Printf("[FrameHandler] Received frame: Session=%s, Type=%d, Size=%d\n", frame.SessionID, frame.Type, len(frame.Payload))
	return websocket.FrameAccepted, nil
}

func parseTimeoutEnv(envKey string, defaultVal time.Duration) (time.Duration, error) {
	val := os.Getenv(envKey)
	if val == "" {
		return defaultVal, nil
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("failed to parse environment variable %s: %w", envKey, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("environment variable %s must represent a positive duration, got: %s", envKey, val)
	}
	return d, nil
}

func main() {
	configPath := flag.String("config", "config.json", "Path to JSON configuration file")
	flag.Parse()

	// 1. Read JSON configuration
	rawConfig, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("FATAL: Failed to read configuration file: %v", err)
	}

	var cfg config.Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		log.Fatalf("FATAL: Failed to parse configuration file: %v", err)
	}

	// 2. Validate configuration
	if err := cfg.Validate(); err != nil {
		log.Fatalf("FATAL: Invalid configuration: %v", err)
	}

	// 3. Load CacheTTLConfig from environment variables
	cacheTTLConfig, err := config.LoadCacheTTLConfigFromEnv()
	if err != nil {
		log.Fatalf("FATAL: Invalid Cache TTL environment variables: %v", err)
	}

	// 4. Parse timeout environment variables
	dispatchTimeout, err := parseTimeoutEnv("OLG_TIMEOUT_DISPATCH", 5*time.Second)
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	_, err = parseTimeoutEnv("OLG_TIMEOUT_CONFIGURE", 30*time.Second)
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	_, err = parseTimeoutEnv("OLG_TIMEOUT_ACTION_DEFAULT", 60*time.Second)
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	_, err = parseTimeoutEnv("OLG_TIMEOUT_ACTION_EXTENDED", 120*time.Second)
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}

	// 5. Initialize capability cache
	log.Println("Initializing CapabilityCache...")
	capCache := nats.NewCapabilityCache("./capabilities.json")

	// 6. Initialize Outbound Schedulers and Buffers
	log.Println("Initializing Outbound Schedulers...")
	scheduler := queues.NewPriorityScheduler(cfg.Queues.WSWriterCapacity, cfg.Queues.EmergencyCapacity)
	_ = queues.NewTelemetryRingBuffer(cfg.Queues.TelemetryCapacity)

	// 7. Initialize Storage and Cache components
	log.Println("Initializing Operation Store...")
	store, err := reqmgr.NewDiskOperationStore("./operations")
	if err != nil {
		log.Fatalf("FATAL: Failed to initialize operation store: %v", err)
	}

	txCache := reqmgr.NewTransactionCache()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	txCache.StartCacheSweeper(ctx, 1*time.Minute)

	// 8. Convert config.CacheTTLConfig to reqmgr.CacheTTLConfig
	reqmgrTTLConfig := reqmgr.CacheTTLConfig{
		DefaultTTL: time.Duration(cacheTTLConfig.Default) * time.Second,
		MethodTTLs: map[string]time.Duration{
			"configure":     time.Duration(cacheTTLConfig.Configure) * time.Second,
			"leds":          time.Duration(cacheTTLConfig.LEDs) * time.Second,
			"reboot":        time.Duration(cacheTTLConfig.Reboot) * time.Second,
			"remote_access": time.Duration(cacheTTLConfig.RemoteAccess) * time.Second,
			"factory":        time.Duration(cacheTTLConfig.Factory) * time.Second,
			"upgrade":        time.Duration(cacheTTLConfig.Upgrade) * time.Second,
			"certupdate":     time.Duration(cacheTTLConfig.Certupdate) * time.Second,
			"reenroll":       time.Duration(cacheTTLConfig.Reenroll) * time.Second,
			"script":         time.Duration(cacheTTLConfig.Script) * time.Second,
		},
	}

	// 9. Instantiate RequestManager
	// Using conservative values for maxConcurrentRequests, sweeperTTL, activeRecordLimit
	reqManager, err := reqmgr.NewRequestManager(
		dispatchTimeout,
		reqmgrTTLConfig,
		txCache,
		scheduler,
		store,
		100,
		5*time.Minute,
		1000,
	)
	if err != nil {
		log.Fatalf("FATAL: Failed to initialize RequestManager: %v", err)
	}

	// 10. Instantiate NATS and WS clients
	natsStateChange := func(state contracts.LinkState) {
		log.Printf("[NATS STATE] Changed to: %v\n", state)
	}
	log.Println("Initializing NATS client...")
	natsClient, err := nats.NewNATSClient(cfg.Serial, cfg.NATS, natsStateChange)
	if err != nil {
		log.Printf("WARNING: NATS failed to initialize (NATSDegraded mode): %v\n", err)
	}

	wsStateChange := func(state contracts.LinkState) {
		log.Printf("[WS STATE] Changed to: %v\n", state)
	}
	metaProvider := &connectMetadataProvider{cache: capCache, serial: cfg.Serial}
	log.Printf("Initializing WSClient for %s...\n", cfg.Cloud.URL)
	wsClient, err := websocket.NewWSClient(cfg.Cloud, scheduler, metaProvider, wsStateChange)
	if err != nil {
		log.Fatalf("FATAL: Failed to initialize WSClient: %v", err)
	}

	// 11. Launch Reconnection & Reader loops
	handler := &frameHandler{reqMgr: reqManager}
	go func() {
		if err := wsClient.ReconnectLoop(ctx, handler); err != nil {
			log.Printf("WS ReconnectLoop exited: %v\n", err)
		}
	}()

	// 12. Listen for SIGINT / SIGTERM for Graceful Teardown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("Received signal: %v. Initiating graceful teardown...\n", sig)

	// Allow a strict 5-second deadline for teardown
	teardownCtx, teardownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer teardownCancel()
	
	// Perform graceful teardowns
	cancel() // Notify loops to stop
	wsClient.Close()
	if natsClient != nil {
		_ = natsClient.Close(teardownCtx)
	}

	log.Println("Graceful teardown complete. Exiting.")
}
