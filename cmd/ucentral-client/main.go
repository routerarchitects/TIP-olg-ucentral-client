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

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/config"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/nats"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/reqmgr"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/websocket"
)

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

	// 1. Read and parse JSON configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("FATAL: %v", err)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize the shared state manager
	stateMgr := &systemStateManager{
		natsLink: contracts.LinkConnecting,
		wsLink:   contracts.LinkConnecting,
	}

	// 4. Initialize all core components
	scheduler, reqManager, natsClient, wsClient, err := initializeComponents(ctx, cfg, cacheTTLConfig, stateMgr)
	if err != nil {
		log.Fatalf("FATAL: Initialization failed: %v", err)
	}

	// Initialize the NATS Dispatch Buffer (REQ-012)
	dispatchBuffer := make(chan struct{}, cfg.Queues.NATSPublishCapacity)

	// 5. Launch Reconnection & Reader loops
	handler := &frameHandler{
		reqMgr:         reqManager,
		stateMgr:       stateMgr,
		scheduler:      scheduler,
		natsClient:     natsClient,
		serial:         cfg.Serial,
		dispatchBuffer: dispatchBuffer,
	}

	// Start the RequestManager background routines (recovery / sweepers)
	reqManager.Start(ctx)

	// Initialize the bounded Command Result Queue (REQ-013)
	resultQueue := make(chan agentcore.ResultEnvelope, cfg.Queues.CommandResultCapacity)

	// Helper to subscribe to NATS results
	subscribeResults := func(nc *nats.NATSClient) {
		err := nc.SubscribeResults(ctx, "vyos", func(res agentcore.ResultEnvelope) {
			select {
			case resultQueue <- res:
			default:
				// Log overflow metric and drop result to protect NATS event loop
				log.Printf("ERROR: command_result_overflow! Dropped result for rpc_id=%s, command=%s (queue capacity %d reached)\n", res.RPCID, res.CommandType, cfg.Queues.CommandResultCapacity)
			}
		})
		if err != nil {
			log.Printf("ERROR: Failed to subscribe to NATS results: %v\n", err)
		}
	}

	// Subscribe if client is ready; otherwise start retry loop in background (resilience against boot outages)
	if natsClient != nil {
		subscribeResults(natsClient)
	} else {
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					log.Println("[NATS] Retrying NATS client initialization...")
					natsStateChange := func(state contracts.LinkState) {
						log.Printf("[NATS STATE] Changed to: %v\n", state)
						stateMgr.UpdateNATSLink(state)
					}
					nc, err := nats.NewNATSClient("vyos", cfg.NATS, natsStateChange)
					if err != nil {
						log.Printf("[NATS] Dynamic NATS initialization failed: %v\n", err)
						continue
					}
					log.Println("[NATS] Dynamic NATS initialization succeeded!")
					handler.SetNATSClient(nc)
					subscribeResults(nc)
					return
				}
			}
		}()
	}

	// Launch background worker to process results from the Command Result Queue
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case res := <-resultQueue:
				log.Printf("[NATS RESULT] Processing result for rpc_id=%s, command=%s, result=%s\n", res.RPCID, res.CommandType, res.Result)

				// Retrieve the transaction from RequestManager using NATS RPCID (UUID)
				tx, exists := reqManager.GetTransaction(res.RPCID)
				if !exists {
					log.Printf("[NATS RESULT] WARNING: Transaction not found for NATS RPCID: %s\n", res.RPCID)
					continue
				}
				sessionID := tx.CloudSessionID
				rawCloudID := tx.CloudRPCID
				isNotification := !tx.RespondToCloud

				// Build standard device result object containing "status", "serial", and "uuid"
				formattedResult := contracts.BuildDeviceResultObject(
					cfg.Serial,
					res.UUID,
					res.Result,
					res.ErrorCode,
					res.Message,
					res.Payload,
				)

				// Complete the transaction in RequestManager
				_ = reqManager.Complete(res.RPCID, formattedResult)

				if !isNotification {
					resp := contracts.JSONRPCResponse{
						JSONRPC: contracts.JSONRPCVersion,
						Result:  formattedResult,
						ID:      rawCloudID,
					}
					respBytes, _ := json.Marshal(resp)
					log.Printf("[NATS RESULT] PUSHING RESPONSE TO CLOUD: %s\n", string(respBytes))
					_ = scheduler.Push(queues.OutboundMessage{
						SessionID: sessionID,
						Priority:  queues.PriorityHighest,
						Payload:   respBytes,
					})
				}
			}
		}
	}()

	go func() {
		if err := wsClient.ReconnectLoop(ctx, handler); err != nil {
			log.Printf("WS ReconnectLoop exited: %v\n", err)
		}
	}()

	// 6. Listen for SIGINT / SIGTERM for Graceful Teardown
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

func initializeComponents(ctx context.Context, cfg *config.Config, cacheTTLConfig config.CacheTTLConfig, stateMgr *systemStateManager) (
	scheduler *queues.PriorityScheduler,
	reqManager *reqmgr.DefaultRequestManager,
	natsClient *nats.NATSClient,
	wsClient *websocket.WSClient,
	err error,
) {
	// Parse timeout environment variables
	dispatchTimeout, err := parseTimeoutEnv("OLG_TIMEOUT_DISPATCH", 5*time.Second)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	_, err = parseTimeoutEnv("OLG_TIMEOUT_CONFIGURE", 30*time.Second)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	_, err = parseTimeoutEnv("OLG_TIMEOUT_ACTION_DEFAULT", 60*time.Second)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	_, err = parseTimeoutEnv("OLG_TIMEOUT_ACTION_EXTENDED", 120*time.Second)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Initialize capability cache
	log.Println("Initializing CapabilityCache...")
	capCache := nats.NewCapabilityCache("./capabilities.json")

	// Initialize Outbound Schedulers and Buffers
	log.Println("Initializing Outbound Schedulers...")
	scheduler = queues.NewPriorityScheduler(cfg.Queues.WSWriterCapacity, cfg.Queues.EmergencyCapacity)
	_ = queues.NewTelemetryRingBuffer(cfg.Queues.TelemetryCapacity)

	// Initialize Storage and Cache components
	log.Println("Initializing Operation Store...")
	store, err := reqmgr.NewDiskOperationStore("./operations")
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to initialize operation store: %w", err)
	}

	txCache := reqmgr.NewTransactionCache()
	txCache.StartCacheSweeper(ctx, 1*time.Minute)

	// Convert config.CacheTTLConfig to reqmgr.CacheTTLConfig
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

	// Instantiate RequestManager
	// Using conservative values for sweeperTTL, activeRecordLimit
	reqManager, err = reqmgr.NewRequestManager(
		dispatchTimeout,
		reqmgrTTLConfig,
		txCache,
		scheduler,
		store,
		cfg.Queues.MaxConcurrentRequests,
		5*time.Minute,
		1000,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to initialize RequestManager: %w", err)
	}

	// Instantiate NATS and WS clients
	natsStateChange := func(state contracts.LinkState) {
		log.Printf("[NATS STATE] Changed to: %v\n", state)
		stateMgr.UpdateNATSLink(state)
	}

	log.Println("Initializing NATS client...")
	natsClient, err = nats.NewNATSClient("vyos", cfg.NATS, natsStateChange)
	if err != nil {
		log.Printf("WARNING: NATS failed to initialize (NATSDegraded mode): %v\n", err)
	}

	wsStateChange := func(state contracts.LinkState) {
		log.Printf("[WS STATE] Changed to: %v\n", state)
		stateMgr.UpdateWSLink(state)
	}
	metaProvider := &connectMetadataProvider{cache: capCache, serial: cfg.Serial}

	log.Printf("Initializing WSClient for %s...\n", cfg.Cloud.URL)
	wsClient, err = websocket.NewWSClient(cfg.Cloud, scheduler, metaProvider, wsStateChange)
	if err != nil {
		if natsClient != nil {
			_ = natsClient.Close(context.Background())
		}
		return nil, nil, nil, nil, fmt.Errorf("failed to initialize WSClient: %w", err)
	}

	return scheduler, reqManager, natsClient, wsClient, nil
}
