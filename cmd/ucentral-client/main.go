package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
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
	// Load .env file if it exists
	_ = godotenv.Load()

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
	components, err := initializeComponents(ctx, cfg, cacheTTLConfig, stateMgr)
	if err != nil {
		log.Fatalf("FATAL: Initialization failed: %v", err)
	}

	// Initialize the NATS Dispatch Buffer (REQ-012)
	dispatchBuffer := make(chan struct{}, cfg.Queues.NATSPublishCapacity)

	// 5. Launch Reconnection & Reader loops
	handler := &frameHandler{
		reqMgr:                 components.ReqManager,
		stateMgr:               stateMgr,
		scheduler:              components.Scheduler,
		natsClient:             components.NatsClient,
		serial:                 cfg.Serial,
		dispatchBuffer:         dispatchBuffer,
		timeoutDispatch:        components.TimeoutDispatch,
		timeoutConfigure:       components.TimeoutConfigure,
		timeoutActionDefault:   components.TimeoutActionDefault,
		timeoutActionExtended:  components.TimeoutActionExtended,
		payloadLimitAbsolute:   components.PayloadLimitAbsolute,
		payloadLimitConfigure:  components.PayloadLimitConfigure,
		payloadLimitScript:     components.PayloadLimitScript,
		payloadLimitCertUpdate: components.PayloadLimitCertUpdate,
		payloadLimitDefault:    components.PayloadLimitDefault,
		target:                 cfg.NATS.Target,
	}

	// Start the RequestManager background routines (recovery / sweepers)
	components.ReqManager.Start(ctx)

	// Initialize the bounded Command Result Queue (REQ-013)
	resultQueue := make(chan agentcore.ResultEnvelope, cfg.Queues.CommandResultCapacity)

	// Helper to subscribe to NATS results
	subscribeResults := func(nc *nats.NATSClient) error {
		err := nc.SubscribeResults(ctx, cfg.NATS.Target, func(res agentcore.ResultEnvelope) {
			handleNATSResult(ctx, res, resultQueue, components, cfg.Serial)
		})
		if err != nil {
			log.Printf("ERROR: Failed to subscribe to NATS results: %v\n", err)
		}
		return err
	}

	// Subscribe if client is ready; otherwise start retry loop in background (resilience against boot outages)
	var subscribed bool
	if components.NatsClient != nil {
		if err := subscribeResults(components.NatsClient); err == nil {
			subscribed = true
		} else {
			log.Printf("[NATS] Initial subscription failed, closing client and entering recovery loop: %v\n", err)
			_ = components.NatsClient.Close(context.Background())
			handler.SetNATSClient(nil)
		}
	}

	if !subscribed {
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
					nc, err := nats.NewNATSClient(cfg.NATS.Target, cfg.NATS, natsStateChange)
					if err != nil {
						log.Printf("[NATS] Dynamic NATS initialization failed: %v\n", err)
						continue
					}
					if err := subscribeResults(nc); err != nil {
						log.Printf("[NATS] Dynamic subscription failed: %v, closing client\n", err)
						_ = nc.Close(context.Background())
						continue
					}
					log.Println("[NATS] Dynamic NATS initialization and subscription succeeded!")
					handler.SetNATSClient(nc)
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
				processNATSResult(ctx, res, components, cfg.Serial)
			}
		}
	}()

	fatalErr := make(chan error, 1)
	go func() {
		if err := components.WsClient.ReconnectLoop(ctx, handler); err != nil {
			if !errors.Is(err, context.Canceled) {
				fatalErr <- err
			}
		}
	}()

	// 6. Listen for SIGINT / SIGTERM or Fatal WebSocket failures
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	exitCode := 0
	select {
	case sig := <-sigChan:
		log.Printf("Received signal: %v. Initiating graceful teardown...\n", sig)
	case err := <-fatalErr:
		log.Printf("Fatal websocket failure: %v. Initiating teardown...\n", err)
		exitCode = 1
	}

	// Allow a strict 5-second deadline for teardown
	teardownCtx, teardownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer teardownCancel()

	// Perform graceful teardowns
	cancel() // Notify loops to stop
	components.WsClient.Close()
	if activeNats := handler.GetNATSClient(); activeNats != nil {
		_ = activeNats.Close(teardownCtx)
	}

	log.Println("Graceful teardown complete. Exiting.")
	os.Exit(exitCode)
}

type AppComponents struct {
	Scheduler              *queues.PriorityScheduler
	ReqManager             *reqmgr.DefaultRequestManager
	NatsClient             *nats.NATSClient
	WsClient               *websocket.WSClient
	TimeoutDispatch        time.Duration
	TimeoutConfigure       time.Duration
	TimeoutActionDefault   time.Duration
	TimeoutActionExtended  time.Duration
	PayloadLimitAbsolute   int
	PayloadLimitConfigure  int
	PayloadLimitScript     int
	PayloadLimitCertUpdate int
	PayloadLimitDefault    int
}

func processNATSResult(ctx context.Context, res agentcore.ResultEnvelope, components *AppComponents, serial string) {
	log.Printf("[NATS RESULT] Processing result for rpc_id=%s, command=%s, error_code=%s, result_size=%d, payload_size=%d\n", res.RPCID, res.CommandType, res.ErrorCode, len(res.Result), len(res.Payload))

	// Retrieve the transaction from RequestManager using NATS RPCID (UUID)
	tx, exists := components.ReqManager.GetTransaction(res.RPCID)
	if !exists {
		log.Printf("[NATS RESULT] WARNING: Transaction not found for NATS RPCID: %s\n", res.RPCID)
		return
	}
	sessionID := tx.CloudSessionID
	rawCloudID := tx.CloudRPCID
	isNotification := !tx.RespondToCloud

	// Build standard device result object containing "status", "serial", and "uuid"
	formattedResult := contracts.BuildDeviceResultObject(
		serial,
		res.UUID,
		res.Result,
		res.ErrorCode,
		res.Message,
		res.Payload,
	)

	var respBytes []byte
	if !isNotification {
		resp := contracts.JSONRPCResponse{
			JSONRPC: contracts.JSONRPCVersion,
			Result:  formattedResult,
			ID:      rawCloudID,
		}
		respBytes, _ = json.Marshal(resp)
	}

	// Complete the transaction in RequestManager with the full response payload (REQ-009)
	if tx.Method == string(contracts.ActionUpgrade) {
		if res.ErrorCode != "" && res.ErrorCode != "0" {
			log.Printf("[NATS RESULT] WARNING: Upgrade request rejected by device. Aborting persistent operation for RPCID %s\n", res.RPCID)
			if err := components.ReqManager.Fail(res.RPCID, respBytes); err != nil {
				log.Printf("[NATS RESULT] WARNING: Fail() rejected for RPCID %s: %v\n", res.RPCID, err)
				return
			}
		} else {
			_, err := components.ReqManager.RespondAndRetain(ctx, res.RPCID, respBytes)
			switch {
			case err == nil:
				// success
			case errors.Is(err, reqmgr.ErrHandoffInProgress):
				log.Printf("[NATS RESULT] Upgrade handoff already in progress for RPCID %s (ignoring duplicate)\n", res.RPCID)
				return
			default:
				log.Printf("[NATS RESULT] ERROR: RespondAndRetain failed for upgrade RPCID %s: %v\n", res.RPCID, err)
				if !isNotification {
					errResp := contracts.JSONRPCResponse{
						JSONRPC: contracts.JSONRPCVersion,
						Error: &contracts.JSONRPCError{
							Code:    -32603,
							Message: "Internal Error",
							Data:    json.RawMessage(`"Failed to establish persistent upgrade operation"`),
						},
						ID: rawCloudID,
					}
					respBytes, _ = json.Marshal(errResp)
				}
				if failErr := components.ReqManager.Fail(res.RPCID, respBytes); failErr != nil {
					log.Printf("[NATS RESULT] WARNING: Fail() rejected after RespondAndRetain failure for RPCID %s: %v\n", res.RPCID, failErr)
					return
				}
			}
		}
	} else {
		if err := components.ReqManager.Complete(res.RPCID, respBytes); err != nil {
			log.Printf("[NATS RESULT OVERFLOW] WARNING: Transaction Complete failed for NATS RPCID %s: %v\n", res.RPCID, err)
			return
		}
	}

	if !isNotification {
		log.Printf("[NATS RESULT] Pushing response to cloud (Session=%s, ID=%s, Size=%d)\n", sessionID, contracts.FormatLogID(rawCloudID), len(respBytes))
		_ = components.Scheduler.Push(queues.OutboundMessage{
			SessionID: sessionID,
			Priority:  queues.PriorityHighest,
			Payload:   respBytes,
		})
	}
}

func handleNATSResult(ctx context.Context, res agentcore.ResultEnvelope, resultQueue chan<- agentcore.ResultEnvelope, components *AppComponents, serial string) {
	select {
	case resultQueue <- res:
	default:
		log.Printf("ERROR: command_result_overflow! Queue capacity reached. Dropped result for rpc_id=%s, command=%s, error_code=%s, result_size=%d, payload_size=%d\n",
			res.RPCID, res.CommandType, res.ErrorCode, len(res.Result), len(res.Payload))

		// Proactively complete and cache the transaction inside RequestManager using the NATS result payload
		if tx, exists := components.ReqManager.GetTransaction(res.RPCID); exists {
			isNotification := !tx.RespondToCloud
			formattedResult := contracts.BuildDeviceResultObject(
				serial,
				res.UUID,
				res.Result,
				res.ErrorCode,
				res.Message,
				res.Payload,
			)

			var respBytes []byte
			if !isNotification {
				resp := contracts.JSONRPCResponse{
					JSONRPC: contracts.JSONRPCVersion,
					Result:  formattedResult,
					ID:      tx.CloudRPCID,
				}
				respBytes, _ = json.Marshal(resp)
			}

			// Complete and cache the transaction in RequestManager so it is resolved and cleaned up from memory.
			// We do not push it to the scheduler to avoid further congestion.
			if tx.Method == string(contracts.ActionUpgrade) {
				if res.ErrorCode != "" && res.ErrorCode != "0" {
					log.Printf("[NATS RESULT OVERFLOW] WARNING: Upgrade request rejected by device. Aborting persistent operation for RPCID %s\n", res.RPCID)
					_ = components.ReqManager.Fail(res.RPCID, respBytes) // Ignore error since we don't push overflow failures anyway
					return
				}
				_, err := components.ReqManager.RespondAndRetain(ctx, res.RPCID, respBytes)
				switch {
				case err == nil:
					// success
				case errors.Is(err, reqmgr.ErrHandoffInProgress):
					log.Printf("[NATS RESULT OVERFLOW] Upgrade handoff already in progress for RPCID %s (ignoring duplicate)\n", res.RPCID)
					return
				default:
					log.Printf("[NATS RESULT OVERFLOW] ERROR: RespondAndRetain failed for upgrade RPCID %s: %v\n", res.RPCID, err)
					_ = components.ReqManager.Fail(res.RPCID, nil) // Ignore error since we don't push overflow failures anyway
					return
				}
			} else {
				if err := components.ReqManager.Complete(res.RPCID, respBytes); err != nil {
					log.Printf("[NATS RESULT OVERFLOW] WARNING: Transaction Complete failed for NATS RPCID %s: %v\n", res.RPCID, err)
					return
				}
			}
			log.Printf("[NATS RESULT OVERFLOW] Warning: Completed/cached transaction rpc_id=%s, but omitted outbound WebSocket scheduler enqueue to avoid congestion.\n", res.RPCID)
		}
	}
}

func initializeComponents(ctx context.Context, cfg *config.Config, cacheTTLConfig config.CacheTTLConfig, stateMgr *systemStateManager) (*AppComponents, error) {
	// Parse timeout environment variables
	dispatchTimeout, err := parseTimeoutEnv("OLG_TIMEOUT_DISPATCH", 5*time.Second)
	if err != nil {
		return nil, err
	}
	timeoutConfigure, err := parseTimeoutEnv("OLG_TIMEOUT_CONFIGURE", 30*time.Second)
	if err != nil {
		return nil, err
	}
	timeoutActionDefault, err := parseTimeoutEnv("OLG_TIMEOUT_ACTION_DEFAULT", 60*time.Second)
	if err != nil {
		return nil, err
	}
	timeoutActionExtended, err := parseTimeoutEnv("OLG_TIMEOUT_ACTION_EXTENDED", 120*time.Second)
	if err != nil {
		return nil, err
	}

	// Parse limit environment variables
	payloadLimitAbsolute, err := parseLimitEnv("OLG_PAYLOAD_LIMIT_ABSOLUTE", 12*1024*1024)
	if err != nil {
		return nil, err
	}
	payloadLimitConfigure, err := parseLimitEnv("OLG_PAYLOAD_LIMIT_CONFIGURE", 10*1024*1024)
	if err != nil {
		return nil, err
	}
	payloadLimitScript, err := parseLimitEnv("OLG_PAYLOAD_LIMIT_SCRIPT", 1*1024*1024)
	if err != nil {
		return nil, err
	}
	payloadLimitCertUpdate, err := parseLimitEnv("OLG_PAYLOAD_LIMIT_CERTUPDATE", 2*1024*1024)
	if err != nil {
		return nil, err
	}
	payloadLimitDefault, err := parseLimitEnv("OLG_PAYLOAD_LIMIT_DEFAULT", 11*1024*1024)
	if err != nil {
		return nil, err
	}

	// Validate and clamp method limits against the absolute limit
	if payloadLimitConfigure > payloadLimitAbsolute {
		payloadLimitConfigure = payloadLimitAbsolute
	}
	if payloadLimitScript > payloadLimitAbsolute {
		payloadLimitScript = payloadLimitAbsolute
	}
	if payloadLimitCertUpdate > payloadLimitAbsolute {
		payloadLimitCertUpdate = payloadLimitAbsolute
	}
	if payloadLimitDefault > payloadLimitAbsolute {
		payloadLimitDefault = payloadLimitAbsolute
	}

	// Validate absolute limit against WebSocket transport maximum frame size
	wsMaxFrame := cfg.Cloud.MaxFrameSizeBytes
	if wsMaxFrame > 0 && payloadLimitAbsolute > wsMaxFrame {
		log.Printf("WARNING: OLG_PAYLOAD_LIMIT_ABSOLUTE (%d) exceeds WebSocket MaxFrameSizeBytes (%d). Clamping absolute limit.", payloadLimitAbsolute, wsMaxFrame)
		payloadLimitAbsolute = wsMaxFrame
		// Re-clamp method limits to the new absolute limit
		if payloadLimitConfigure > payloadLimitAbsolute {
			payloadLimitConfigure = payloadLimitAbsolute
		}
		if payloadLimitScript > payloadLimitAbsolute {
			payloadLimitScript = payloadLimitAbsolute
		}
		if payloadLimitCertUpdate > payloadLimitAbsolute {
			payloadLimitCertUpdate = payloadLimitAbsolute
		}
		if payloadLimitDefault > payloadLimitAbsolute {
			payloadLimitDefault = payloadLimitAbsolute
		}
	}

	// Publish the final clamped limits to the contracts validation package
	contracts.SetLimits(payloadLimitConfigure, payloadLimitCertUpdate, payloadLimitScript)

	var traceUploadAllowedURL *url.URL
	traceUploadAllowedURLStr := os.Getenv("OLG_TRACE_UPLOAD_ALLOWED_URL")
	if traceUploadAllowedURLStr != "" {
		u, err := url.Parse(traceUploadAllowedURLStr)
		if err != nil {
			return nil, fmt.Errorf("invalid OLG_TRACE_UPLOAD_ALLOWED_URL: %w", err)
		}
		if u.Scheme != "https" {
			return nil, fmt.Errorf("OLG_TRACE_UPLOAD_ALLOWED_URL must have https scheme, got: %s", u.Scheme)
		}
		if u.User != nil {
			return nil, fmt.Errorf("OLG_TRACE_UPLOAD_ALLOWED_URL must not contain credentials")
		}
		traceUploadAllowedURL = u
	}
	contracts.AllowedTraceUploadURL = traceUploadAllowedURL

	// Initialize capability cache
	log.Println("Initializing CapabilityCache...")
	capCache := nats.NewCapabilityCache("./capabilities.json")

	// Initialize Outbound Schedulers and Buffers
	log.Println("Initializing Outbound Schedulers...")
	scheduler := queues.NewPriorityScheduler(cfg.Queues.WSWriterCapacity, cfg.Queues.EmergencyCapacity)
	_ = queues.NewTelemetryRingBuffer(cfg.Queues.TelemetryCapacity)

	// Initialize Storage and Cache components
	log.Println("Initializing Operation Store...")
	store, err := reqmgr.NewDiskOperationStore("./operations")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize operation store: %w", err)
	}

	txCache := reqmgr.NewTransactionCache()
	txCache.StartCacheSweeper(ctx, 1*time.Minute)

	// Convert config.CacheTTLConfig to reqmgr.CacheTTLConfig
	reqmgrTTLConfig := reqmgr.CacheTTLConfig{
		DefaultTTL: time.Duration(cacheTTLConfig.Default) * time.Second,
		MethodTTLs: map[string]time.Duration{
			"configure":  time.Duration(cacheTTLConfig.Configure) * time.Second,
			"leds":       time.Duration(cacheTTLConfig.LEDs) * time.Second,
			"reboot":     time.Duration(cacheTTLConfig.Reboot) * time.Second,
			"rtty":       time.Duration(cacheTTLConfig.RemoteAccess) * time.Second,
			"factory":    time.Duration(cacheTTLConfig.Factory) * time.Second,
			"upgrade":    time.Duration(cacheTTLConfig.Upgrade) * time.Second,
			"certupdate": time.Duration(cacheTTLConfig.Certupdate) * time.Second,
			"reenroll":   time.Duration(cacheTTLConfig.Reenroll) * time.Second,
			"script":     time.Duration(cacheTTLConfig.Script) * time.Second,
		},
	}

	// Instantiate RequestManager
	reqManager, err := reqmgr.NewRequestManager(
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
		return nil, fmt.Errorf("failed to initialize RequestManager: %w", err)
	}

	// Instantiate NATS and WS clients
	natsStateChange := func(state contracts.LinkState) {
		log.Printf("[NATS STATE] Changed to: %v\n", state)
		stateMgr.UpdateNATSLink(state)
	}

	log.Println("Initializing NATS client...")
	natsClient, err := nats.NewNATSClient(cfg.NATS.Target, cfg.NATS, natsStateChange)
	if err != nil {
		log.Printf("WARNING: NATS failed to initialize (NATSDegraded mode): %v\n", err)
	}

	wsStateChange := func(state contracts.LinkState) {
		log.Printf("[WS STATE] Changed to: %v\n", state)
		stateMgr.UpdateWSLink(state)
	}
	metaProvider := &connectMetadataProvider{cache: capCache, serial: cfg.Serial}

	log.Printf("Initializing WSClient for %s...\n", cfg.Cloud.URL)
	wsClient, err := websocket.NewWSClient(cfg.Cloud, scheduler, metaProvider, wsStateChange)
	if err != nil {
		if natsClient != nil {
			_ = natsClient.Close(context.Background())
		}
		return nil, fmt.Errorf("failed to initialize WSClient: %w", err)
	}

	return &AppComponents{
		Scheduler:              scheduler,
		ReqManager:             reqManager,
		NatsClient:             natsClient,
		WsClient:               wsClient,
		TimeoutDispatch:        dispatchTimeout,
		TimeoutConfigure:       timeoutConfigure,
		TimeoutActionDefault:   timeoutActionDefault,
		TimeoutActionExtended:  timeoutActionExtended,
		PayloadLimitAbsolute:   payloadLimitAbsolute,
		PayloadLimitConfigure:  payloadLimitConfigure,
		PayloadLimitScript:     payloadLimitScript,
		PayloadLimitCertUpdate: payloadLimitCertUpdate,
		PayloadLimitDefault:    payloadLimitDefault,
	}, nil
}

func parseLimitEnv(envName string, defaultVal int) (int, error) {
	valStr := os.Getenv(envName)
	if valStr == "" {
		return defaultVal, nil
	}
	val, err := strconv.Atoi(strings.TrimSpace(valStr))
	if err != nil || val <= 0 {
		return 0, fmt.Errorf("invalid limit for %s: must be a positive integer", envName)
	}
	return val, nil
}
