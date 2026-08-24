package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
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

// systemStateManager tracks NATS and WS connection links thread-safely
type systemStateManager struct {
	mu       sync.RWMutex
	natsLink contracts.LinkState
	wsLink   contracts.LinkState
}

func (s *systemStateManager) UpdateNATSLink(state contracts.LinkState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.natsLink = state
}

func (s *systemStateManager) UpdateWSLink(state contracts.LinkState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wsLink = state
}

func (s *systemStateManager) GetSystemState() contracts.ConnectionState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, _ := contracts.DeriveConnectionState(s.wsLink, s.natsLink)
	return state
}

// frameHandler routes inbound websocket frames to NATS via the RequestManager
type frameHandler struct {
	mu                     sync.RWMutex
	reqMgr                 *reqmgr.DefaultRequestManager
	stateMgr               *systemStateManager
	scheduler              *queues.PriorityScheduler
	natsClient             *nats.NATSClient
	serial                 string
	dispatchBuffer         chan struct{}
	timeoutDispatch        time.Duration
	timeoutConfigure       time.Duration
	timeoutActionDefault   time.Duration
	timeoutActionExtended  time.Duration
	payloadLimitAbsolute   int
	payloadLimitConfigure  int
	payloadLimitScript     int
	payloadLimitCertUpdate int
	payloadLimitDefault    int
	traceUploadAllowedURL  *url.URL
	target                 string
}

func (h *frameHandler) GetNATSClient() *nats.NATSClient {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.natsClient
}

func (h *frameHandler) SetNATSClient(nc *nats.NATSClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.natsClient = nc
}

func getCommandAction(method string) (contracts.CommandType, contracts.ActionType, bool) {
	switch method {
	case "configure":
		return contracts.CommandConfigure, "", true
	case "reboot":
		return contracts.CommandReboot, contracts.ActionReboot, true
	case "factory":
		return contracts.CommandAction, contracts.ActionFactory, true
	case "leds":
		return contracts.CommandAction, contracts.ActionLeds, true
	case "upgrade":
		return contracts.CommandUpgrade, contracts.ActionUpgrade, true
	case "certupdate":
		return contracts.CommandAction, contracts.ActionCertupdate, true
	case "reenroll":
		return contracts.CommandAction, contracts.ActionReenroll, true
	case "script":
		return contracts.CommandScript, contracts.ActionExecute, true
	case "ping":
		return contracts.CommandAction, contracts.ActionPing, false
	case "trace":
		return contracts.CommandAction, contracts.ActionTrace, false
	case "telemetry":
		return contracts.CommandAction, contracts.ActionTelemetry, false
	case "rtty":
		return contracts.CommandAction, contracts.ActionRTTY, false
	default:
		return "", "", false
	}
}

func (h *frameHandler) getMethodPayloadLimit(method string) int {
	switch method {
	case "configure":
		return h.payloadLimitConfigure
	case "certupdate":
		return h.payloadLimitCertUpdate
	case "script":
		return h.payloadLimitScript
	default:
		return h.payloadLimitDefault
	}
}

func (h *frameHandler) getTransactionTimeout(method string, params json.RawMessage) time.Duration {
	switch method {
	case "configure":
		return h.timeoutConfigure
	case "upgrade", "certupdate", "script":
		return h.timeoutActionExtended
	case "trace":
		// The VyOS agent returns only after packet capture has completed. Keep a
		// full transport/dispatch buffer beyond the requested capture duration so
		// a 30-second trace cannot expire at the same instant as its result.
		var trace contracts.CloudTraceRequest
		if err := json.Unmarshal(params, &trace); err == nil && trace.Duration != nil && *trace.Duration > 0 {
			return time.Duration(*trace.Duration)*time.Second + 30*time.Second
		}
		return h.timeoutActionExtended
	default:
		return h.timeoutActionDefault
	}
}

func (h *frameHandler) pushResponse(sessionID string, id json.RawMessage, result json.RawMessage, errObj *contracts.JSONRPCError) {
	var finalResult json.RawMessage
	if errObj == nil {
		finalResult = contracts.EnsureStatusInResult(result)
	} else {
		finalResult = result
	}
	resp := contracts.JSONRPCResponse{
		JSONRPC: contracts.JSONRPCVersion,
		ID:      id,
		Result:  finalResult,
		Error:   errObj,
	}
	respBytes, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[FrameHandler] ERROR: Failed to marshal JSON-RPC response: %v\n", err)
		return
	}
	var errCode int
	if errObj != nil {
		errCode = errObj.Code
	}
	log.Printf("[FrameHandler] Pushing response to cloud (Session=%s, ID=%s, ErrorCode=%d, Size=%d)\n", sessionID, contracts.FormatLogID(id), errCode, len(respBytes))
	if err := h.scheduler.Push(queues.OutboundMessage{
		SessionID: sessionID,
		Priority:  queues.PriorityHighest,
		Payload:   respBytes,
	}); err != nil {
		log.Printf("[FrameHandler] WARNING: Failed to push response to cloud (Session=%s, ID=%s, Error=%v)\n", sessionID, contracts.FormatLogID(id), err)
	}
}

func (h *frameHandler) HandleFrame(ctx context.Context, frame websocket.InboundFrame) (websocket.FrameDisposition, error) {
	// 1. Enforce absolute transport/boundary size limit before any parsing/logging (REQ-020)
	if len(frame.Payload) > h.payloadLimitAbsolute {
		log.Printf("[FrameHandler] Received frame exceeds absolute limit (Session=%s, Size=%d, Limit=%d)\n", frame.SessionID, len(frame.Payload), h.payloadLimitAbsolute)
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Log frame metadata only. Avoid logging raw payload to prevent leaking configuration, certificates, or script contents.
	log.Printf("[FrameHandler] Received frame: Session=%s, Type=%d, Size=%d\n", frame.SessionID, frame.Type, len(frame.Payload))

	// 2. Extract method and ID using a lightweight, bounded parse to enforce specific limits before full unmarshalling (REQ-020)
	var metaExtractor struct {
		Method string          `json:"method"`
		ID     json.RawMessage `json:"id"`
	}
	_ = json.Unmarshal(frame.Payload, &metaExtractor)

	log.Printf("[FrameHandler] Parsed request metadata: Method=%s, ID=%s\n", metaExtractor.Method, contracts.FormatLogID(metaExtractor.ID))

	isNotificationMeta := len(metaExtractor.ID) == 0 || string(metaExtractor.ID) == "null"
	limit := h.getMethodPayloadLimit(metaExtractor.Method)
	if len(frame.Payload) > limit {
		log.Printf("[FrameHandler] Payload size %d exceeds limit of %d for method %s\n", len(frame.Payload), limit, metaExtractor.Method)
		if !isNotificationMeta {
			errObj, _ := contracts.NewInternalJSONRPCError(contracts.ErrValidationFailed, fmt.Sprintf("Payload size exceeds method limit of %d", limit))
			errObj.Code = contracts.ErrInvalidParams
			h.pushResponse(frame.SessionID, metaExtractor.ID, nil, errObj)
		}
		return websocket.FrameRejectedKeepConnection, nil
	}

	// 3. Now perform full parse and allocate memory for JSONRPCRequest
	var rpcReq contracts.JSONRPCRequest
	if err := json.Unmarshal(frame.Payload, &rpcReq); err != nil {
		log.Printf("[FrameHandler] Parse error: %v\n", err)
		var raw struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.Unmarshal(frame.Payload, &raw)

		id := json.RawMessage("null")
		if len(raw.ID) > 0 {
			id = raw.ID
		}

		errObj := &contracts.JSONRPCError{
			Code:    contracts.ErrParse,
			Message: "Parse error",
		}
		h.pushResponse(frame.SessionID, id, nil, errObj)
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Validate JSON-RPC structure (REQ-027)
	if err := rpcReq.Validate(); err != nil {
		log.Printf("[FrameHandler] Invalid request: %v\n", err)
		id := json.RawMessage("null")
		if len(rpcReq.ID) > 0 {
			id = rpcReq.ID
		}
		errObj := &contracts.JSONRPCError{
			Code:    contracts.ErrInvalidRequest,
			Message: fmt.Sprintf("Invalid request: %v", err),
		}
		h.pushResponse(frame.SessionID, id, nil, errObj)
		return websocket.FrameRejectedKeepConnection, nil
	}

	isNotification := len(rpcReq.ID) == 0 || string(rpcReq.ID) == "null"

	// Get NATS command/action mappings
	command, action, isStateChanging := getCommandAction(rpcReq.Method)
	if command == "" {
		log.Printf("[FrameHandler] Method not found: %s\n", rpcReq.Method)
		if !isNotification {
			errObj := &contracts.JSONRPCError{
				Code:    contracts.ErrMethodNotFound,
				Message: fmt.Sprintf("Method %s not found", rpcReq.Method),
			}
			h.pushResponse(frame.SessionID, rpcReq.ID, nil, errObj)
		}
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Validate method-specific parameter payload (Validate incoming API inputs)
	if err := contracts.ValidateCommandPayload(command, action, rpcReq.Params); err != nil {
		log.Printf("[FrameHandler] Invalid parameters for method %s: %v\n", rpcReq.Method, err)
		if !isNotification {
			errObj := &contracts.JSONRPCError{
				Code:    contracts.ErrInvalidParams,
				Message: fmt.Sprintf("Invalid parameters: %v", err),
			}
			h.pushResponse(frame.SessionID, rpcReq.ID, nil, errObj)
		}
		return websocket.FrameRejectedKeepConnection, nil
	}

	// State-changing or security-sensitive check for notifications (REQ-029)
	if isNotification && (isStateChanging || rpcReq.Method == string(contracts.ActionRTTY)) {
		log.Printf("[FrameHandler] Rejecting notification: method %s is state-changing or security-sensitive (REQ-029)\n", rpcReq.Method)
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Validate NATS availability. Reject with Service Unavailable if NATS is down (REQ-002)
	if h.stateMgr.GetSystemState() == contracts.StateNATSDegraded {
		log.Println("[FrameHandler] Rejecting request: local NATS service is degraded (unavailable)")
		if !isNotification {
			errObj, _ := contracts.NewInternalJSONRPCError(contracts.ErrServiceUnavailable, "Local NATS service is unavailable")
			h.pushResponse(frame.SessionID, rpcReq.ID, nil, errObj)
		}
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Determine transaction timeout duration.
	timeout := h.getTransactionTimeout(rpcReq.Method, rpcReq.Params)

	// Create transaction (REQ-007, REQ-008, REQ-009, REQ-029)
	tx, err := h.reqMgr.CreateTransaction(frame.SessionID, rpcReq.ID, !isNotification, rpcReq.Method, timeout, isStateChanging)
	if err != nil {
		var cacheErr *reqmgr.CachedResponseError
		if errors.As(err, &cacheErr) {
			log.Printf("[FrameHandler] Duplicate request detected, replaying cached response for ID %s\n", string(rpcReq.ID))
			_ = h.scheduler.Push(queues.OutboundMessage{
				SessionID: frame.SessionID,
				Priority:  queues.PriorityHighest,
				Payload:   cacheErr.Payload,
			})
			return websocket.FrameAccepted, nil
		}

		log.Printf("[FrameHandler] Transaction admission failed: %v\n", err)
		if !isNotification {
			if errors.Is(err, reqmgr.ErrCapacityExceeded) || strings.Contains(err.Error(), "busy") || strings.Contains(err.Error(), "concurrency lock") {
				errObj := &contracts.JSONRPCError{
					Code:    contracts.ErrInternal,
					Message: "Device is busy",
				}
				h.pushResponse(frame.SessionID, rpcReq.ID, nil, errObj)
			} else {
				errObj, _ := contracts.NewInternalJSONRPCError(contracts.ErrAppFailure, fmt.Sprintf("Transaction creation failed: %v", err))
				h.pushResponse(frame.SessionID, rpcReq.ID, nil, errObj)
			}
		}
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Handle transaction execution asynchronously (or synchronously for queries)
	go h.executeTransaction(ctx, tx, command, action, rpcReq.Params)

	return websocket.FrameAccepted, nil
}

func (h *frameHandler) executeTransaction(ctx context.Context, tx *reqmgr.Transaction, command contracts.CommandType, action contracts.ActionType, params json.RawMessage) {
	// Enforce NATS dispatch buffer limits asynchronously after admission (REQ-012).
	// Note: This temporarily consumes request capacity before failing if the buffer is full.
	select {
	case h.dispatchBuffer <- struct{}{}:
		defer func() { <-h.dispatchBuffer }()
	default:
		log.Println("[FrameHandler] NATS dispatch buffer is full, failing transaction asynchronously")
		h.failTransactionWithCode(tx, errors.New("NATS dispatch buffer is full"), contracts.ErrServiceUnavailable, "Local NATS service is unavailable")
		return
	}

	// Transition to PreparingDispatch
	if err := h.reqMgr.MarkPreparingDispatch(tx.RPCID); err != nil {
		log.Printf("[FrameHandler] MarkPreparingDispatch failed: %v\n", err)
		h.failTransaction(tx, fmt.Errorf("failed to enter preparing dispatch: %w", err))
		return
	}

	// Retrieve capabilities uuid for configure or default checks
	uuidVal := "0"
	if command == contracts.CommandConfigure {
		// Extract configuration UUID from params
		var cfgParams struct {
			UUID int64 `json:"uuid"`
		}
		if err := json.Unmarshal(params, &cfgParams); err == nil && cfgParams.UUID > 0 {
			uuidVal = strconv.FormatInt(cfgParams.UUID, 10)
		}
	}

	// Prepare NATS dispatch payload and transition to PendingPublish
	if err := h.reqMgr.MarkPendingPublish(tx.RPCID); err != nil {
		log.Printf("[FrameHandler] MarkPendingPublish failed: %v\n", err)
		h.failTransaction(tx, fmt.Errorf("failed to enter pending publish: %w", err))
		return
	}

	// Execute operation against NATS Client
	dispatchCtx, cancel := context.WithTimeout(ctx, h.timeoutDispatch)
	defer cancel()

	nClient := h.GetNATSClient()
	if nClient == nil {
		h.failTransactionWithCode(tx, errors.New("NATS client not initialized"), contracts.ErrServiceUnavailable, "Local NATS service is unavailable")
		return
	}

	var dispatchErr error
	switch command {
	case contracts.CommandConfigure:
		cmd := &agentcore.ConfigureCommand{
			Version:   contracts.EnvelopeVersion,
			RPCID:     tx.RPCID,
			Target:    h.target,
			UUID:      uuidVal,
			Payload:   params,
			Timestamp: time.Now().UTC(),
		}
		dispatchErr = nClient.SubmitConfigure(dispatchCtx, cmd)

	default: // CommandAction, CommandUpgrade, CommandScript, CommandReboot
		cmd := &agentcore.ActionCommand{
			Version:     contracts.EnvelopeVersion,
			RPCID:       tx.RPCID,
			Target:      h.target,
			CommandType: string(command),
			Action:      string(action),
			Payload:     params,
			Timestamp:   time.Now().UTC(),
		}
		dispatchErr = nClient.ExecuteAction(dispatchCtx, cmd)
	}

	if dispatchErr != nil {
		log.Printf("[FrameHandler] NATS dispatch failed: %v\n", dispatchErr)
		h.failTransaction(tx, fmt.Errorf("NATS dispatch failed: %w", dispatchErr))
		return
	}

	// Transition to InFlight (NATS execution runs asynchronously now)
	if err := h.reqMgr.MarkInFlight(tx.RPCID); err != nil {
		log.Printf("[FrameHandler] MarkInFlight failed: %v\n", err)
	}
}


func (h *frameHandler) failTransaction(tx *reqmgr.Transaction, err error) {
	h.failTransactionWithCode(tx, err, contracts.ErrAppFailure, err.Error())
}

func (h *frameHandler) failTransactionWithCode(tx *reqmgr.Transaction, err error, appCode int, message string) {
	errObj, _ := contracts.NewInternalJSONRPCError(appCode, message)
	resp := contracts.JSONRPCResponse{
		JSONRPC: contracts.JSONRPCVersion,
		Error:   errObj,
		ID:      tx.CloudRPCID,
	}
	respBytes, _ := json.Marshal(resp)
	_ = h.reqMgr.Fail(tx.RPCID, respBytes)

	if tx.RespondToCloud {
		_ = h.scheduler.Push(queues.OutboundMessage{
			SessionID: tx.CloudSessionID,
			Priority:  queues.PriorityHighest,
			Payload:   respBytes,
		})
	}
}
