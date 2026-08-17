package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	reqMgr     *reqmgr.DefaultRequestManager
	stateMgr   *systemStateManager
	scheduler  *queues.PriorityScheduler
	natsClient     *nats.NATSClient
	serial         string
	dispatchBuffer chan struct{}
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
	case "remote_access":
		return contracts.CommandAction, contracts.ActionRTTY, false
	case "capabilities.get":
		return contracts.CommandQuery, contracts.ActionCapabilitiesGet, false
	case "status.get":
		return contracts.CommandQuery, contracts.ActionStatusGet, false
	default:
		return "", "", false
	}
}

func getMethodPayloadLimit(method string) int {
	switch method {
	case "configure":
		return 10 * 1024 * 1024 // 10MB
	case "certupdate":
		return 2 * 1024 * 1024  // 2MB
	case "script":
		return 1 * 1024 * 1024  // 1MB
	default:
		return 11 * 1024 * 1024 // 11MB (transport frame limit)
	}
}

func (h *frameHandler) pushResponse(sessionID string, id json.RawMessage, result json.RawMessage, errObj *contracts.JSONRPCError) {
	resp := contracts.JSONRPCResponse{
		JSONRPC: contracts.JSONRPCVersion,
		ID:      id,
		Result:  result,
		Error:   errObj,
	}
	respBytes, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[FrameHandler] ERROR: Failed to marshal JSON-RPC response: %v\n", err)
		return
	}
	_ = h.scheduler.Push(queues.OutboundMessage{
		SessionID: sessionID,
		Priority:  queues.PriorityHighest,
		Payload:   respBytes,
	})
}

func (h *frameHandler) HandleFrame(ctx context.Context, frame websocket.InboundFrame) (websocket.FrameDisposition, error) {
	log.Printf("[FrameHandler] Received frame: Session=%s, Type=%d, Size=%d\n", frame.SessionID, frame.Type, len(frame.Payload))

	// Parse JSON-RPC request structure
	var rpcReq contracts.JSONRPCRequest
	if err := json.Unmarshal(frame.Payload, &rpcReq); err != nil {
		log.Printf("[FrameHandler] Parse error: %v\n", err)
		// If ID is valid JSON, respond; otherwise drop/close
		var raw struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.Unmarshal(frame.Payload, &raw)
		if len(raw.ID) > 0 && string(raw.ID) != "null" {
			errObj := &contracts.JSONRPCError{
				Code:    contracts.ErrParse,
				Message: "Parse error",
			}
			h.pushResponse(frame.SessionID, raw.ID, nil, errObj)
		}
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Validate JSON-RPC structure (REQ-027)
	if err := rpcReq.Validate(); err != nil {
		log.Printf("[FrameHandler] Invalid request: %v\n", err)
		if len(rpcReq.ID) > 0 && string(rpcReq.ID) != "null" {
			errObj := &contracts.JSONRPCError{
				Code:    contracts.ErrInvalidRequest,
				Message: fmt.Sprintf("Invalid request: %v", err),
			}
			h.pushResponse(frame.SessionID, rpcReq.ID, nil, errObj)
		}
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Get NATS command/action mappings
	command, action, isStateChanging := getCommandAction(rpcReq.Method)
	if command == "" {
		log.Printf("[FrameHandler] Method not found: %s\n", rpcReq.Method)
		errObj := &contracts.JSONRPCError{
			Code:    contracts.ErrMethodNotFound,
			Message: fmt.Sprintf("Method %s not found", rpcReq.Method),
		}
		h.pushResponse(frame.SessionID, rpcReq.ID, nil, errObj)
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Enforce payload size limits (REQ-020)
	limit := getMethodPayloadLimit(rpcReq.Method)
	if len(frame.Payload) > limit {
		log.Printf("[FrameHandler] Payload size %d exceeds limit of %d for method %s\n", len(frame.Payload), limit, rpcReq.Method)
		errObj, _ := contracts.NewInternalJSONRPCError(contracts.ErrValidationFailed, fmt.Sprintf("Payload size exceeds method limit of %d", limit))
		errObj.Code = contracts.ErrInvalidParams
		h.pushResponse(frame.SessionID, rpcReq.ID, nil, errObj)
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Validate NATS availability. Reject with Service Unavailable if NATS is down (REQ-002)
	if h.stateMgr.GetSystemState() == contracts.StateNATSDegraded {
		log.Println("[FrameHandler] Rejecting request: local NATS service is degraded (unavailable)")
		errObj, _ := contracts.NewInternalJSONRPCError(contracts.ErrServiceUnavailable, "Local NATS service is unavailable")
		h.pushResponse(frame.SessionID, rpcReq.ID, nil, errObj)
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Determine transaction timeout duration
	timeout := 30 * time.Second
	if rpcReq.Method == "configure" {
		timeout = 120 * time.Second
	} else if rpcReq.Method == "upgrade" {
		timeout = 60 * time.Second
	}

	// Create transaction (REQ-007, REQ-008, REQ-009)
	tx, err := h.reqMgr.CreateTransaction(frame.SessionID, rpcReq.ID, true, rpcReq.Method, timeout, isStateChanging)
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
		return websocket.FrameRejectedKeepConnection, nil
	}

	// Handle transaction execution asynchronously (or synchronously for queries)
	go h.executeTransaction(ctx, tx, command, action, rpcReq.Params)

	return websocket.FrameAccepted, nil
}

func (h *frameHandler) executeTransaction(ctx context.Context, tx *reqmgr.Transaction, command contracts.CommandType, action contracts.ActionType, params json.RawMessage) {
	// Enforce NATS dispatch buffer limits (REQ-012)
	select {
	case h.dispatchBuffer <- struct{}{}:
		defer func() { <-h.dispatchBuffer }()
	default:
		log.Println("[FrameHandler] NATS dispatch buffer is full, failing fast")
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
	dispatchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var dispatchErr error
	switch command {
	case contracts.CommandConfigure:
		cmd := &agentcore.ConfigureCommand{
			Version:   contracts.EnvelopeVersion,
			RPCID:     tx.RPCID,
			Target:    h.serial,
			UUID:      uuidVal,
			Payload:   params,
			Timestamp: time.Now().UTC(),
		}
		dispatchErr = h.natsClient.SubmitConfigure(dispatchCtx, cmd)

	case contracts.CommandQuery:
		if action == contracts.ActionCapabilitiesGet {
			query := &contracts.CloudCapabilitiesQuery{
				Version:   contracts.EnvelopeVersion,
				RPCID:     tx.RPCID,
				Target:    h.serial,
				Timestamp: time.Now().UTC(),
			}
			res, err := h.natsClient.QueryCapabilities(dispatchCtx, query)
			if err != nil {
				h.failTransaction(tx, err)
				return
			}
			_ = h.reqMgr.MarkInFlight(tx.RPCID)
			h.completeTransaction(tx, res)
			return
		} else if action == contracts.ActionStatusGet {
			query := &contracts.CloudDeviceStatusQuery{
				Version:   contracts.EnvelopeVersion,
				RPCID:     tx.RPCID,
				Target:    h.serial,
				Timestamp: time.Now().UTC(),
			}
			res, err := h.natsClient.QueryDeviceStatus(dispatchCtx, query)
			if err != nil {
				h.failTransaction(tx, err)
				return
			}
			_ = h.reqMgr.MarkInFlight(tx.RPCID)
			resBytes, _ := json.Marshal(res.Payload)
			h.completeTransaction(tx, resBytes)
			return
		}

	default: // CommandAction, CommandUpgrade, CommandScript, CommandReboot
		cmd := &agentcore.ActionCommand{
			Version:     contracts.EnvelopeVersion,
			RPCID:       tx.RPCID,
			Target:      h.serial,
			CommandType: string(command),
			Action:      string(action),
			Payload:     params,
			Timestamp:   time.Now().UTC(),
		}
		dispatchErr = h.natsClient.ExecuteAction(dispatchCtx, cmd)
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

func (h *frameHandler) completeTransaction(tx *reqmgr.Transaction, payload []byte) {
	resp := contracts.JSONRPCResponse{
		JSONRPC: contracts.JSONRPCVersion,
		Result:  payload,
		ID:      tx.CloudRPCID,
	}
	respBytes, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[FrameHandler] ERROR: Failed to marshal response: %v\n", err)
		return
	}

	if err := h.reqMgr.Complete(tx.RPCID, respBytes); err != nil {
		log.Printf("[FrameHandler] Complete transaction failed: %v\n", err)
		return
	}

	_ = h.scheduler.Push(queues.OutboundMessage{
		SessionID: tx.CloudSessionID,
		Priority:  queues.PriorityHighest,
		Payload:   respBytes,
	})
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
	
	_ = h.scheduler.Push(queues.OutboundMessage{
		SessionID: tx.CloudSessionID,
		Priority:  queues.PriorityHighest,
		Payload:   respBytes,
	})
}
