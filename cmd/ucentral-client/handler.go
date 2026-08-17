package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

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
	reqMgr    *reqmgr.DefaultRequestManager
	stateMgr  *systemStateManager
	scheduler *queues.PriorityScheduler
}

func (h *frameHandler) HandleFrame(ctx context.Context, frame websocket.InboundFrame) (websocket.FrameDisposition, error) {
	log.Printf("[FrameHandler] Received frame: Session=%s, Type=%d, Size=%d\n", frame.SessionID, frame.Type, len(frame.Payload))

	// Parse JSON-RPC ID so we can respond to the exact request on failure
	var req struct {
		ID json.RawMessage `json:"id"`
	}
	_ = json.Unmarshal(frame.Payload, &req)

	// Validate connectivity state. If NATS is degraded, reject with Service Unavailable (-32603 / code 3)
	if h.stateMgr.GetSystemState() == contracts.StateNATSDegraded {
		log.Println("[FrameHandler] Rejecting request: local NATS service is degraded (unavailable)")

		if len(req.ID) > 0 && string(req.ID) != "null" {
			errObj, _ := contracts.NewInternalJSONRPCError(contracts.ErrServiceUnavailable, "Local NATS service is unavailable")
			resp := contracts.JSONRPCResponse{
				JSONRPC: contracts.JSONRPCVersion,
				Error:   errObj,
				ID:      req.ID,
			}
			respBytes, _ := json.Marshal(resp)
			_ = h.scheduler.Push(queues.OutboundMessage{
				SessionID: frame.SessionID,
				Priority:  queues.PriorityHighest,
				Payload:   respBytes,
			})
		}
		return websocket.FrameRejectedKeepConnection, nil
	}

	// For production: the main loop frame handler processes incoming frames from the cloud.
	// In the real system, it would call h.reqMgr.HandleRequest(...) to start transactions.
	return websocket.FrameAccepted, nil
}
