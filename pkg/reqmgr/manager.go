package reqmgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
)

var (
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrAlreadyTerminal        = errors.New("transaction already in terminal state")
	ErrBusy                   = errors.New("system busy or transaction active")
)

type PendingReply struct {
	Payload []byte
	State   TransactionState
}

type DefaultRequestManager struct {
	mu                  sync.Mutex
	dispatchTimeout     time.Duration
	transactionsByRPCID map[string]*Transaction
	activeCloudRequests map[string]string // Key: RequestKey, Value: RPCID
	stateLock           sync.Mutex
	activeStateTx       string // RPCID or OperationID holding the state lock
	cache               *TransactionCache
	cacheTTLConfig      CacheTTLConfig
	scheduler           *queues.PriorityScheduler
	store               OperationStore
	pendingReplies      map[string]PendingReply
}

func NewRequestManager(dispatchTimeout time.Duration, cacheTTLConfig CacheTTLConfig, cache *TransactionCache, scheduler *queues.PriorityScheduler, store OperationStore) *DefaultRequestManager {
	if cache == nil {
		panic("cache cannot be nil")
	}
	if store == nil {
		panic("store cannot be nil")
	}
	return &DefaultRequestManager{
		dispatchTimeout:     dispatchTimeout,
		transactionsByRPCID: make(map[string]*Transaction),
		activeCloudRequests: make(map[string]string),
		cache:               cache,
		cacheTTLConfig:      cacheTTLConfig,
		scheduler:           scheduler,
		store:               store,
		pendingReplies:      make(map[string]PendingReply),
	}
}

// CanonicalRequestKey formats the session ID and raw JSON-RPC ID into a strongly-typed string.
func CanonicalRequestKey(sessionID string, id json.RawMessage) (string, error) {
	if len(id) == 0 || string(id) == "null" {
		// For notifications, use a generated UUID
		return fmt.Sprintf("%s:%s", sessionID, uuid.New().String()), nil
	}

	decoder := json.NewDecoder(bytes.NewReader(id))
	decoder.UseNumber()

	var parsed interface{}
	if err := decoder.Decode(&parsed); err != nil {
		return "", fmt.Errorf("invalid json-rpc id: %w", err)
	}

	switch v := parsed.(type) {
	case string:
		return fmt.Sprintf("%s:s:%s", sessionID, v), nil
	case json.Number:
		f, _, err := big.ParseFloat(v.String(), 10, 256, big.ToNearestEven)
		if err != nil {
			return "", fmt.Errorf("invalid json-rpc number: %w", err)
		}
		// Text('f', -1) correctly normalizes 1, 1.0, and 1e0 to "1" without losing large integer precision
		return fmt.Sprintf("%s:n:%s", sessionID, f.Text('f', -1)), nil
	default:
		return "", errors.New("json-rpc id must be string or number")
	}
}

func (m *DefaultRequestManager) CreateTransaction(sessionID string, cloudRPCID json.RawMessage, respondToCloud bool, method string, timeout time.Duration, isStateChanging bool) (*Transaction, error) {
	isNotification := len(cloudRPCID) == 0 || string(cloudRPCID) == "null"
	respondToCloud = respondToCloud && !isNotification

	reqKey, err := CanonicalRequestKey(sessionID, cloudRPCID)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Check cache (simplified for now, full impl in PR 3.2)
	if _, ok := m.cache.Get(reqKey); ok {
		// Should replay, but for PR 3.1 we just return busy for now
		return nil, ErrBusy
	}

	// 2. Check active map
	if _, active := m.activeCloudRequests[reqKey]; active {
		return nil, ErrBusy
	}

	// 3. Check state-changing rules
	if isStateChanging {
		if !respondToCloud || len(cloudRPCID) == 0 || string(cloudRPCID) == "null" {
			return nil, errors.New("state-changing commands must have a non-null ID")
		}

		// Note: We use TryLock here to avoid deadlocks. If we can't get it immediately, return busy.
		if !m.stateLock.TryLock() {
			return nil, ErrBusy
		}
	}

	// 4. Create transaction
	rpcID := uuid.New().String()
	tx := &Transaction{
		RPCID:            rpcID,
		CloudSessionID:   sessionID,
		CloudRPCID:       cloudRPCID,
		RequestKey:       reqKey,
		RespondToCloud:   respondToCloud,
		Method:           method,
		State:            TxCreated,
		CreatedAt:        time.Now(),
		TimeoutDuration:  timeout,
		DispatchDeadline: time.Now().Add(m.dispatchTimeout),
	}

	if isStateChanging {
		m.activeStateTx = rpcID
	}

	if respondToCloud {
		m.activeCloudRequests[reqKey] = rpcID
	}
	m.transactionsByRPCID[rpcID] = tx

	// Setup DispatchTimer
	tx.DispatchTimer = time.AfterFunc(m.dispatchTimeout, func() {
		m.dispatchTimeoutFail(rpcID)
	})

	return tx, nil
}

func (m *DefaultRequestManager) dispatchTimeoutFail(rpcID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return
	}

	// Only fail if it is still in a pre-flight state (not TxInFlight)
	if tx.State == TxCreated || tx.State == TxPreparingDispatch || tx.State == TxPendingPublish {
		m.terminalTransition(rpcID, TxFailed)
	}
}

func (m *DefaultRequestManager) MarkPreparingDispatch(rpcID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return ErrAlreadyTerminal
	}

	if tx.State != TxCreated {
		return ErrInvalidStateTransition
	}

	tx.State = TxPreparingDispatch
	return nil
}

func (m *DefaultRequestManager) MarkPendingPublish(rpcID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return ErrAlreadyTerminal
	}

	if tx.State != TxPreparingDispatch {
		return ErrInvalidStateTransition
	}

	tx.State = TxPendingPublish
	return nil
}

func (m *DefaultRequestManager) MarkInFlight(rpcID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return ErrAlreadyTerminal
	}

	if tx.State != TxPendingPublish {
		return ErrInvalidStateTransition
	}

	tx.State = TxInFlight

	// Process any buffered fast-reply that arrived while we were preparing or publishing
	if pending, ok := m.pendingReplies[rpcID]; ok {
		delete(m.pendingReplies, rpcID)
		return m.terminalTransition(rpcID, pending.State)
	}

	if tx.DispatchTimer != nil {
		tx.DispatchTimer.Stop()
	}

	// Start the downstream response timer
	tx.DispatchTimer = time.AfterFunc(tx.TimeoutDuration, func() {
		m.Timeout(rpcID)
	})

	return nil
}

// Complete moves a transaction to the TxCompleted state.
// Note: The `response` argument is currently unused. It is reserved for
// the TransactionCache implementation in Epic 3 (PR 3.2), which will cache this payload.
func (m *DefaultRequestManager) Complete(rpcID string, response []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return ErrAlreadyTerminal
	}

	isPreFlight := tx.State == TxCreated || tx.State == TxPreparingDispatch || tx.State == TxPendingPublish
	isHandoff := tx.Method == "upgrade" && m.activeStateTx != rpcID && m.activeStateTx != ""

	// Buffer fast replies that arrive before the request is officially in flight,
	// or during lock handoff disk I/O
	if isPreFlight || isHandoff {
		if m.pendingReplies == nil {
			m.pendingReplies = make(map[string]PendingReply)
		}
		m.pendingReplies[rpcID] = PendingReply{Payload: response, State: TxCompleted}
		return nil
	}

	// Cache logic would go here in PR 3.2
	return m.terminalTransition(rpcID, TxCompleted)
}

// ReleaseOperationLock releases the stateLock if it is currently held by the specified background operation.
// This is used by the NATS event subscriber or background timeout sweeper to free the router.
func (m *DefaultRequestManager) ReleaseOperationLock(operationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeStateTx != operationID {
		return errors.New("state lock is not held by the specified operation")
	}

	// Release the lock
	m.activeStateTx = ""
	m.stateLock.Unlock()

	return nil
}

// RespondAndRetain transfers stateLock ownership to a background operation and cleans up the transaction.
// Note: The `response` argument is currently unused. It is reserved for
// the TransactionCache implementation in Epic 3 (PR 3.2), which will cache this payload.
func (m *DefaultRequestManager) RespondAndRetain(rpcID string, response []byte) error {
	m.mu.Lock()

	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		m.mu.Unlock()
		return ErrAlreadyTerminal
	}

	if tx.Method != "upgrade" {
		m.mu.Unlock()
		return errors.New("only upgrade operations can be retained")
	}

	if tx.State != TxInFlight {
		m.mu.Unlock()
		return ErrInvalidStateTransition
	}

	if m.activeStateTx != rpcID {
		m.mu.Unlock()
		return errors.New("transaction does not own the state lock")
	}

	// 1. Pause response timer to prevent timeouts during disk I/O
	if tx.DispatchTimer != nil {
		tx.DispatchTimer.Stop()
	}

	// 2. Pre-transfer lock ownership to prevent fast-replies from unlocking it
	// We intentionally leave the transaction in the active maps so concurrent
	// terminal methods can find it and buffer their replies.
	operationID := uuid.New().String()
	m.activeStateTx = operationID
	m.mu.Unlock()

	// 3. Perform disk I/O outside the lock
	op := &PersistentOperation{
		OperationID: operationID,
		RPCID:       rpcID,
		CloudRPCID:  tx.CloudRPCID,
		Target:      "system",
		Action:      "upgrade",
		Stage:       "started",
		Status:      "started",
		Active:      true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := m.store.Save(ctx, op)

	m.mu.Lock()
	defer m.mu.Unlock()

	if err != nil {
		// Rollback lock ownership if disk fails
		if m.activeStateTx == operationID {
			m.activeStateTx = rpcID
		}

		// If a terminal event arrived while we were writing to disk, process it now!
		if pending, ok := m.pendingReplies[rpcID]; ok {
			delete(m.pendingReplies, rpcID)
			return m.terminalTransition(rpcID, pending.State)
		}

		// Restart timer
		tx.DispatchTimer = time.AfterFunc(tx.TimeoutDuration, func() {
			m.Timeout(rpcID)
		})
		return fmt.Errorf("failed to persist operation: %w", err)
	}

	// Cache logic would go here in PR 3.2

	// 4. Safely clean up via the standard terminal transition
	// If a buffered reply exists, process it with its proper state
	if pending, ok := m.pendingReplies[rpcID]; ok {
		delete(m.pendingReplies, rpcID)

		// If we are processing a buffered reply AFTER a successful save,
		// the lock is currently held by operationID, not rpcID.
		// We must explicitly release it since terminalTransition only releases rpcID locks.
		if m.activeStateTx == operationID {
			m.activeStateTx = ""
			m.stateLock.Unlock()
		}

		return m.terminalTransition(rpcID, pending.State)
	}

	return m.terminalTransition(rpcID, TxCompleted)
}

// Fail moves a transaction to the TxFailed state.
// Note: The `errResponse` argument is currently unused. It is reserved for
// the TransactionCache implementation in Epic 3 (PR 3.2), which will cache this payload.
func (m *DefaultRequestManager) Fail(rpcID string, errResponse []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return ErrAlreadyTerminal
	}

	isPreFlight := tx.State == TxCreated || tx.State == TxPreparingDispatch || tx.State == TxPendingPublish
	isHandoff := tx.Method == "upgrade" && m.activeStateTx != rpcID && m.activeStateTx != ""

	// Buffer fast replies that arrive before the request is officially in flight,
	// or during lock handoff disk I/O
	if isPreFlight || isHandoff {
		if m.pendingReplies == nil {
			m.pendingReplies = make(map[string]PendingReply)
		}
		m.pendingReplies[rpcID] = PendingReply{Payload: errResponse, State: TxFailed}
		return nil
	}

	return m.terminalTransition(rpcID, TxFailed)
}

func (m *DefaultRequestManager) Timeout(rpcID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return ErrAlreadyTerminal
	}

	isPreFlight := tx.State == TxCreated || tx.State == TxPreparingDispatch || tx.State == TxPendingPublish
	isHandoff := tx.Method == "upgrade" && m.activeStateTx != rpcID && m.activeStateTx != ""

	// Buffer timeouts that arrive before the request is officially in flight,
	// or during lock handoff disk I/O
	if isPreFlight || isHandoff {
		if m.pendingReplies == nil {
			m.pendingReplies = make(map[string]PendingReply)
		}
		m.pendingReplies[rpcID] = PendingReply{Payload: nil, State: TxTimedOut}
		return nil
	}

	return m.terminalTransition(rpcID, TxTimedOut)
}

func (m *DefaultRequestManager) terminalTransition(rpcID string, finalState TransactionState) error {
	tx, ok := m.transactionsByRPCID[rpcID]
	if !ok {
		// Since RPCIDs are internally generated UUIDs, a missing transaction
		// in a terminal call means it was already completed and removed.
		return ErrAlreadyTerminal
	}

	if tx.State == TxCompleted || tx.State == TxFailed || tx.State == TxTimedOut {
		return ErrAlreadyTerminal
	}

	// Enforce strict state machine transitions for terminal states
	if finalState == TxCompleted || finalState == TxTimedOut {
		if tx.State != TxInFlight {
			return ErrInvalidStateTransition
		}
	}

	tx.State = finalState

	if tx.DispatchTimer != nil {
		tx.DispatchTimer.Stop()
	}

	if tx.RespondToCloud {
		delete(m.activeCloudRequests, tx.RequestKey)
	}
	delete(m.transactionsByRPCID, rpcID)

	if m.activeStateTx == rpcID {
		m.activeStateTx = ""
		m.stateLock.Unlock()
	}

	return nil
}
