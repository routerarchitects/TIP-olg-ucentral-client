package reqmgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
)

var (
	ErrInvalidStateTransition     = errors.New("invalid state transition")
	ErrAlreadyTerminal            = errors.New("transaction already in terminal state")
	ErrBusy                       = errors.New("system busy or transaction active")
	ErrOperationNotActive         = errors.New("operation is not active")
	ErrOperationReleaseInProgress = errors.New("operation release already in progress")
	ErrOperationOwnershipChanged  = errors.New("operation ownership changed during release")
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
	if len(id) > 256 {
		return "", errors.New("json-rpc id exceeds maximum length")
	}

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

	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return "", errors.New("invalid json-rpc id: trailing content")
	}

	switch v := parsed.(type) {
	case string:
		return fmt.Sprintf("%s:s:%s", sessionID, v), nil
	case json.Number:
		r, ok := new(big.Rat).SetString(v.String())
		if !ok {
			return "", errors.New("invalid json-rpc number")
		}
		// RatString correctly normalizes 1, 1.0, and 1e0 to "1" with exact arithmetic
		return fmt.Sprintf("%s:n:%s", sessionID, r.RatString()), nil
	default:
		return "", errors.New("json-rpc id must be string or number")
	}
}

func (m *DefaultRequestManager) CreateTransaction(sessionID string, cloudRPCID json.RawMessage, respondToCloud bool, method string, timeout time.Duration, isStateChanging bool) (*Transaction, error) {
	isNotification := len(cloudRPCID) == 0 || string(cloudRPCID) == "null"
	respondToCloud = respondToCloud && !isNotification

	if method == "upgrade" && !isStateChanging {
		return nil, errors.New("upgrade must be state-changing")
	}

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

	// 4. Create transaction
	rpcID := uuid.New().String()

	// 3. Check state-changing rules
	if isStateChanging {
		if !respondToCloud || len(cloudRPCID) == 0 || string(cloudRPCID) == "null" {
			return nil, errors.New("state-changing commands must have a non-null ID")
		}

		if m.activeStateTx != "" {
			return nil, ErrBusy
		}

		// Note: We use TryLock here to avoid deadlocks. If we can't get it immediately, return busy.
		if !m.stateLock.TryLock() {
			return nil, ErrBusy
		}
		m.activeStateTx = rpcID
	}

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
		if pending, ok := m.pendingReplies[rpcID]; ok {
			delete(m.pendingReplies, rpcID)
			// The hardware successfully responded, which proves the command was published.
			// Catch up the local state machine so terminalTransition accepts it.
			tx.State = TxInFlight
			m.terminalTransition(rpcID, pending.State, pending.Payload)
		} else {
			m.terminalTransition(rpcID, TxFailed, nil)
		}
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
		return m.terminalTransition(rpcID, pending.State, pending.Payload)
	}

	if tx.DispatchTimer != nil {
		tx.DispatchTimer.Stop()
	}

	// Start the downstream response timer
	tx.ResponseDeadline = time.Now().Add(tx.TimeoutDuration)
	tx.DispatchTimer = time.AfterFunc(tx.TimeoutDuration, func() {
		m.Timeout(rpcID)
	})

	return nil
}

// Complete marks a transaction as successfully completed.
// STRICT CONTRACT: Complete MUST NOT be called for the intermediate acknowledgement of an
// asynchronous state-changing operation (e.g. upgrade). Those must be handled exclusively by
// RespondAndRetain. Complete must only be called for the final terminal result.
// If Complete is called during a pre-flight state (before MarkInFlight),
// it assumes it is a downstream fast-reply and buffers the success.
func (m *DefaultRequestManager) Complete(rpcID string, response []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return ErrAlreadyTerminal
	}

	isPreFlight := tx.State == TxCreated || tx.State == TxPreparingDispatch || tx.State == TxPendingPublish
	// Determine if this transaction is actively undergoing a lock handoff
	isHandoff := tx.HandoffInProgress

	// Buffer fast replies that arrive before the request is officially in flight,
	// or during lock handoff disk I/O
	if isPreFlight || isHandoff {
		if m.pendingReplies == nil {
			m.pendingReplies = make(map[string]PendingReply)
		} else if _, exists := m.pendingReplies[rpcID]; exists {
			return ErrAlreadyTerminal
		}
		m.pendingReplies[rpcID] = PendingReply{Payload: response, State: TxCompleted}
		return nil
	}

	// Cache logic would go here in PR 3.2
	return m.terminalTransition(rpcID, TxCompleted, response)
}

// ReleaseOperationLock releases the stateLock if it is currently held by the specified background operation.
func (m *DefaultRequestManager) ReleaseOperationLock(ctx context.Context, operationID string) error {
	m.mu.Lock()
	if m.activeStateTx != operationID {
		// If another worker is already deleting it, we cannot return success yet because
		// their deletion might fail. Return a distinct error so the caller knows it is pending.
		if m.activeStateTx == "deleting:"+operationID {
			m.mu.Unlock()
			return ErrOperationReleaseInProgress
		}
		m.mu.Unlock()
		return ErrOperationNotActive
	}

	// Mark it as actively being deleted so concurrent callers return ErrOperationReleaseInProgress
	m.activeStateTx = "deleting:" + operationID
	m.mu.Unlock()

	// Perform disk I/O outside lock
	if err := m.store.Delete(ctx, operationID); err != nil {
		m.mu.Lock()
		// Roll back the state if deletion failed so it can be retried later
		// Update local state to point to the new background operation ID
		if m.activeStateTx == "deleting:"+operationID {
			m.activeStateTx = operationID
		}
		m.mu.Unlock()
		return fmt.Errorf("failed to delete persistent operation: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Re-check after I/O
	if m.activeStateTx != "deleting:"+operationID {
		return ErrOperationOwnershipChanged
	}
	m.activeStateTx = ""
	m.stateLock.Unlock()
	return nil
}

// RespondAndRetain transfers stateLock ownership to a background operation and cleans up the transaction.
// Note: The `response` argument is currently unused. It is reserved for
// the TransactionCache implementation in Epic 3 (PR 3.2), which will cache this payload.
func (m *DefaultRequestManager) RespondAndRetain(ctx context.Context, rpcID string, response []byte) (string, error) {
	m.mu.Lock()

	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		m.mu.Unlock()
		return "", ErrAlreadyTerminal
	}

	if tx.Method != "upgrade" {
		m.mu.Unlock()
		return "", errors.New("only upgrade operations can be retained")
	}

	if tx.State != TxInFlight {
		m.mu.Unlock()
		return "", ErrInvalidStateTransition
	}

	if m.activeStateTx != rpcID {
		m.mu.Unlock()
		return "", errors.New("transaction does not own the state lock")
	}

	// 1. Pause response timer to prevent timeouts during disk I/O
	if tx.DispatchTimer != nil {
		tx.DispatchTimer.Stop()
	}

	// 2. Pre-transfer lock ownership to prevent fast-replies from unlocking it
	// We intentionally leave the transaction in the active maps so concurrent
	// terminal methods can find it and buffer their replies.
	operationID := uuid.New().String()
	tx.HandoffInProgress = true
	m.activeStateTx = operationID
	m.mu.Unlock()

	// 3. Perform disk I/O outside the lock
	now := time.Now().UTC().Format(time.RFC3339)
	op := &PersistentOperation{
		OperationID: operationID,
		RPCID:       rpcID,
		CloudRPCID:  tx.CloudRPCID,
		Target:      "system",
		Action:      "upgrade",
		Stage:       "started",
		Status:      "started",
		Active:      true,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	saveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := m.store.Save(saveCtx, op)

	m.mu.Lock()
	defer m.mu.Unlock()

	if err != nil {
		tx.HandoffInProgress = false
		// Rollback lock ownership if disk fails
		if m.activeStateTx == operationID {
			m.activeStateTx = rpcID
		}

		// If a terminal event arrived while we were writing to disk, process it now!
		if pending, ok := m.pendingReplies[rpcID]; ok {
			delete(m.pendingReplies, rpcID)
			return "", m.terminalTransition(rpcID, pending.State, pending.Payload)
		}

		// Restart timer
		remaining := time.Until(tx.ResponseDeadline)
		if remaining <= 0 {
			return "", m.terminalTransition(rpcID, TxTimedOut, nil)
		}

		tx.DispatchTimer = time.AfterFunc(remaining, func() {
			_ = m.Timeout(rpcID)
		})
		return "", fmt.Errorf("failed to persist operation: %w", err)
	}

	// Cache logic would go here in PR 3.2

	// 4. Safely clean up via the standard terminal transition
	// If a buffered reply exists, process it with its proper state
	if pending, ok := m.pendingReplies[rpcID]; ok {
		// If we are processing a buffered reply AFTER a successful save,
		// the lock is currently held by operationID, not rpcID.
		// We must explicitly release it since terminalTransition only releases rpcID locks.
		if m.activeStateTx == operationID {
			m.mu.Unlock()

			ctxDelete, cancelDelete := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelDelete()
			errDelete := m.ReleaseOperationLock(ctxDelete, operationID)

			m.mu.Lock()

			if errDelete != nil && errDelete != ErrOperationNotActive {
				// The active record was NOT deleted from the database!
				// Process the buffered terminal transition locally so the correct result is
				// recorded, but LEAVE the memory lock held by operationID.
				// STRICT CALLER CONTRACT: The caller MUST reliably capture the returned operationID
				// and schedule a background retry to eventually call ReleaseOperationLock(operationID).
				// Failure to do so will leave the device permanently locked and busy!
				// (Note: The actual caller implementing this retry mechanism will be introduced in subsequent PRs).
				delete(m.pendingReplies, rpcID)
				_ = m.terminalTransition(rpcID, pending.State, pending.Payload)

				return operationID, errDelete
			}
		}

		delete(m.pendingReplies, rpcID)
		return "", m.terminalTransition(rpcID, pending.State, pending.Payload)
	}

	return operationID, m.terminalTransition(rpcID, TxCompleted, response)
}

// Fail marks a transaction as failed.
// Unlike Complete, Fail does NOT buffer during standard pre-flight states.
// It is permitted to immediately terminate a pre-flight transaction
// to handle internal local-dispatch errors (e.g. payload validation failures).
// It only buffers during an active RespondAndRetain lock handoff.
func (m *DefaultRequestManager) Fail(rpcID string, errResponse []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return ErrAlreadyTerminal
	}

	isHandoff := tx.HandoffInProgress

	// Buffer fast replies that arrive during lock handoff disk I/O
	if isHandoff {
		if m.pendingReplies == nil {
			m.pendingReplies = make(map[string]PendingReply)
		} else if _, exists := m.pendingReplies[rpcID]; exists {
			return ErrAlreadyTerminal
		}
		m.pendingReplies[rpcID] = PendingReply{Payload: errResponse, State: TxFailed}
		return nil
	}

	return m.terminalTransition(rpcID, TxFailed, errResponse)
}

// Timeout marks a transaction as timed out.
// Like Fail, it does not buffer during standard pre-flight states,
// as timeouts are internally generated by the dispatch timer.
// It only buffers during an active RespondAndRetain lock handoff.
func (m *DefaultRequestManager) Timeout(rpcID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return ErrAlreadyTerminal
	}

	isHandoff := tx.HandoffInProgress

	// Buffer timeout that arrives during lock handoff disk I/O
	if isHandoff {
		if m.pendingReplies == nil {
			m.pendingReplies = make(map[string]PendingReply)
		} else if _, exists := m.pendingReplies[rpcID]; exists {
			return ErrAlreadyTerminal
		}
		m.pendingReplies[rpcID] = PendingReply{Payload: nil, State: TxTimedOut}
		return nil
	}

	return m.terminalTransition(rpcID, TxTimedOut, nil)
}

func (m *DefaultRequestManager) terminalTransition(rpcID string, finalState TransactionState, payload []byte) error {
	switch finalState {
	case TxCompleted, TxFailed, TxTimedOut:
	default:
		return ErrInvalidStateTransition
	}

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
	tx.Payload = payload

	if tx.DispatchTimer != nil {
		tx.DispatchTimer.Stop()
	}

	if m.pendingReplies != nil {
		delete(m.pendingReplies, rpcID)
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
