package reqmgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	pendingReplies      map[string][]byte
}

func NewRequestManager(dispatchTimeout time.Duration, cacheTTLConfig CacheTTLConfig, cache *TransactionCache, scheduler *queues.PriorityScheduler, store OperationStore) *DefaultRequestManager {
	return &DefaultRequestManager{
		dispatchTimeout:     dispatchTimeout,
		transactionsByRPCID: make(map[string]*Transaction),
		activeCloudRequests: make(map[string]string),
		cache:               cache,
		cacheTTLConfig:      cacheTTLConfig,
		scheduler:           scheduler,
		store:               store,
		pendingReplies:      make(map[string][]byte),
	}
}

// CanonicalRequestKey formats the session ID, method, and raw JSON-RPC ID into a strongly-typed string.
func CanonicalRequestKey(sessionID string, method string, id json.RawMessage) (string, error) {
	if len(id) == 0 || string(id) == "null" {
		// For notifications, use a generated UUID
		return fmt.Sprintf("%s:%s:%s", sessionID, method, uuid.New().String()), nil
	}
	return fmt.Sprintf("%s:%s:%s", sessionID, method, string(id)), nil
}

func (m *DefaultRequestManager) CreateTransaction(sessionID string, cloudRPCID json.RawMessage, respondToCloud bool, method string, timeout time.Duration, isStateChanging bool) (*Transaction, error) {
	reqKey, err := CanonicalRequestKey(sessionID, method, cloudRPCID)
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
		if !respondToCloud {
			return nil, errors.New("state changing commands must have an ID")
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
		m.Fail(rpcID, []byte(`{"error":{"code":-32603,"message":"dispatch timeout"}}`))
	})

	return tx, nil
}

func (m *DefaultRequestManager) MarkPreparingDispatch(rpcID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.transactionsByRPCID[rpcID]
	if !ok {
		return errors.New("transaction not found")
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

	tx, ok := m.transactionsByRPCID[rpcID]
	if !ok {
		return errors.New("transaction not found")
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

	tx, ok := m.transactionsByRPCID[rpcID]
	if !ok {
		return errors.New("transaction not found")
	}

	if tx.State == TxFailed || tx.State == TxTimedOut || tx.State == TxCompleted {
		return ErrAlreadyTerminal
	}

	if tx.State != TxPendingPublish {
		return ErrInvalidStateTransition
	}

	tx.State = TxInFlight
	if tx.DispatchTimer != nil {
		tx.DispatchTimer.Stop()
	}

	// Start the downstream response timer
	tx.DispatchTimer = time.AfterFunc(tx.TimeoutDuration, func() {
		m.Timeout(rpcID)
	})

	return nil
}

func (m *DefaultRequestManager) Complete(rpcID string, response []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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

	// 1. Pause response timer to prevent timeouts during disk I/O
	if tx.DispatchTimer != nil {
		tx.DispatchTimer.Stop()
	}

	// 2. Pre-transfer lock ownership to prevent fast-replies from unlocking it
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

	if err != nil {
		// Rollback lock ownership if disk fails
		m.mu.Lock()
		if m.activeStateTx == operationID {
			m.activeStateTx = rpcID
		}
		// Restart timer
		tx.DispatchTimer = time.AfterFunc(tx.TimeoutDuration, func() {
			m.Timeout(rpcID)
		})
		m.mu.Unlock()
		return fmt.Errorf("failed to persist operation: %w", err)
	}

	// Cache logic would go here in PR 3.2

	// 4. Safely clean up via the standard terminal transition
	return m.terminalTransition(rpcID, TxCompleted)
}

func (m *DefaultRequestManager) Fail(rpcID string, errResponse []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.terminalTransition(rpcID, TxFailed)
}

func (m *DefaultRequestManager) Timeout(rpcID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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
