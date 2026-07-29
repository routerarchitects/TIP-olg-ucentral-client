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
		m.Timeout(rpcID) // simplified for now
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

	// For PR 3.1, this is a basic stub of MarkInFlight
	return nil
}

func (m *DefaultRequestManager) Complete(rpcID string, response []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Add to cache (simplified)
	return m.terminalTransition(rpcID, TxCompleted)
}

func (m *DefaultRequestManager) RespondAndRetain(rpcID string, response []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.transactionsByRPCID[rpcID]
	if !ok {
		return ErrAlreadyTerminal
	}

	if tx.Method != "upgrade" {
		return errors.New("only upgrade operations can be retained")
	}

	operationID := uuid.New().String()
	op := &PersistentOperation{
		OperationID: operationID,
		RPCID:       rpcID,
		CloudRPCID:  tx.CloudRPCID,
		Target:      tx.CloudSessionID,
		Action:      tx.Method,
		Stage:       "started",
		Status:      "active",
		Active:      true,
		CreatedAt:   time.Now().Format(time.RFC3339),
		UpdatedAt:   time.Now().Format(time.RFC3339),
	}

	if err := m.store.Save(context.Background(), op); err != nil {
		return err
	}

	// Transfer lock ownership
	if m.activeStateTx == rpcID {
		m.activeStateTx = operationID
	}

	// Cache the "started" response
	m.cache.Set(tx.RequestKey, response, m.cacheTTLConfig.TTLForMethod(tx.Method))

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
