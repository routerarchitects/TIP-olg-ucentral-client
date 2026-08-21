package reqmgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
)

var (
	ErrInvalidStateTransition     = errors.New("invalid state transition")
	ErrAlreadyTerminal            = errors.New("transaction already in terminal state")
	ErrTransactionNotFound        = errors.New("transaction not found")
	ErrDuplicateRequest           = errors.New("duplicate request already in progress")
	ErrStateLockBusy              = errors.New("device is currently executing a state-changing operation")
	ErrOperationNotActive         = errors.New("operation is not active")
	ErrOperationReleaseInProgress = errors.New("operation release already in progress")
	ErrOperationOwnershipChanged  = errors.New("operation ownership changed during release")
	ErrCapacityExceeded           = errors.New("request manager capacity exceeded")
)

type PendingReply struct {
	Payload []byte
	State   TransactionState
}

type LockOwnerState int

const (
	LockNone LockOwnerState = iota
	LockOwnedByRPC
	LockTransferPending
	LockOwnedByOperation
)

type DefaultRequestManager struct {
	mu                    sync.Mutex
	dispatchTimeout       time.Duration
	transactionsByRPCID   map[string]*Transaction
	activeCloudRequests   map[string]string // Key: RequestKey, Value: RPCID
	stateLock             sync.Mutex
	activeStateTx         string         // RPCID or OperationID holding the state lock
	activeStateOwner      LockOwnerState // Explicitly tracks ownership phase
	releasingOperationID  string         // Operation ID currently being deleted
	cache                 *TransactionCache
	cacheTTLConfig        CacheTTLConfig
	scheduler             *queues.PriorityScheduler
	store                 OperationStore
	pendingReplies        map[string]PendingReply
	maxConcurrentRequests int
	sweeperTTL            time.Duration
	activeRecordLimit     int
}

func NewRequestManager(dispatchTimeout time.Duration, cacheTTLConfig CacheTTLConfig, cache *TransactionCache, scheduler *queues.PriorityScheduler, store OperationStore, maxConcurrentRequests int, sweeperTTL time.Duration, activeRecordLimit int) (*DefaultRequestManager, error) {
	if cache == nil {
		return nil, errors.New("cache cannot be nil")
	}
	if store == nil {
		return nil, fmt.Errorf("store cannot be nil")
	}

	return &DefaultRequestManager{
		dispatchTimeout:       dispatchTimeout,
		transactionsByRPCID:   make(map[string]*Transaction),
		activeCloudRequests:   make(map[string]string),
		cache:                 cache,
		cacheTTLConfig:        cacheTTLConfig,
		scheduler:             scheduler,
		store:                 store,
		pendingReplies:        make(map[string]PendingReply),
		maxConcurrentRequests: maxConcurrentRequests,
		sweeperTTL:            sweeperTTL,
		activeRecordLimit:     activeRecordLimit,
	}, nil
}

// CanonicalRequestKey generates a normalized, deterministic string key for a transaction
// based solely on the Cloud RPC ID and the Cloud Session ID.
//
// NOTE: This explicitly omits the method name to enforce a strict design constraint:
// A JSON-RPC ID must be globally unique for the lifetime of a cloud session, regardless of method.
// Attempting to reuse an ID with a different method will intentionally trigger duplicate rejection.
//
// NOTE: Numeric IDs are mathematically normalized (e.g., 1, 1.0, 1e0 all become 1)
// to ensure duplicate tracking correctly handles visually different but mathematically
// identical inputs. The JSON-RPC specification requires IDs to be matched exactly, but
// some legacy controllers may mutate representations. This normalization prevents duplicates.
func CanonicalRequestKey(sessionID string, id json.RawMessage) (string, error) {
	if len(id) > 256 {
		return "", errors.New("json-rpc id exceeds maximum length")
	}

	if len(id) == 0 || string(id) == "null" {
		return "", nil
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
	if isNotification && respondToCloud {
		return nil, errors.New("cannot request response for a notification (null/empty ID)")
	}

	if method == "upgrade" && !isStateChanging {
		return nil, errors.New("upgrade must be state-changing")
	}

	reqKey, err := CanonicalRequestKey(sessionID, cloudRPCID)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Check cache for duplicates of already completed requests
	if cachedPayload, ok := m.cache.Get(reqKey); ok {
		return nil, &CachedResponseError{Payload: cachedPayload}
	}

	// 2. Check active map
	if _, active := m.activeCloudRequests[reqKey]; active {
		return nil, ErrDuplicateRequest
	}

	// 2.5 Check global concurrency capacity
	if len(m.transactionsByRPCID) >= m.maxConcurrentRequests {
		return nil, ErrCapacityExceeded
	}

	// 4. Create transaction
	rpcID := uuid.New().String()

	// 3. Check state-changing rules
	if isStateChanging {
		if !respondToCloud || isNotification {
			return nil, errors.New("state-changing commands must have a non-null ID")
		}

		// Note: We use TryLock here to avoid deadlocks. If we can't get it immediately, return busy.
		if !m.stateLock.TryLock() {
			return nil, ErrStateLockBusy
		}
		m.activeStateTx = rpcID
		m.activeStateOwner = LockOwnedByRPC
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

	hasValidID := !isNotification && reqKey != ""
	if hasValidID {
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
			if err := m.recoverToInFlight(tx); err == nil {
				m.terminalTransition(rpcID, pending.State, pending.Payload)
			} else {
				m.terminalTransition(rpcID, TxFailed, nil)
			}
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
		return ErrTransactionNotFound
	}

	if err := validateTransition(tx.State, TxPreparingDispatch); err != nil {
		return err
	}

	tx.State = TxPreparingDispatch
	return nil
}

func (m *DefaultRequestManager) MarkPendingPublish(rpcID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return ErrTransactionNotFound
	}

	if err := validateTransition(tx.State, TxPendingPublish); err != nil {
		return err
	}

	tx.State = TxPendingPublish
	return nil
}

func (m *DefaultRequestManager) MarkInFlight(rpcID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return ErrTransactionNotFound
	}

	if err := validateTransition(tx.State, TxInFlight); err != nil {
		return err
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
	tx.ResponseTimer = time.AfterFunc(tx.TimeoutDuration, func() {
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
		return ErrTransactionNotFound
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
		m.pendingReplies[rpcID] = PendingReply{Payload: bytes.Clone(response), State: TxCompleted}
		return nil
	}
	return m.terminalTransition(rpcID, TxCompleted, response)
}

func (m *DefaultRequestManager) ReleaseOperationLock(ctx context.Context, operationID string) error {
	if operationID == "" {
		return ErrOperationNotActive
	}

	m.mu.Lock()

	if m.releasingOperationID == operationID {
		m.mu.Unlock()
		return ErrOperationReleaseInProgress
	}

	if m.activeStateTx != operationID || m.activeStateOwner != LockOwnedByOperation {
		m.mu.Unlock()
		return ErrOperationNotActive
	}

	m.releasingOperationID = operationID
	m.activeStateTx = ""
	m.activeStateOwner = LockNone
	m.stateLock.Unlock()
	m.mu.Unlock()

	err := m.store.Delete(ctx, operationID)
	if err != nil {
		log.Printf("reqmgr: ReleaseOperationLock failed to delete operation %s: %v", operationID, err)
	}

	m.mu.Lock()
	if m.releasingOperationID == operationID {
		m.releasingOperationID = ""
	}
	m.mu.Unlock()

	return err
}

// RespondAndRetain transfers stateLock ownership to a background operation and cleans up the transaction.
// Note: The response payload is passed through the terminal transition and stored by the
// transaction cache for completed requests.
func (m *DefaultRequestManager) RespondAndRetain(ctx context.Context, rpcID string, response []byte) (string, error) {
	m.mu.Lock()

	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		m.mu.Unlock()
		return "", ErrTransactionNotFound
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
	if tx.ResponseTimer != nil {
		tx.ResponseTimer.Stop()
	}

	// 2. Mark handoff pending to buffer concurrent terminal replies
	operationID := uuid.New().String()
	tx.HandoffInProgress = true
	m.activeStateOwner = LockTransferPending
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
		log.Printf("reqmgr: RespondAndRetain failed to persist operation %s: %v", operationID, err)
		tx.HandoffInProgress = false
		// NOTE: The caller is expected to handle this failure (e.g., by retrying the
		// RespondAndRetain process or failing the upgrade). The robust retry logic
		// for the caller will be introduced by us in a subsequent PR.

		// Ensure no concurrent terminal transition mutated state while we were saving
		if m.activeStateTx == rpcID {
			m.activeStateOwner = LockOwnedByRPC
		}

		// If a terminal event arrived while we were writing to disk, process it now!
		if pending, ok := m.pendingReplies[rpcID]; ok {
			delete(m.pendingReplies, rpcID)
			return "", m.terminalTransition(rpcID, pending.State, pending.Payload)
		}

		// Restart timer
		remaining := time.Until(tx.ResponseDeadline)
		if remaining <= 0 {
			_ = m.terminalTransition(rpcID, TxTimedOut, nil)
			return "", ErrAlreadyTerminal
		}
		tx.ResponseTimer = time.AfterFunc(remaining, func() {
			m.Timeout(rpcID)
		})

		return "", fmt.Errorf("failed to persist background operation: %w", err)
	}

	// 4. Persistence succeeded. Complete the transfer.
	if m.activeStateTx == rpcID {
		m.activeStateTx = operationID
		m.activeStateOwner = LockOwnedByOperation
	}

	// 4. Safely clean up via the standard terminal transition
	// If a buffered reply exists, process it with its proper state
	if pending, ok := m.pendingReplies[rpcID]; ok {
		// If we are processing a buffered reply AFTER a successful save,
		// the lock is currently held by operationID, not rpcID.
		// We must explicitly release it since terminalTransition only releases rpcID locks.
		if m.activeStateTx == operationID {
			m.mu.Unlock()

			ctxDelete, cancelDelete := context.WithTimeout(ctx, 5*time.Second)
			defer cancelDelete()
			errDelete := m.ReleaseOperationLock(ctxDelete, operationID)

			m.mu.Lock()

			if errDelete != nil {
				log.Printf("reqmgr: failed to safely release persistent record for operation %s (in-memory lock was still released): %v", operationID, errDelete)
				// The active record was NOT deleted from the database, but ReleaseOperationLock
				// guarantees that it WAS successfully unlocked in memory!
				// The background sweeper will eventually clean up the stale DB record.
				// We can safely return success to the caller.
			}

			delete(m.pendingReplies, rpcID)
			return "", m.terminalTransition(rpcID, pending.State, pending.Payload)
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
		return ErrTransactionNotFound
	}

	isHandoff := tx.HandoffInProgress

	if !isHandoff {
		// Check if a terminal reply was already buffered
		if pending, ok := m.pendingReplies[rpcID]; ok {
			delete(m.pendingReplies, rpcID)
			if err := m.recoverToInFlight(tx); err == nil {
				_ = m.terminalTransition(rpcID, pending.State, pending.Payload)
				return ErrAlreadyTerminal
			}
			// If recovery fails (e.g. TxCreated), discard the invalid fast-reply
			// and fall through to native failure.
		}
	}

	// Buffer fast replies that arrive during lock handoff disk I/O
	if isHandoff {
		if m.pendingReplies == nil {
			m.pendingReplies = make(map[string]PendingReply)
		} else if _, exists := m.pendingReplies[rpcID]; exists {
			return ErrAlreadyTerminal
		}
		m.pendingReplies[rpcID] = PendingReply{Payload: bytes.Clone(errResponse), State: TxFailed}
		return nil
	}

	return m.terminalTransition(rpcID, TxFailed, bytes.Clone(errResponse))
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
		return ErrTransactionNotFound
	}

	isHandoff := tx.HandoffInProgress

	if !isHandoff {
		// Check if a terminal reply was already buffered
		if pending, ok := m.pendingReplies[rpcID]; ok {
			delete(m.pendingReplies, rpcID)
			if err := m.recoverToInFlight(tx); err == nil {
				_ = m.terminalTransition(rpcID, pending.State, pending.Payload)
				return ErrAlreadyTerminal
			}
			// If recovery fails (e.g. TxCreated), discard the invalid fast-reply
			// and fall through to native timeout.
		}
	}

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
		return ErrTransactionNotFound
	}

	if err := validateTransition(tx.State, finalState); err != nil {
		return err
	}

	tx.State = finalState
	tx.Payload = payload

	if finalState == TxCompleted && tx.RespondToCloud && payload != nil {
		ttl := m.cacheTTLConfig.TTLForMethod(tx.Method)
		m.cache.Set(tx.RequestKey, payload, ttl)
	}

	if tx.DispatchTimer != nil {
		tx.DispatchTimer.Stop()
		tx.DispatchTimer = nil
	}
	if tx.ResponseTimer != nil {
		tx.ResponseTimer.Stop()
		tx.ResponseTimer = nil
	}

	if m.pendingReplies != nil {
		delete(m.pendingReplies, rpcID)
	}

	hasValidID := tx.RequestKey != ""
	if hasValidID {
		delete(m.activeCloudRequests, tx.RequestKey)
	}
	delete(m.transactionsByRPCID, rpcID)

	releaseStateLock := false
	if m.activeStateTx == rpcID {
		m.activeStateTx = ""
		m.activeStateOwner = LockNone
		releaseStateLock = true
	}

	if releaseStateLock {
		m.stateLock.Unlock()
	}

	return nil
}

// Start runs the background sweepers to clean up orphaned persistent operations and expired cache entries.
func (m *DefaultRequestManager) Start(ctx context.Context) {
	m.cache.StartCacheSweeper(ctx, 1*time.Minute)

	if ops, err := m.store.GetActive(ctx, m.activeRecordLimit); err == nil {
		m.mu.Lock()
		now := time.Now().UTC()
		for _, op := range ops {
			updatedAt, errTime := time.Parse(time.RFC3339, op.UpdatedAt)

			// If the timestamp is missing/malformed, treat it as expired to avoid deadlocks.
			isExpired := true
			if errTime == nil {
				isExpired = now.Sub(updatedAt) > m.sweeperTTL
			} else {
				log.Printf("reqmgr: Start() encountered invalid timestamp for operation %s, treating as expired: %v", op.OperationID, errTime)
			}

			if m.activeStateTx == "" {
				if !isExpired {
					// A background operation was running when we crashed.
					// Re-acquire the memory lock to protect the device until it finishes!
					if m.stateLock.TryLock() {
						m.activeStateTx = op.OperationID
						m.activeStateOwner = LockOwnedByOperation
					}
				}
			}
		}
		m.mu.Unlock()
	} else {
		log.Printf("reqmgr: Start() failed to load active operations from store: %v", err)
	}

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		// Run once on startup to reconcile state immediately (e.g. clean stale DB records)
		m.sweepOrphanedOperations(ctx)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.sweepOrphanedOperations(ctx)
			}
		}
	}()
}

// sweepOrphanedOperations periodically attempts to clean up operations that failed to delete.
func (m *DefaultRequestManager) sweepOrphanedOperations(ctx context.Context) {
	ops, err := m.store.GetActive(ctx, m.activeRecordLimit)
	if err != nil {
		log.Printf("reqmgr: sweeper failed to read active operations: %v", err)
		return
	}

	now := time.Now().UTC()

	for _, op := range ops {
		updatedAt, errTime := time.Parse(time.RFC3339, op.UpdatedAt)

		// If the timestamp is missing/malformed, treat it as expired to avoid deadlocks.
		isExpired := true
		if errTime == nil {
			isExpired = now.Sub(updatedAt) > m.sweeperTTL
		} else {
			log.Printf("reqmgr: sweeper encountered invalid timestamp for operation %s, treating as expired: %v", op.OperationID, errTime)
		}

		m.mu.Lock()
		isActive := (m.activeStateTx == op.OperationID)

		// 1. If the operation has exceeded the maximum 15-minute TTL, force kill it
		if isExpired {
			if isActive {
				m.activeStateTx = ""
				m.activeStateOwner = LockNone
				if m.releasingOperationID == op.OperationID {
					m.releasingOperationID = ""
				}
				m.stateLock.Unlock()
			}
			m.mu.Unlock()

			// Delete the stale record from the database
			if err := m.store.Delete(ctx, op.OperationID); err != nil {
				log.Printf("reqmgr: sweeper failed to durably delete expired operation %s: %v", op.OperationID, err)
			}
			continue
		}

		// 2. If it's NOT active in memory (meaning it successfully unlocked earlier but the DB delete failed),
		// the sweeper directly cleans up the orphaned DB record.
		if !isActive {
			m.mu.Unlock()
			if err := m.store.Delete(ctx, op.OperationID); err != nil {
				log.Printf("reqmgr: sweeper failed to delete orphaned operation %s: %v", op.OperationID, err)
			}
			continue
		}

		// 3. Otherwise, it is active and not expired. Leave it safely alone.
		m.mu.Unlock()
	}
}

// recoverToInFlight explicitly promotes a pre-flight transaction to TxInFlight
// if a downstream fast-reply proves it was published. It strictly rejects TxCreated
// because a response for an unprepared transaction indicates an invalid dispatch sequence.
func (m *DefaultRequestManager) recoverToInFlight(tx *Transaction) error {
	if err := validateTransition(tx.State, TxInFlight); err != nil {
		return err
	}
	tx.State = TxInFlight
	return nil
}

func (m *DefaultRequestManager) GetTransaction(rpcID string) (*Transaction, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, exists := m.transactionsByRPCID[rpcID]
	if !exists {
		return nil, false
	}
	return tx.Clone(), true
}
