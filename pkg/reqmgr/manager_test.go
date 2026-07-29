package reqmgr

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
)

// mockStore for testing
type mockStore struct{}

func (s *mockStore) Save(ctx context.Context, operation *PersistentOperation) error { return nil }
func (s *mockStore) Get(ctx context.Context, operationID string) (*PersistentOperation, error) {
	return nil, nil
}
func (s *mockStore) GetActive(ctx context.Context) (*PersistentOperation, error) { return nil, nil }
func (s *mockStore) GetPendingTerminalDelivery(ctx context.Context) ([]*PersistentOperation, error) {
	return nil, nil
}
func (s *mockStore) Delete(ctx context.Context, operationID string) error { return nil }

func setupTestManager() *DefaultRequestManager {
	cache := NewTransactionCache()
	config := CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &mockStore{}
	return NewRequestManager(10*time.Second, config, cache, scheduler, store)
}

func TestTCRM001_StateMachineTransitions(t *testing.T) {
	m := setupTestManager()

	cloudRPCID := json.RawMessage(`"tx-1"`)
	tx, err := m.CreateTransaction("session-1", cloudRPCID, true, "action", 10*time.Second, false)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	if tx.State != TxCreated {
		t.Fatalf("expected state TxCreated, got %v", tx.State)
	}

	err = m.MarkPreparingDispatch(tx.RPCID)
	if err != nil {
		t.Fatalf("failed to transition to TxPreparingDispatch: %v", err)
	}

	// Verify illegal jump: TxPreparingDispatch -> TxCompleted
	err = m.Complete(tx.RPCID, []byte("success"))
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition for Complete from TxPreparingDispatch, got %v", err)
	}

	// Verify illegal jump: TxPreparingDispatch -> TxTimedOut
	err = m.Timeout(tx.RPCID)
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition for Timeout from TxPreparingDispatch, got %v", err)
	}

	if tx.State != TxPreparingDispatch {
		t.Fatalf("expected state TxPreparingDispatch, got %v", tx.State)
	}

	err = m.MarkPendingPublish(tx.RPCID)
	if err != nil {
		t.Fatalf("failed to transition to TxPendingPublish: %v", err)
	}
	if tx.State != TxPendingPublish {
		t.Fatalf("expected state TxPendingPublish, got %v", tx.State)
	}

	err = m.MarkInFlight(tx.RPCID)
	if err != nil {
		t.Fatalf("failed to transition to TxInFlight: %v", err)
	}
	if tx.State != TxInFlight {
		t.Fatalf("expected state TxInFlight, got %v", tx.State)
	}

	err = m.Complete(tx.RPCID, []byte("success"))
	if err != nil {
		t.Fatalf("failed to transition to TxCompleted: %v", err)
	}
	if tx.State != TxCompleted {
		t.Fatalf("expected state TxCompleted, got %v", tx.State)
	}

	// Try an illegal transition
	err = m.Fail(tx.RPCID, []byte("fail"))
	if err != ErrAlreadyTerminal {
		t.Fatalf("expected ErrAlreadyTerminal, got %v", err)
	}
}

func TestTCRM002_ConcurrencyRejection(t *testing.T) {
	m := setupTestManager()

	cloudRPCID1 := json.RawMessage(`"tx-1"`)
	cloudRPCID2 := json.RawMessage(`"tx-2"`)

	// Start a state-changing transaction
	tx1, err := m.CreateTransaction("session-1", cloudRPCID1, true, "action", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create tx1: %v", err)
	}
	if tx1.State != TxCreated {
		t.Fatalf("expected state TxCreated, got %v", tx1.State)
	}

	// Try to start another state-changing transaction while tx1 holds the lock
	_, err = m.CreateTransaction("session-1", cloudRPCID2, true, "action", 10*time.Second, true)
	if err != ErrBusy {
		t.Fatalf("expected ErrBusy for concurrent state-changing tx, got %v", err)
	}

	// Fail tx1 to release the lock (Fail is valid from TxCreated)
	err = m.Fail(tx1.RPCID, []byte("failed"))
	if err != nil {
		t.Fatalf("failed to fail tx1: %v", err)
	}

	// Now we should be able to create tx2
	tx2, err := m.CreateTransaction("session-1", cloudRPCID2, true, "action", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create tx2 after lock released: %v", err)
	}
	if tx2.State != TxCreated {
		t.Fatalf("expected state TxCreated, got %v", tx2.State)
	}
}

func TestTCRM003_ParallelReadOperations(t *testing.T) {
	m := setupTestManager()

	cloudRPCID1 := json.RawMessage(`"tx-1"`)
	queryRPCID := json.RawMessage(`"query-1"`)

	// Start a state-changing transaction
	tx1, err := m.CreateTransaction("session-1", cloudRPCID1, true, "action", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create tx1: %v", err)
	}

	// Try to start a read-only transaction (isStateChanging = false)
	queryTx, err := m.CreateTransaction("session-1", queryRPCID, true, "get_status", 10*time.Second, false)
	if err != nil {
		t.Fatalf("failed to create parallel query: %v", err)
	}

	if tx1.State != TxCreated || queryTx.State != TxCreated {
		t.Fatalf("both transactions should be active in TxCreated")
	}
}

func TestTCUPG004_UpgradeAsynchronousLockHandoff(t *testing.T) {
	m := setupTestManager()

	cloudRPCID := json.RawMessage(`"upg-1"`)

	// Start a state-changing upgrade transaction
	tx, err := m.CreateTransaction("session-1", cloudRPCID, true, "upgrade", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create upgrade tx: %v", err)
	}

	// Verify lock is held by the RPCID
	m.mu.Lock()
	if m.activeStateTx != tx.RPCID {
		t.Fatalf("expected state lock to be held by RPCID %s, got %s", tx.RPCID, m.activeStateTx)
	}
	m.mu.Unlock()

	// Verify illegal jump: calling RespondAndRetain from TxCreated
	err = m.RespondAndRetain(tx.RPCID, []byte(`{"status": {"error": 0}}`))
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition for RespondAndRetain from TxCreated, got %v", err)
	}

	// Advance transaction to TxInFlight
	if err := m.MarkPreparingDispatch(tx.RPCID); err != nil {
		t.Fatalf("failed to mark preparing dispatch: %v", err)
	}
	if err := m.MarkPendingPublish(tx.RPCID); err != nil {
		t.Fatalf("failed to mark pending publish: %v", err)
	}
	if err := m.MarkInFlight(tx.RPCID); err != nil {
		t.Fatalf("failed to mark in flight: %v", err)
	}

	// Call RespondAndRetain
	err = m.RespondAndRetain(tx.RPCID, []byte(`{"status": {"error": 0}}`))
	if err != nil {
		t.Fatalf("RespondAndRetain failed: %v", err)
	}

	// Verify transaction is terminal
	m.mu.Lock()
	if _, ok := m.transactionsByRPCID[tx.RPCID]; ok {
		t.Fatalf("transaction should be removed from active map")
	}

	// Verify lock was transferred to a persistent operation ID
	opID := m.activeStateTx
	if opID == "" || opID == tx.RPCID {
		t.Fatalf("expected state lock to be transferred to an OperationID, got %s", opID)
	}
	m.mu.Unlock()

	// Verify the lock can be released via the new operation ID
	err = m.ReleaseOperationLock(opID)
	if err != nil {
		t.Fatalf("failed to release operation lock: %v", err)
	}

	// Verify we can now start a new state-changing command
	tx2, err := m.CreateTransaction("session-2", json.RawMessage(`3`), true, "configure", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create second tx after unlock: %v", err)
	}
	m.Fail(tx2.RPCID, []byte("cleanup"))
}

// errorMockStore for testing persistence failures
type errorMockStore struct {
	mockStore
}

func (s *errorMockStore) Save(ctx context.Context, operation *PersistentOperation) error {
	return errors.New("simulated disk failure")
}

func TestTCUPG005_RespondAndRetainRollback(t *testing.T) {
	cache := NewTransactionCache()
	config := CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &errorMockStore{}
	m := NewRequestManager(10*time.Second, config, cache, scheduler, store)

	cloudRPCID := json.RawMessage(`"upg-rollback"`)
	tx, err := m.CreateTransaction("session-err", cloudRPCID, true, "upgrade", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create upgrade tx: %v", err)
	}

	m.MarkPreparingDispatch(tx.RPCID)
	m.MarkPendingPublish(tx.RPCID)
	m.MarkInFlight(tx.RPCID)

	// Call RespondAndRetain which will fail during Save
	err = m.RespondAndRetain(tx.RPCID, []byte(`{"status": {"error": 0}}`))
	if err == nil {
		t.Fatalf("expected error from RespondAndRetain, got nil")
	}

	// Verify rollback succeeded
	m.mu.Lock()
	if m.activeStateTx != tx.RPCID {
		t.Fatalf("expected lock to be restored to rpcID, got %s", m.activeStateTx)
	}

	restoredTx, exists := m.transactionsByRPCID[tx.RPCID]
	if !exists {
		t.Fatalf("transaction was not restored to transactionsByRPCID")
	}
	if restoredTx.State != TxInFlight {
		t.Fatalf("restored transaction is in wrong state: %v", restoredTx.State)
	}

	_, exists = m.activeCloudRequests[tx.RequestKey]
	if !exists {
		t.Fatalf("transaction was not restored to activeCloudRequests")
	}
	m.mu.Unlock()

	// Verify we can now properly clean it up using standard methods
	err = m.Fail(tx.RPCID, []byte("cleanup"))
	if err != nil {
		t.Fatalf("failed to cleanup restored transaction: %v", err)
	}
}
