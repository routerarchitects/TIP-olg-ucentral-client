package reqmgr

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
)

// mockStore for testing
type mockStore struct{}

func (s *mockStore) Save(ctx context.Context, operation *PersistentOperation) error       { return nil }
func (s *mockStore) Get(ctx context.Context, operationID string) (*PersistentOperation, error) { return nil, nil }
func (s *mockStore) GetActive(ctx context.Context) (*PersistentOperation, error)            { return nil, nil }
func (s *mockStore) GetPendingTerminalDelivery(ctx context.Context) ([]*PersistentOperation, error) { return nil, nil }
func (s *mockStore) Delete(ctx context.Context, operationID string) error                 { return nil }

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

	// Complete tx1 to release the lock
	err = m.Complete(tx1.RPCID, []byte("success"))
	if err != nil {
		t.Fatalf("failed to complete tx1: %v", err)
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
