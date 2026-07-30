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

	// A fast Complete response received before TxInFlight is buffered.
	// Timeout remains invalid before TxInFlight.

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
	_, err = m.RespondAndRetain(tx.RPCID, []byte(`{"status": {"error": 0}}`))
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
	_, err = m.RespondAndRetain(tx.RPCID, []byte(`{"status": {"error": 0}}`))
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
	err = m.ReleaseOperationLock(context.Background(), opID)
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

	if err := m.MarkPreparingDispatch(tx.RPCID); err != nil {
		t.Fatalf("MarkPreparingDispatch failed: %v", err)
	}

	if err := m.MarkPendingPublish(tx.RPCID); err != nil {
		t.Fatalf("MarkPendingPublish failed: %v", err)
	}

	if err := m.MarkInFlight(tx.RPCID); err != nil {
		t.Fatalf("MarkInFlight failed: %v", err)
	}

	// Call RespondAndRetain which will fail during Save
	_, err = m.RespondAndRetain(tx.RPCID, []byte(`{"status": {"error": 0}}`))
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

// blockingMockStore blocks Save until a channel is closed, then returns a configurable error
type blockingMockStore struct {
	mockStore
	entered   chan struct{}
	release   chan struct{}
	err       error
	deleteErr error
}

func (s *blockingMockStore) Save(ctx context.Context, operation *PersistentOperation) error {
	close(s.entered)
	select {
	case <-s.release:
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockingMockStore) Delete(ctx context.Context, operationID string) error {
	return s.deleteErr
}

func TestTCUPG006_RespondAndRetainConcurrentTerminalEvent(t *testing.T) {
	cases := []struct {
		name       string
		saveErr    error
		deleteErr  error
		terminalOp string
	}{
		{"Complete during Save Failure", errors.New("disk fail"), nil, "complete"},
		{"Fail during Save Failure", errors.New("disk fail"), nil, "fail"},
		{"Timeout during Save Failure", errors.New("disk fail"), nil, "timeout"},
		{"Complete during Save Success", nil, nil, "complete"},
		{"Fail during Save Success", nil, nil, "fail"},
		{"Timeout during Save Success", nil, nil, "timeout"},
		{"Complete during Delete Failure", nil, errors.New("delete fail"), "complete"},
		{"Fail during Delete Failure", nil, errors.New("delete fail"), "fail"},
		{"Timeout during Delete Failure", nil, errors.New("delete fail"), "timeout"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewTransactionCache()
			config := CacheTTLConfig{}
			scheduler := queues.NewPriorityScheduler(10, 10)
			store := &blockingMockStore{
				entered:   make(chan struct{}),
				release:   make(chan struct{}),
				err:       tc.saveErr,
				deleteErr: tc.deleteErr,
			}
			m := NewRequestManager(10*time.Second, config, cache, scheduler, store)

			cloudRPCID := json.RawMessage(`"upg-concurrent"`)
			tx, err := m.CreateTransaction("session-err", cloudRPCID, true, "upgrade", 10*time.Second, true)
			if err != nil {
				t.Fatalf("failed to create upgrade tx: %v", err)
			}

			m.MarkPreparingDispatch(tx.RPCID)
			m.MarkPendingPublish(tx.RPCID)
			m.MarkInFlight(tx.RPCID)

			// Start RespondAndRetain in a goroutine so it blocks on Save
			errCh := make(chan error)
			go func() {
				_, err := m.RespondAndRetain(tx.RPCID, []byte(`{"status": {"error": 0}}`))
				errCh <- err
			}()

			// Wait deterministically for Save to actually start (lock transferred)
			<-store.entered

			// Simulate a concurrent terminal event arriving
			switch tc.terminalOp {
			case "complete":
				err = m.Complete(tx.RPCID, []byte("success"))
			case "fail":
				err = m.Fail(tx.RPCID, []byte("fail"))
			case "timeout":
				err = m.Timeout(tx.RPCID)
			}
			if err != nil {
				t.Fatalf("%s failed concurrently: %v", tc.terminalOp, err)
			}

			// Unblock the Save
			close(store.release)

			// Get the RespondAndRetain result
			err = <-errCh

			// If Save succeeded, RespondAndRetain should succeed (nil).
			// If Save failed, RespondAndRetain should still succeed (nil) because the buffered terminal event satisfies the state machine!
			// If Delete failed, RespondAndRetain should return the delete error.
			if tc.deleteErr != nil {
				if err == nil {
					t.Fatalf("expected RespondAndRetain to return delete error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("expected RespondAndRetain to successfully return nil due to buffered reply, got %v", err)
				}
			}

			// Verify the lock is fully released and transaction is deleted
			m.mu.Lock()
			if tc.deleteErr != nil {
				if m.activeStateTx == "" || m.activeStateTx == tx.RPCID {
					t.Fatalf("expected lock to be held by operationID when delete fails, got %s", m.activeStateTx)
				}
			} else {
				if m.activeStateTx != "" {
					t.Fatalf("expected lock to be fully released, got %s", m.activeStateTx)
				}
			}
			if _, exists := m.transactionsByRPCID[tx.RPCID]; exists {
				t.Fatalf("expected transaction to be deleted by the buffered event")
			}
			m.mu.Unlock()
		})
	}
}

func TestTCRM007_CanonicalRequestKey(t *testing.T) {
	tests := []struct {
		name      string
		sessionID string
		id        json.RawMessage
		wantKey   string // leave empty if expecting an error
		wantErr   bool
	}{
		{"String ID", "sess", json.RawMessage(`"123"`), "sess:s:123", false},
		{"Number ID", "sess", json.RawMessage(`123`), "sess:n:123", false},
		{"Float ID", "sess", json.RawMessage(`123.0`), "sess:n:123", false},
		{"Exponential ID", "sess", json.RawMessage(`1.23e2`), "sess:n:123", false},
		{"Large Int 1", "sess", json.RawMessage(`9007199254740992`), "sess:n:9007199254740992", false},
		{"Large Int 2", "sess", json.RawMessage(`9007199254740993`), "sess:n:9007199254740993", false},
		{"Null ID", "sess", json.RawMessage(`null`), "", false}, // generates UUID
		{"Empty ID", "sess", json.RawMessage(``), "", false},    // generates UUID
		{"Object ID", "sess", json.RawMessage(`{}`), "", true},
		{"Array ID", "sess", json.RawMessage(`[]`), "", true},
		{"Boolean ID", "sess", json.RawMessage(`true`), "", true},
		{"Invalid JSON", "sess", json.RawMessage(`{bad`), "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CanonicalRequestKey(tt.sessionID, tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("CanonicalRequestKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.wantKey != "" && got != tt.wantKey {
				t.Errorf("CanonicalRequestKey() got = %v, want %v", got, tt.wantKey)
			}
			if !tt.wantErr && tt.wantKey == "" && len(got) < 36 {
				t.Errorf("CanonicalRequestKey() for null/empty should generate UUID, got = %v", got)
			}
		})
	}
}

func TestTCRM008_NullIDBypass(t *testing.T) {
	cache := NewTransactionCache()
	config := CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &mockStore{}
	m := NewRequestManager(10*time.Second, config, cache, scheduler, store)

	// Test 1: respondToCloud = true, but id = null
	_, err := m.CreateTransaction("sess", json.RawMessage(`null`), true, "configure", 10*time.Second, true)
	if err == nil {
		t.Fatalf("expected error for state-changing command with null ID, got nil")
	}

	// Test 2: respondToCloud = true, but id = empty
	_, err = m.CreateTransaction("sess", json.RawMessage(``), true, "configure", 10*time.Second, true)
	if err == nil {
		t.Fatalf("expected error for state-changing command with empty ID, got nil")
	}

	// Test 3: non-state-changing command with null ID (should succeed, but respondToCloud must be false)
	tx, err := m.CreateTransaction("sess", json.RawMessage(`null`), true, "status.get", 10*time.Second, false)
	if err != nil {
		t.Fatalf("expected non-state-changing command with null ID to succeed, got %v", err)
	}
	if tx == nil {
		t.Fatalf("expected transaction, got nil")
	}
	if tx.RespondToCloud {
		t.Fatalf("expected RespondToCloud to be overridden to false for notification")
	}
}

func TestTCRM009_FastReplyBeforeInFlight(t *testing.T) {
	cases := []struct {
		name       string
		terminalOp string
		wantState  TransactionState
	}{
		{"Complete before InFlight", "complete", TxCompleted},
		{"Fail before InFlight", "fail", TxFailed},
		{"Timeout before InFlight", "timeout", TxTimedOut},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewTransactionCache()
			config := CacheTTLConfig{}
			scheduler := queues.NewPriorityScheduler(10, 10)
			store := &mockStore{}
			m := NewRequestManager(10*time.Second, config, cache, scheduler, store)

			tx, err := m.CreateTransaction("sess", json.RawMessage(`123`), true, "configure", 10*time.Second, true)
			if err != nil {
				t.Fatalf("failed to create tx: %v", err)
			}

			m.MarkPreparingDispatch(tx.RPCID)
			m.MarkPendingPublish(tx.RPCID)

			// Simulate a fast reply arriving before MarkInFlight
			switch tc.terminalOp {
			case "complete":
				err = m.Complete(tx.RPCID, []byte("success"))
			case "fail":
				err = m.Fail(tx.RPCID, []byte("fail"))
			case "timeout":
				err = m.Timeout(tx.RPCID)
			}
			if tc.terminalOp == "timeout" {
				if err != ErrInvalidStateTransition {
					t.Fatalf("expected ErrInvalidStateTransition for timeout, got %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("%s failed concurrently: %v", tc.terminalOp, err)
				}
			}

			// Verify behavior based on operation type
			m.mu.Lock()
			if tc.terminalOp == "complete" || tc.terminalOp == "timeout" {
				// Complete is buffered, Timeout was rejected. In both cases, the tx is still alive.
				if _, exists := m.transactionsByRPCID[tx.RPCID]; !exists {
					t.Fatalf("expected transaction to still exist in map")
				}
				if tc.terminalOp == "complete" && len(m.pendingReplies) != 1 {
					t.Fatalf("expected 1 pending reply, got %d", len(m.pendingReplies))
				}
			} else if tc.terminalOp == "fail" {
				// Fail is processed immediately in pre-flight states
				if _, exists := m.transactionsByRPCID[tx.RPCID]; exists {
					t.Fatalf("expected transaction to be deleted immediately")
				}
				if len(m.pendingReplies) != 0 {
					t.Fatalf("expected 0 pending replies, got %d", len(m.pendingReplies))
				}
			}
			m.mu.Unlock()

			// Call MarkInFlight
			err = m.MarkInFlight(tx.RPCID)

			if tc.terminalOp == "complete" || tc.terminalOp == "timeout" {
				if err != nil {
					t.Fatalf("MarkInFlight failed: %v", err)
				}
			} else if tc.terminalOp == "fail" {
				if err != ErrAlreadyTerminal {
					t.Fatalf("expected ErrAlreadyTerminal for MarkInFlight after immediate termination, got %v", err)
				}
			}

			m.mu.Lock()
			if len(m.pendingReplies) != 0 {
				t.Fatalf("expected pending reply to be drained")
			}
			if tc.terminalOp == "complete" || tc.terminalOp == "fail" {
				if _, exists := m.transactionsByRPCID[tx.RPCID]; exists {
					t.Fatalf("expected transaction to be deleted")
				}
				if m.activeStateTx != "" {
					t.Fatalf("expected lock to be released")
				}
			} else if tc.terminalOp == "timeout" {
				// Timeout was rejected, and MarkInFlight succeeded, so it's now in-flight!
				tx, exists := m.transactionsByRPCID[tx.RPCID]
				if !exists {
					t.Fatalf("expected transaction to still exist after MarkInFlight")
				}
				if tx.State != TxInFlight {
					t.Fatalf("expected transaction to be in TxInFlight state")
				}
			}
			m.mu.Unlock()
		})
	}
}

func TestTCRM010_DispatchTimeoutProcessesBufferedReply(t *testing.T) {
	cache := NewTransactionCache()
	config := CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &mockStore{}
	m := NewRequestManager(10*time.Second, config, cache, scheduler, store)

	tx, err := m.CreateTransaction("sess", json.RawMessage(`123`), true, "configure", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create tx: %v", err)
	}

	m.MarkPreparingDispatch(tx.RPCID)
	m.MarkPendingPublish(tx.RPCID)

	// Simulate a fast reply arriving early and getting buffered
	err = m.Complete(tx.RPCID, []byte("success"))
	if err != nil {
		t.Fatalf("Complete failed concurrently: %v", err)
	}

	// Verify the transaction is still technically alive and the reply is buffered
	m.mu.Lock()
	if _, exists := m.transactionsByRPCID[tx.RPCID]; !exists {
		t.Fatalf("expected transaction to still exist in map")
	}
	if len(m.pendingReplies) != 1 {
		t.Fatalf("expected 1 pending reply, got %d", len(m.pendingReplies))
	}
	m.mu.Unlock()

	// Simulate dispatch stalling and timing out
	// Instead of failing the transaction, it should process the buffered Complete
	m.dispatchTimeoutFail(tx.RPCID)

	// Verify both the transaction and the pending reply were removed
	m.mu.Lock()
	_, txExists := m.transactionsByRPCID[tx.RPCID]
	_, pendingExists := m.pendingReplies[tx.RPCID]

	if txExists || pendingExists {
		m.mu.Unlock()
		t.Fatalf("transaction and pending reply should be removed, got txExists=%v pendingExists=%v", txExists, pendingExists)
	}
	m.mu.Unlock()
}

func TestTCRM011_BufferedTerminalEventRace(t *testing.T) {
	cache := NewTransactionCache()
	config := CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &mockStore{}
	m := NewRequestManager(10*time.Second, config, cache, scheduler, store)

	tx, err := m.CreateTransaction("sess", json.RawMessage(`123`), true, "configure", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create tx: %v", err)
	}

	m.MarkPreparingDispatch(tx.RPCID)
	m.MarkPendingPublish(tx.RPCID)

	// First event wins the race and buffers successfully
	err = m.Complete(tx.RPCID, []byte("success"))
	if err != nil {
		t.Fatalf("first event failed: %v", err)
	}

	// Second concurrent event should lose the race and receive ErrAlreadyTerminal
	err = m.Complete(tx.RPCID, []byte("late-success"))
	if err != ErrAlreadyTerminal {
		t.Fatalf("expected loser to get ErrAlreadyTerminal, got %v", err)
	}

	// Verify the original buffered state is preserved
	m.mu.Lock()
	pending, exists := m.pendingReplies[tx.RPCID]
	if !exists {
		t.Fatalf("expected pending reply to exist")
	}
	if string(pending.Payload) != "success" {
		t.Fatalf("expected payload to be 'success', got '%s'", string(pending.Payload))
	}
	m.mu.Unlock()
}

func TestTCUPG012_RejectUpgradeNonStateChanging(t *testing.T) {
	cache := NewTransactionCache()
	config := CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &mockStore{}
	m := NewRequestManager(10*time.Second, config, cache, scheduler, store)

	_, err := m.CreateTransaction("sess", json.RawMessage(`"upg-err"`), true, "upgrade", 10*time.Second, false)
	if err == nil {
		t.Fatalf("expected error when upgrade is not state-changing")
	}
}

type blockDeleteMockStore struct {
	mockStore
	block   chan struct{}
	release chan struct{}
}

func (s *blockDeleteMockStore) Delete(ctx context.Context, operationID string) error {
	s.block <- struct{}{}
	<-s.release
	return errors.New("delete failed")
}

func TestTCRM013_ConcurrentReleaseOperationLock(t *testing.T) {
	cache := NewTransactionCache()
	config := CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &blockDeleteMockStore{
		block:   make(chan struct{}),
		release: make(chan struct{}),
	}
	m := NewRequestManager(10*time.Second, config, cache, scheduler, store)

	// manually set active state tx
	m.mu.Lock()
	m.activeStateTx = "op-1"
	m.mu.Unlock()

	errCh := make(chan error)
	go func() {
		errCh <- m.ReleaseOperationLock(context.Background(), "op-1")
	}()

	// Wait for first caller to enter Delete
	<-store.block

	// Second concurrent caller
	err2 := m.ReleaseOperationLock(context.Background(), "op-1")
	if err2 != ErrOperationReleaseInProgress {
		t.Fatalf("expected ErrOperationReleaseInProgress, got %v", err2)
	}

	// release first caller
	store.release <- struct{}{}

	err1 := <-errCh
	if err1 == nil {
		t.Fatalf("expected delete to fail")
	}
}

type dualBlockMockStore struct {
	mockStore
	blockSave     chan struct{}
	releaseSave   chan struct{}
	blockDelete   chan struct{}
	releaseDelete chan struct{}
}

func (s *dualBlockMockStore) Save(ctx context.Context, op *PersistentOperation) error {
	s.blockSave <- struct{}{}
	<-s.releaseSave
	return nil
}

func (s *dualBlockMockStore) Delete(ctx context.Context, opID string) error {
	s.blockDelete <- struct{}{}
	<-s.releaseDelete
	return nil
}

func TestTCRM014_ConcurrentRespondAndRetainDeletion(t *testing.T) {
	cache := NewTransactionCache()
	config := CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &dualBlockMockStore{
		blockSave:     make(chan struct{}),
		releaseSave:   make(chan struct{}),
		blockDelete:   make(chan struct{}),
		releaseDelete: make(chan struct{}),
	}
	m := NewRequestManager(10*time.Second, config, cache, scheduler, store)

	tx, err := m.CreateTransaction("sess", json.RawMessage(`"concurrent-del"`), true, "upgrade", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create upgrade tx: %v", err)
	}

	m.MarkPreparingDispatch(tx.RPCID)
	m.MarkPendingPublish(tx.RPCID)
	m.MarkInFlight(tx.RPCID)

	errCh := make(chan error)
	go func() {
		_, err := m.RespondAndRetain(tx.RPCID, []byte(`{}`))
		errCh <- err
	}()

	// Wait for RespondAndRetain to enter Save
	<-store.blockSave

	// Buffer a terminal reply during Save
	if err := m.Complete(tx.RPCID, []byte("success")); err != nil {
		t.Fatalf("failed to buffer complete: %v", err)
	}

	// Release Save so RespondAndRetain proceeds to Delete
	store.releaseSave <- struct{}{}

	// Wait for RespondAndRetain to enter Delete
	<-store.blockDelete

	// Get the active operation ID
	m.mu.Lock()
	active := m.activeStateTx
	m.mu.Unlock()

	// It should be deleting:<operationID>
	if len(active) < 10 || active[:9] != "deleting:" {
		t.Fatalf("expected state lock to be 'deleting:<opID>', got '%s'", active)
	}
	opID := active[9:]

	// Concurrently call ReleaseOperationLock
	errRel := m.ReleaseOperationLock(context.Background(), opID)
	if errRel != ErrOperationReleaseInProgress {
		t.Fatalf("expected ErrOperationReleaseInProgress, got %v", errRel)
	}

	// Release Delete
	store.releaseDelete <- struct{}{}

	// Wait for RespondAndRetain to finish
	errRetain := <-errCh
	if errRetain != nil {
		t.Fatalf("RespondAndRetain failed: %v", errRetain)
	}
}

func TestTCRM016_DuplicateRequestRejection(t *testing.T) {
	cache := NewTransactionCache()
	config := CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &mockStore{}
	m := NewRequestManager(10*time.Second, config, cache, scheduler, store)

	cloudRPCID := json.RawMessage(`42`)

	// 1. Create tx1 (session-1, id=42, method=configure)
	tx1, err := m.CreateTransaction("session-1", cloudRPCID, true, "configure", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create first tx: %v", err)
	}

	// 2. Try to create tx2 (session-1, id=42, method=ping) -> should fail with ErrBusy
	_, err = m.CreateTransaction("session-1", cloudRPCID, true, "ping", 10*time.Second, false)
	if err != ErrBusy {
		t.Fatalf("expected ErrBusy for same-session, same-ID, different-method, got %v", err)
	}

	// 3. Create tx3 (session-2, id=42, method=ping) -> should succeed
	tx3, err := m.CreateTransaction("session-2", cloudRPCID, true, "ping", 10*time.Second, false)
	if err != nil {
		t.Fatalf("failed to create same-ID different-session tx: %v", err)
	}
	if tx3 == nil || tx3.State != TxCreated {
		t.Fatalf("expected tx3 to be created")
	}

	// Cleanup to leave state clean (optional)
	m.Fail(tx1.RPCID, nil)
	m.Fail(tx3.RPCID, nil)
}

