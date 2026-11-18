package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/contracts"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/queues"
	"github.com/routerarchitects/TIP-olg-ucentral-client/pkg/reqmgr"
)

type errorMockStore struct{}

func (s *errorMockStore) Get(ctx context.Context, id string) (*reqmgr.PersistentOperation, error) {
	return nil, nil
}
func (s *errorMockStore) GetActive(ctx context.Context, limit int) ([]*reqmgr.PersistentOperation, error) {
	return nil, nil
}
func (s *errorMockStore) SetActive(ctx context.Context, id string) error {
	return nil
}
func (s *errorMockStore) Save(ctx context.Context, operation *reqmgr.PersistentOperation) error {
	return context.DeadlineExceeded // simulate save failure
}
func (s *errorMockStore) LoadAll(ctx context.Context) ([]*reqmgr.PersistentOperation, error) {
	return nil, nil
}
func (s *errorMockStore) Delete(ctx context.Context, operationID string) error {
	return nil
}

func TestProcessNATSResult_UpgradePersistenceFailure(t *testing.T) {
	cache := reqmgr.NewTransactionCache()
	cacheTTL := reqmgr.CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &errorMockStore{}

	m, _ := reqmgr.NewRequestManager(10*time.Second, cacheTTL, cache, scheduler, store, 1000, 15*time.Minute, 100)

	components := &AppComponents{
		ReqManager: m,
		Scheduler:  scheduler,
	}

	cloudRPCID := json.RawMessage(`"upg-test-1"`)
	tx, err := m.CreateTransaction("session-1", cloudRPCID, true, "upgrade", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create tx: %v", err)
	}

	// Advance state to TxPendingPublish to simulate normal flow before result arrives
	_ = m.MarkPreparingDispatch(tx.RPCID)
	_ = m.MarkPendingPublish(tx.RPCID)

	res := agentcore.ResultEnvelope{
		RPCID:       tx.RPCID,
		CommandType: "upgrade",
		Result:      "success", // Success from agent
		ErrorCode:   "0",
	}

	// Process the result. This will attempt to persist the operation and fail.
	processNATSResult(context.Background(), res, components, "serial-123")

	// Pull the pushed message from the scheduler queue
	outbound, err := scheduler.Next(context.Background())
	if err != nil {
		t.Fatalf("expected a response to be pushed to the scheduler")
	}

	// The pushed message should NOT be the success result, but the internal error result
	var jsonResp contracts.JSONRPCResponse
	if err := json.Unmarshal(outbound.Payload, &jsonResp); err != nil {
		t.Fatalf("failed to unmarshal pushed payload: %v", err)
	}

	if jsonResp.Error == nil {
		t.Fatalf("expected an error response to be pushed, got success: %s", string(outbound.Payload))
	}
	if jsonResp.Error.Code != -32603 {
		t.Fatalf("expected error code -32603, got %d", jsonResp.Error.Code)
	}
}

func TestHandleNATSResultOverflow_UpgradePersistenceFailure(t *testing.T) {
	cache := reqmgr.NewTransactionCache()
	cacheTTL := reqmgr.CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &errorMockStore{}

	m, _ := reqmgr.NewRequestManager(10*time.Second, cacheTTL, cache, scheduler, store, 1000, 15*time.Minute, 100)

	components := &AppComponents{
		ReqManager: m,
		Scheduler:  scheduler,
	}

	cloudRPCID := json.RawMessage(`"upg-overflow-test-1"`)
	tx, err := m.CreateTransaction("session-1", cloudRPCID, true, "upgrade", 10*time.Second, true)
	if err != nil {
		t.Fatalf("failed to create tx: %v", err)
	}

	// Advance state to TxPendingPublish to simulate normal flow before result arrives
	_ = m.MarkPreparingDispatch(tx.RPCID)
	_ = m.MarkPendingPublish(tx.RPCID)

	res := agentcore.ResultEnvelope{
		RPCID:       tx.RPCID,
		CommandType: "upgrade",
		Result:      "success", // Success from agent
		ErrorCode:   "0",
	}

	// Create a full queue so the default overflow branch is taken
	resultQueue := make(chan agentcore.ResultEnvelope) // capacity 0 means it blocks immediately

	// Process the result via the overflow handler
	handleNATSResult(context.Background(), res, resultQueue, components, "serial-123")

	// 1. Transaction should be failed and removed from the active map
	_, exists := m.GetTransaction(tx.RPCID)
	if exists {
		t.Fatalf("expected transaction to be deleted upon failure in overflow path, but it exists")
	}

	// 2. The state-changing lock should be released, allowing a new upgrade transaction
	_, err = m.CreateTransaction("session-1", cloudRPCID, true, "upgrade", 10*time.Second, true)
	if err != nil {
		t.Fatalf("expected state-changing lock to be released, but got error: %v", err)
	}
}

func TestParseTimeoutEnv_OperationSweeperTTL(t *testing.T) {
	t.Run("Default value when unset", func(t *testing.T) {
		t.Setenv("OLG_OPERATION_SWEEPER_TTL", "")
		ttl, err := parseTimeoutEnv("OLG_OPERATION_SWEEPER_TTL", 15*time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ttl != 15*time.Minute {
			t.Errorf("expected 15m, got %v", ttl)
		}
	})

	t.Run("Custom valid duration 30m", func(t *testing.T) {
		t.Setenv("OLG_OPERATION_SWEEPER_TTL", "30m")
		ttl, err := parseTimeoutEnv("OLG_OPERATION_SWEEPER_TTL", 15*time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ttl != 30*time.Minute {
			t.Errorf("expected 30m, got %v", ttl)
		}
	})

	t.Run("Invalid duration string fails", func(t *testing.T) {
		t.Setenv("OLG_OPERATION_SWEEPER_TTL", "foo")
		_, err := parseTimeoutEnv("OLG_OPERATION_SWEEPER_TTL", 15*time.Minute)
		if err == nil {
			t.Fatal("expected error for invalid duration string 'foo', got nil")
		}
	})

	t.Run("Zero duration fails", func(t *testing.T) {
		t.Setenv("OLG_OPERATION_SWEEPER_TTL", "0s")
		_, err := parseTimeoutEnv("OLG_OPERATION_SWEEPER_TTL", 15*time.Minute)
		if err == nil {
			t.Fatal("expected error for zero duration, got nil")
		}
	})

	t.Run("Negative duration fails", func(t *testing.T) {
		t.Setenv("OLG_OPERATION_SWEEPER_TTL", "-10m")
		_, err := parseTimeoutEnv("OLG_OPERATION_SWEEPER_TTL", 15*time.Minute)
		if err == nil {
			t.Fatal("expected error for negative duration, got nil")
		}
	})
}

func TestOperationStoreDirEnv(t *testing.T) {
	t.Run("Default directory is ./operations", func(t *testing.T) {
		t.Setenv("OLG_OPERATION_STORE_DIR", "")
		dir := "./operations"
		if envDir := os.Getenv("OLG_OPERATION_STORE_DIR"); envDir != "" {
			dir = envDir
		}
		if dir != "./operations" {
			t.Errorf("expected ./operations, got %s", dir)
		}
	})

	t.Run("Custom directory /tmp/operations", func(t *testing.T) {
		t.Setenv("OLG_OPERATION_STORE_DIR", "/tmp/operations")
		dir := "./operations"
		if envDir := os.Getenv("OLG_OPERATION_STORE_DIR"); envDir != "" {
			dir = envDir
		}
		if dir != "/tmp/operations" {
			t.Errorf("expected /tmp/operations, got %s", dir)
		}
	})
}

func TestProcessNATSResult_ConfigureUUIDMismatch_DropsResultAndPreservesLock(t *testing.T) {
	cache := reqmgr.NewTransactionCache()
	cacheTTL := reqmgr.CacheTTLConfig{}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &errorMockStore{}

	m, err := reqmgr.NewRequestManager(10*time.Second, cacheTTL, cache, scheduler, store, 1000, 15*time.Minute, 100)
	if err != nil {
		t.Fatalf("failed to create reqmgr: %v", err)
	}

	components := &AppComponents{
		ReqManager: m,
		Scheduler:  scheduler,
		Target:     "vyos",
	}

	cloudRPCID := json.RawMessage(`"cfg-test-200"`)
	tx, err := m.CreateTransaction("session-1", cloudRPCID, true, "configure", 10*time.Second, true, "200")
	if err != nil {
		t.Fatalf("failed to create tx: %v", err)
	}

	// Submit configure and acknowledge Cloud
	_ = m.MarkPreparingDispatch(tx.RPCID)
	_ = m.MarkPendingPublish(tx.RPCID)
	ackPayload := []byte(`{"jsonrpc":"2.0","id":"cfg-test-200","result":{"status":{"error":0,"text":"Success"}}}`)
	if err := m.AcknowledgeConfigure(tx.RPCID, "200", ackPayload); err != nil {
		t.Fatalf("AcknowledgeConfigure failed: %v", err)
	}

	assertTxActiveAndLocked := func(step string) {
		t.Helper()
		activeTx, exists := m.GetTransaction(tx.RPCID)
		if !exists {
			t.Fatalf("[%s] expected transaction to remain active, but it was removed", step)
		}
		if activeTx.State != reqmgr.TxInFlight {
			t.Fatalf("[%s] expected transaction to remain in TxInFlight, got %v", step, activeTx.State)
		}
		_, err = m.CreateTransaction("session-1", json.RawMessage(`"concurrent-cfg"`), true, "configure", 10*time.Second, true)
		if err == nil {
			t.Fatalf("[%s] expected state-changing lock to be held, but new transaction was admitted", step)
		}
	}

	// 1. Deliver downstream result with empty UUID -> must be dropped
	resEmptyUUID := agentcore.ResultEnvelope{
		RPCID:       tx.RPCID,
		Target:      "vyos",
		CommandType: "configure",
		UUID:        "",
		Result:      "success",
		Message:     "empty uuid result",
	}
	processNATSResult(context.Background(), resEmptyUUID, components, "serial-123")
	assertTxActiveAndLocked("empty UUID")

	// 2. Deliver downstream result with mismatched UUID (199 != 200) -> must be dropped
	resMismatch := agentcore.ResultEnvelope{
		RPCID:       tx.RPCID,
		Target:      "vyos",
		CommandType: "configure",
		UUID:        "199",
		Result:      "success",
		Message:     "applied stale uuid",
	}
	processNATSResult(context.Background(), resMismatch, components, "serial-123")
	assertTxActiveAndLocked("mismatched UUID")

	// 3. Deliver downstream result with mismatched CommandType ("action" != "configure") -> must be dropped
	resWrongCmd := agentcore.ResultEnvelope{
		RPCID:       tx.RPCID,
		Target:      "vyos",
		CommandType: "action",
		UUID:        "200",
		Result:      "success",
		Message:     "unrelated action",
	}
	processNATSResult(context.Background(), resWrongCmd, components, "serial-123")
	assertTxActiveAndLocked("mismatched command type")

	// 4. Deliver downstream result with mismatched Target ("other" != "vyos") -> must be dropped
	resWrongTarget := agentcore.ResultEnvelope{
		RPCID:       tx.RPCID,
		Target:      "other",
		CommandType: "configure",
		UUID:        "200",
		Result:      "success",
		Message:     "unrelated target",
	}
	processNATSResult(context.Background(), resWrongTarget, components, "serial-123")
	assertTxActiveAndLocked("mismatched target")

	// 4.5 Deliver downstream result with empty Target ("" != "vyos") -> must be dropped
	resEmptyTarget := agentcore.ResultEnvelope{
		RPCID:       tx.RPCID,
		Target:      "",
		CommandType: "configure",
		UUID:        "200",
		Result:      "success",
		Message:     "empty target",
	}
	processNATSResult(context.Background(), resEmptyTarget, components, "serial-123")
	assertTxActiveAndLocked("empty target")

	// 5. Now deliver the strictly matching downstream result (Target: vyos, CommandType: configure, UUID: 200)
	resMatching := agentcore.ResultEnvelope{
		RPCID:       tx.RPCID,
		Target:      "vyos",
		CommandType: "configure",
		UUID:        "200",
		Result:      "success",
		Message:     "commit successful",
	}

	processNATSResult(context.Background(), resMatching, components, "serial-123")

	// Verify transaction is now completed and removed
	if _, exists := m.GetTransaction(tx.RPCID); exists {
		t.Fatalf("expected transaction to be completed and removed after matching UUID result, but it still exists")
	}

	// Verify state lock is now released
	newTx, err := m.CreateTransaction("session-1", json.RawMessage(`"next-cfg"`), true, "configure", 10*time.Second, true)
	if err != nil {
		t.Fatalf("expected state lock to be released after matching UUID result, but got error: %v", err)
	}
	if newTx == nil {
		t.Fatal("expected new transaction to be created")
	}
}

func TestProcessNATSResult_LateResultAfterClientTimeout_SafelyIgnored(t *testing.T) {
	cache := reqmgr.NewTransactionCache()
	config := reqmgr.CacheTTLConfig{DefaultTTL: 10 * time.Minute}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &mockStore{}
	m, _ := reqmgr.NewRequestManager(10*time.Second, config, cache, scheduler, store, 1000, 15*time.Minute, 100)

	components := &AppComponents{
		ReqManager: m,
		Target:     "vyos",
	}

	// 1. Create and submit configure transaction
	tx, err := m.CreateTransaction("session-1", json.RawMessage(`"late-test"`), true, "configure", 50*time.Millisecond, true)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}
	_ = m.MarkPreparingDispatch(tx.RPCID)
	_ = m.MarkPendingPublish(tx.RPCID)
	ackBytes := []byte(`{"jsonrpc":"2.0","id":"late-test","result":{"status":{"error":0,"text":"Success"}}}`)
	_ = m.AcknowledgeConfigure(tx.RPCID, "300", ackBytes)

	// 2. Client-side timeout fires before downstream finishes
	err = m.Timeout(tx.RPCID)
	if err != nil {
		t.Fatalf("Timeout failed: %v", err)
	}

	// Verify state lock is now released
	newTx, err := m.CreateTransaction("session-1", json.RawMessage(`"after-timeout"`), true, "configure", 10*time.Second, true)
	if err != nil {
		t.Fatalf("expected state lock to be available after timeout: %v", err)
	}

	// 3. Now downstream VyOS finishes very late and sends a result for the timed-out rpc_id
	lateResult := agentcore.ResultEnvelope{
		RPCID:       tx.RPCID,
		Target:      "vyos",
		CommandType: "configure",
		UUID:        "300",
		Result:      "success",
		Message:     "late commit finished",
	}

	// Must be safely ignored without panicking or affecting active transactions
	processNATSResult(context.Background(), lateResult, components, "serial-123")

	// Verify the currently active transaction (after-timeout) is untouched and still holds the lock
	_, busyErr := m.CreateTransaction("session-1", json.RawMessage(`"busy-check"`), true, "configure", 10*time.Second, true)
	if !errors.Is(busyErr, reqmgr.ErrStateLockBusy) {
		t.Fatalf("expected stateLock to still be held by active transaction after late result dropped: %v", busyErr)
	}

	_ = m.Fail(newTx.RPCID, nil)
}

func TestProcessNATSResult_EarlyMismatchedResultBeforeAcknowledgeConfigure_Dropped(t *testing.T) {
	cache := reqmgr.NewTransactionCache()
	config := reqmgr.CacheTTLConfig{DefaultTTL: 10 * time.Minute}
	scheduler := queues.NewPriorityScheduler(10, 10)
	store := &mockStore{}
	m, _ := reqmgr.NewRequestManager(10*time.Second, config, cache, scheduler, store, 1000, 15*time.Minute, 100)

	components := &AppComponents{
		ReqManager: m,
		Target:     "vyos",
	}

	cloudRPCID := json.RawMessage(`"tx-early"`)
	// Create transaction with expected UUID registered upfront
	tx, err := m.CreateTransaction("session-1", cloudRPCID, true, "configure", 10*time.Second, true, "200")
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	_ = m.MarkPreparingDispatch(tx.RPCID)
	_ = m.MarkPendingPublish(tx.RPCID)

	// 1. Deliver an early mismatched downstream result while still in TxPendingPublish
	// (before AcknowledgeConfigure has executed)
	earlyMismatched := agentcore.ResultEnvelope{
		RPCID:       tx.RPCID,
		Target:      "vyos",
		CommandType: "configure",
		UUID:        "199", // Mismatched! Expected 200
		Result:      "success",
		Message:     "early stale commit",
	}

	processNATSResult(context.Background(), earlyMismatched, components, "serial-123")

	// Verify the result was dropped and NOT buffered
	activeTx, exists := m.GetTransaction(tx.RPCID)
	if !exists {
		t.Fatalf("transaction was incorrectly removed by mismatched early result")
	}
	if activeTx.State != reqmgr.TxPendingPublish {
		t.Fatalf("transaction state altered by mismatched early result: got %v, want TxPendingPublish", activeTx.State)
	}

	// 2. Now AcknowledgeConfigure executes
	ackBytes := []byte(`{"jsonrpc":"2.0","id":"tx-early","result":{"status":{"error":0,"text":"Success"}}}`)
	if err := m.AcknowledgeConfigure(tx.RPCID, "200", ackBytes); err != nil {
		t.Fatalf("AcknowledgeConfigure failed: %v", err)
	}

	// Because the bad result was not buffered, the transaction should be in TxInFlight (not completed)
	inFlightTx, exists := m.GetTransaction(tx.RPCID)
	if !exists {
		t.Fatalf("transaction was prematurely completed by AcknowledgeConfigure (should be TxInFlight awaiting matching result)")
	}
	if inFlightTx.State != reqmgr.TxInFlight {
		t.Fatalf("expected state TxInFlight, got %v", inFlightTx.State)
	}

	// Verify state lock is still held
	_, err = m.CreateTransaction("session-1", json.RawMessage(`"busy"`), true, "configure", 10*time.Second, true)
	if !errors.Is(err, reqmgr.ErrStateLockBusy) {
		t.Fatalf("expected ErrStateLockBusy while configure is in flight, got: %v", err)
	}

	// 3. Deliver the authentic matching downstream result (UUID 200)
	matchingResult := agentcore.ResultEnvelope{
		RPCID:       tx.RPCID,
		Target:      "vyos",
		CommandType: "configure",
		UUID:        "200",
		Result:      "success",
		Message:     "commit successful",
	}

	processNATSResult(context.Background(), matchingResult, components, "serial-123")

	// Verify transaction is completed and removed
	if _, exists := m.GetTransaction(tx.RPCID); exists {
		t.Fatalf("expected transaction to be completed and removed after authentic result")
	}

	// Verify state lock is released
	nextTx, err := m.CreateTransaction("session-1", json.RawMessage(`"next"`), true, "configure", 10*time.Second, true)
	if err != nil {
		t.Fatalf("expected state lock to be released, got: %v", err)
	}
	_ = m.Fail(nextTx.RPCID, nil)
}
