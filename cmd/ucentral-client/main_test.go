package main

import (
	"context"
	"encoding/json"
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
