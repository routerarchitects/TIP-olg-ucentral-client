package reqmgr

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestDiskOperationStore_SaveAndGet(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "olg-store-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store, err := NewDiskOperationStore(tempDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	op := &PersistentOperation{
		OperationID: "op-123",
		RPCID:       "rpc-123",
		CloudRPCID:  json.RawMessage(`"123"`),
		Target:      "upgrade",
		Action:      "upgrade",
		Stage:       "download",
		Status:      "in_progress",
		Active:      true,
		CreatedAt:   time.Now().Format(time.RFC3339),
		UpdatedAt:   time.Now().Format(time.RFC3339),
	}

	ctx := context.Background()

	// 1. Save
	if err := store.Save(ctx, op); err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	// 2. Get
	fetched, err := store.Get(ctx, "op-123")
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if fetched == nil {
		t.Fatalf("expected operation, got nil")
	}
	if fetched.OperationID != "op-123" {
		t.Fatalf("expected op-123, got %s", fetched.OperationID)
	}
	if string(fetched.CloudRPCID) != `"123"` {
		t.Fatalf("expected cloud RPC ID '123', got %s", string(fetched.CloudRPCID))
	}

	// 3. Delete
	if err := store.Delete(ctx, "op-123"); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// 4. Get after delete
	fetched, err = store.Get(ctx, "op-123")
	if err != nil {
		t.Fatalf("unexpected error getting deleted op: %v", err)
	}
	if fetched != nil {
		t.Fatalf("expected nil for deleted op, got %v", fetched)
	}
}

func TestDiskOperationStore_GetActive_SortingAndLimit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "olg-store-test-active-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store, err := NewDiskOperationStore(tempDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()

	// Create 3 operations
	for i := 1; i <= 3; i++ {
		op := &PersistentOperation{
			OperationID: fmt.Sprintf("op-%d", i),
		}
		if err := store.Save(ctx, op); err != nil {
			t.Fatalf("failed to save op-%d: %v", i, err)
		}
		// ensure modification times are distinct
		time.Sleep(10 * time.Millisecond)
	}

	// GetActive with limit 2
	ops, err := store.GetActive(ctx, 2)
	if err != nil {
		t.Fatalf("GetActive failed: %v", err)
	}

	if len(ops) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(ops))
	}

	// Because of descending ModTime sort, the newest should be first.
	// We slept between saves, so op-3 is newest, op-2 is next, op-1 is oldest.
	if ops[0].OperationID != "op-3" {
		t.Fatalf("expected first element to be newest (op-3), got %s", ops[0].OperationID)
	}
	if ops[1].OperationID != "op-2" {
		t.Fatalf("expected second element to be next newest (op-2), got %s", ops[1].OperationID)
	}
}
