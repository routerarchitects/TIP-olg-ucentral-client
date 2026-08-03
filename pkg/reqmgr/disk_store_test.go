package reqmgr

import (
	"context"
	"encoding/json"
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
		OperationID: "12345678-1234-1234-1234-123456789012",
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
	fetched, err := store.Get(ctx, "12345678-1234-1234-1234-123456789012")
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if fetched == nil {
		t.Fatalf("expected operation, got nil")
	}
	if fetched.OperationID != "12345678-1234-1234-1234-123456789012" {
		t.Fatalf("expected 12345678-1234-1234-1234-123456789012, got %s", fetched.OperationID)
	}
	if string(fetched.CloudRPCID) != `"123"` {
		t.Fatalf("expected cloud RPC ID '123', got %s", string(fetched.CloudRPCID))
	}

	// 3. Delete
	if err := store.Delete(ctx, "12345678-1234-1234-1234-123456789012"); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// 4. Get after delete
	fetched, err = store.Get(ctx, "12345678-1234-1234-1234-123456789012")
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

	uuids := []string{
		"11111111-1111-1111-1111-111111111111",
		"22222222-2222-2222-2222-222222222222",
		"33333333-3333-3333-3333-333333333333",
	}

	// Create 3 operations
	for i := 0; i < 3; i++ {
		op := &PersistentOperation{
			OperationID: uuids[i],
		}
		if err := store.Save(ctx, op); err != nil {
			t.Fatalf("failed to save op-%d: %v", i+1, err)
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
	// We slept between saves, so uuids[2] is newest, uuids[1] is next, uuids[0] is oldest.
	if ops[0].OperationID != uuids[2] {
		t.Fatalf("expected first element to be newest, got %s", ops[0].OperationID)
	}
	if ops[1].OperationID != uuids[1] {
		t.Fatalf("expected second element to be next newest, got %s", ops[1].OperationID)
	}
}

func TestDiskOperationStore_NilOperation(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "olg-store-test-*")
	defer os.RemoveAll(tempDir)
	store, _ := NewDiskOperationStore(tempDir)

	err := store.Save(context.Background(), nil)
	if err == nil || err.Error() != "operation cannot be nil" {
		t.Fatalf("expected nil operation error, got %v", err)
	}
}

func TestDiskOperationStore_InvalidUUIDs(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "olg-store-test-*")
	defer os.RemoveAll(tempDir)
	store, _ := NewDiskOperationStore(tempDir)
	ctx := context.Background()

	invalidIDs := []string{
		"",
		"../../etc/passwd",
		"not-a-uuid",
	}
	for _, id := range invalidIDs {
		err := store.Save(ctx, &PersistentOperation{OperationID: id})
		if err == nil {
			t.Fatalf("expected error for invalid ID '%s'", id)
		}
		_, err = store.Get(ctx, id)
		if err == nil {
			t.Fatalf("expected error for invalid ID '%s'", id)
		}
		err = store.Delete(ctx, id)
		if err == nil {
			t.Fatalf("expected error for invalid ID '%s'", id)
		}
	}
}

func TestDiskOperationStore_GetActiveCorruptPolicy(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "olg-store-test-*")
	defer os.RemoveAll(tempDir)
	store, _ := NewDiskOperationStore(tempDir)

	// Create a corrupt file directly
	corruptPath := tempDir + "/12345678-1234-1234-1234-123456789012.json"
	os.WriteFile(corruptPath, []byte("{corrupt-json"), 0640)

	// GetActive should log and delete it
	ops, err := store.GetActive(context.Background(), 10)
	if err != nil {
		t.Fatalf("GetActive failed: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected 0 ops, got %d", len(ops))
	}

	// Verify it was deleted
	if _, err := os.Stat(corruptPath); !os.IsNotExist(err) {
		t.Fatalf("expected corrupt file to be deleted")
	}
}

func TestDiskOperationStore_ContextCancelled(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "olg-store-test-*")
	defer os.RemoveAll(tempDir)
	store, _ := NewDiskOperationStore(tempDir)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	op := &PersistentOperation{OperationID: "11111111-1111-1111-1111-111111111111"}
	err := store.Save(ctx, op)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled for Save, got %v", err)
	}

	err = store.Delete(ctx, op.OperationID)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled for Delete, got %v", err)
	}
}
