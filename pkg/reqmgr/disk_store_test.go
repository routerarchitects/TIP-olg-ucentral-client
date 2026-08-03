package reqmgr

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
	now := time.Now()
	for i := 0; i < 3; i++ {
		op := &PersistentOperation{
			OperationID: uuids[i],
			UpdatedAt:   now.Add(time.Duration(i) * time.Hour).Format(time.RFC3339),
			Active:      true,
		}
		if err := store.Save(ctx, op); err != nil {
			t.Fatalf("failed to save op-%d: %v", i+1, err)
		}
	}

	// GetActive with limit 2
	ops, err := store.GetActive(ctx, 2)
	if err != nil {
		t.Fatalf("GetActive failed: %v", err)
	}

	if len(ops) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(ops))
	}

	// Because of descending UpdatedAt sort, the newest should be first.
	// uuids[2] is newest (+2 hours), uuids[1] is next (+1 hour).
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

	// 1. Corrupt JSON
	corruptPath1 := tempDir + "/11111111-1111-1111-1111-111111111111.json"
	os.WriteFile(corruptPath1, []byte("{corrupt-json"), 0640)

	// 2. Valid JSON, bad UUID
	corruptPath2 := tempDir + "/22222222-2222-2222-2222-222222222222.json"
	b2, _ := json.Marshal(map[string]any{"operation_id": "not-a-uuid", "active": true, "updated_at": time.Now().Format(time.RFC3339)})
	os.WriteFile(corruptPath2, b2, 0640)

	// 3. Valid JSON, mismatched filename
	corruptPath3 := tempDir + "/33333333-3333-3333-3333-333333333333.json"
	b3, _ := json.Marshal(map[string]any{"operation_id": "44444444-4444-4444-4444-444444444444", "active": true, "updated_at": time.Now().Format(time.RFC3339)})
	os.WriteFile(corruptPath3, b3, 0640)

	// 4. Valid JSON, invalid UpdatedAt
	corruptPath4 := tempDir + "/55555555-5555-5555-5555-555555555555.json"
	b4, _ := json.Marshal(map[string]any{"operation_id": "55555555-5555-5555-5555-555555555555", "active": true, "updated_at": "not-a-time"})
	os.WriteFile(corruptPath4, b4, 0640)

	// GetActive should log and quarantine all of them
	ops, err := store.GetActive(context.Background(), 10)
	if err != nil {
		t.Fatalf("GetActive failed: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected 0 ops, got %d", len(ops))
	}

	for i, path := range []string{corruptPath1, corruptPath2, corruptPath3, corruptPath4} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected file %d to be removed from original location", i)
		}

		matches, err := filepath.Glob(tempDir + "/quarantine/" + filepath.Base(path) + ".*.corrupt")
		if err != nil || len(matches) == 0 {
			t.Fatalf("expected file %d to exist in quarantine directory", i)
		}
	}
}

func TestDiskOperationStore_GetActive_ActiveFilter(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "olg-store-test-*")
	defer os.RemoveAll(tempDir)
	store, _ := NewDiskOperationStore(tempDir)
	ctx := context.Background()

	// 1 Active, 1 Inactive
	opActive := &PersistentOperation{OperationID: "11111111-1111-1111-1111-111111111111", Active: true, UpdatedAt: time.Now().Format(time.RFC3339)}
	opInactive := &PersistentOperation{OperationID: "22222222-2222-2222-2222-222222222222", Active: false, UpdatedAt: time.Now().Format(time.RFC3339)}

	store.Save(ctx, opActive)
	store.Save(ctx, opInactive)

	ops, err := store.GetActive(ctx, 10)
	if err != nil {
		t.Fatalf("GetActive failed: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 active operation, got %d", len(ops))
	}
	if ops[0].OperationID != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("expected active operation to be returned")
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
