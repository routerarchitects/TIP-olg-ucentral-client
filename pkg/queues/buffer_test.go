package queues

import (
	"testing"
)

// TC-BUF-001 (Telemetry Ring Buffer FIFO Drop)
func TestTCBUF001_TelemetryRingBuffer_FIFODropAndClone(t *testing.T) {
	capacity := 5
	b := NewTelemetryRingBuffer(capacity)

	// Push 5 messages
	for i := 1; i <= 5; i++ {
		payload := []byte{byte(i)}
		dropped := b.Push(payload)
		if dropped {
			t.Fatalf("unexpected drop on push %d", i)
		}
	}

	if b.size != capacity {
		t.Fatalf("expected size %d, got %d", capacity, b.size)
	}

	// Create a payload we will mutate to prove cloning works
	payload6 := []byte{6}

	// Push 6th message, should cause the 1st to be dropped
	dropped := b.Push(payload6)
	if !dropped {
		t.Fatalf("expected dropped=true for 6th push")
	}

	if b.size != capacity {
		t.Fatalf("expected size to remain %d after overflow, got %d", capacity, b.size)
	}

	// Mutate the original slice to ensure the buffer cloned it
	payload6[0] = 99

	// Pop and verify
	// The oldest should now be 2, since 1 was dropped
	expectedOrder := []byte{2, 3, 4, 5, 6}
	for i, expected := range expectedOrder {
		popped, found := b.Pop()
		if !found {
			t.Fatalf("expected to find item at index %d", i)
		}
		if len(popped) != 1 || popped[0] != expected {
			t.Fatalf("expected payload %d, got %v", expected, popped)
		}
	}

	// Next pop should be empty
	_, found := b.Pop()
	if found {
		t.Fatalf("expected ring buffer to be empty")
	}
}
