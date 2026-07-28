package queues

import (
	"bytes"
	"testing"
)

// TC-BUF-002 (State Coalescing last-write-wins and Generation Tracking)
func TestTCBUF002_StateCoalescing_GenerationsAndCloning(t *testing.T) {
	c := NewStateCoalescer()

	// 1. Write State A
	stateA := []byte(`{"uptime": 10}`)
	c.Update(stateA)

	// 2. Write State B (should overwrite A)
	stateB := []byte(`{"uptime": 20}`)
	c.Update(stateB)

	// 3. Call Peek() and hold the generation
	snapshot, found := c.Peek()
	if !found {
		t.Fatalf("expected to find state B")
	}
	if !bytes.Equal(snapshot.Payload, stateB) {
		t.Fatalf("expected state B %s, got %s", stateB, snapshot.Payload)
	}
	genB := snapshot.Generation

	// Verify cloning: mutating original stateB doesn't affect Peek
	stateB[13] = '9' // Change "20" to "29"

	snapshotCheck, _ := c.Peek()
	if bytes.Equal(snapshotCheck.Payload, stateB) {
		t.Fatalf("payload was not cloned, mutation leaked into coalescer")
	}

	// 4. Write State C while we hold genB
	stateC := []byte(`{"uptime": 30}`)
	c.Update(stateC)

	// 5. Call Commit() with State B's generation
	// Should fail because State C was written
	committed := c.Commit(genB)
	if committed {
		t.Fatalf("expected commit to fail due to newer generation")
	}

	// State C must remain available for the next Peek()
	snapshotC, found := c.Peek()
	if !found {
		t.Fatalf("expected state C to remain")
	}
	if !bytes.Equal(snapshotC.Payload, stateC) {
		t.Fatalf("expected state C %s, got %s", stateC, snapshotC.Payload)
	}

	// A valid commit should clear the state
	committedC := c.Commit(snapshotC.Generation)
	if !committedC {
		t.Fatalf("expected valid commit to succeed")
	}

	_, foundAfterCommit := c.Peek()
	if foundAfterCommit {
		t.Fatalf("expected coalescer to be empty after valid commit")
	}
}

func TestStateCoalescer_PeekOwnership(t *testing.T) {
	c := NewStateCoalescer()

	originalState := []byte(`{"status": "ok"}`)
	c.Update(originalState)

	snapshot1, found := c.Peek()
	if !found {
		t.Fatalf("expected to find state")
	}

	// Mutate the returned payload
	snapshot1.Payload[13] = 'x' // `{"status": "ox"}`

	// Verify that the mutation did not affect the coalescer's internal state
	snapshot2, _ := c.Peek()
	if bytes.Equal(snapshot2.Payload, snapshot1.Payload) {
		t.Fatalf("mutation on Peek result leaked into coalescer: %s", snapshot2.Payload)
	}

	if !bytes.Equal(snapshot2.Payload, originalState) {
		t.Fatalf("internal state was corrupted: got %s, expected %s", snapshot2.Payload, originalState)
	}
}
