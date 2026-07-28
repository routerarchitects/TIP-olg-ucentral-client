package queues

import (
	"bytes"
	"context"
	"testing"
)

// TC-BUF-003 (NATS Dispatch Buffer Busy Rejection) - Part A
func TestTCBUF003_NATSDispatchBuffer_BusyRejection(t *testing.T) {
	d := NewNATSDispatchBuffer(2)

	err := d.Push([]byte("cmd 1"))
	if err != nil {
		t.Fatalf("unexpected error on push 1: %v", err)
	}

	err = d.Push([]byte("cmd 2"))
	if err != nil {
		t.Fatalf("unexpected error on push 2: %v", err)
	}

	err = d.Push([]byte("cmd 3"))
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull on push 3, got %v", err)
	}

	// Pop should work
	payload, err := d.Pop(context.Background())
	if err != nil || string(payload) != "cmd 1" {
		t.Fatalf("expected cmd 1, got %v (err: %v)", string(payload), err)
	}
}

// TC-BUF-006 (Command Result Queue Non-Blocking)
func TestTCBUF006_CommandResultQueue_NonBlocking(t *testing.T) {
	q := NewCommandResultQueue(2)

	err := q.Push([]byte("result 1"))
	if err != nil {
		t.Fatalf("unexpected error on push 1: %v", err)
	}

	err = q.Push([]byte("result 2"))
	if err != nil {
		t.Fatalf("unexpected error on push 2: %v", err)
	}

	err = q.Push([]byte("result 3"))
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull on push 3, got %v", err)
	}

	// Utilization
	if q.Utilization() != 1.0 {
		t.Fatalf("expected utilization 1.0, got %f", q.Utilization())
	}

	// Pop
	payload, found := q.Pop()
	if !found || string(payload) != "result 1" {
		t.Fatalf("expected result 1, got %v (found=%v)", string(payload), found)
	}

	// Utilization after pop
	if q.Utilization() != 0.5 {
		t.Fatalf("expected utilization 0.5, got %f", q.Utilization())
	}

	// Pop remaining
	_, _ = q.Pop()
	_, found = q.Pop()
	if found {
		t.Fatalf("expected empty queue")
	}
	if q.Utilization() != 0.0 {
		t.Fatalf("expected utilization 0.0, got %f", q.Utilization())
	}
}

func TestCommandResultQueue_PushOwnership(t *testing.T) {
	q := NewCommandResultQueue(1)

	originalData := []byte(`{"result": "success"}`)
	err := q.Push(originalData)
	if err != nil {
		t.Fatalf("failed to push: %v", err)
	}

	// Mutate the original slice
	originalData[13] = 'f'
	originalData[14] = 'a'
	originalData[15] = 'i'
	originalData[16] = 'l'

	// Pop and verify
	popped, found := q.Pop()
	if !found {
		t.Fatalf("expected to find item")
	}

	expectedData := []byte(`{"result": "success"}`)
	if bytes.Equal(popped, originalData) {
		t.Fatalf("mutation on input slice leaked into the queue: %s", popped)
	}
	if !bytes.Equal(popped, expectedData) {
		t.Fatalf("queue data was corrupted: got %s, expected %s", popped, expectedData)
	}
}

func TestCommandResultQueue_InvalidCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on zero capacity")
		}
	}()
	NewCommandResultQueue(0)
}

func TestNATSDispatchBuffer_InvalidCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on zero capacity")
		}
	}()
	NewNATSDispatchBuffer(-1)
}
