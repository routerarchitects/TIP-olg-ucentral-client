package queues

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestTCSCH001_PriorityOutboundOrdering(t *testing.T) {
	s := NewPriorityScheduler(10, 100)
	
	// Push 5 PriorityLow messages with distinct payloads to verify FIFO
	for i := 0; i < 5; i++ {
		err := s.Push(OutboundMessage{
			Priority:  PriorityLow,
			SessionID: fmt.Sprintf("low-%d", i),
		})
		if err != nil {
			t.Fatalf("unexpected error pushing low priority: %v", err)
		}
	}
	
	// Push 1 PriorityHighest
	err := s.Push(OutboundMessage{
		Priority:  PriorityHighest,
		SessionID: "highest-0",
	})
	if err != nil {
		t.Fatalf("unexpected error pushing highest priority: %v", err)
	}

	ctx := context.Background()
	
	// First should be PriorityHighest
	msg, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Priority != PriorityHighest || msg.SessionID != "highest-0" {
		t.Fatalf("expected PriorityHighest (highest-0), got %v (%s)", msg.Priority, msg.SessionID)
	}
	
	// Next 5 should be PriorityLow in FIFO order
	for i := 0; i < 5; i++ {
		msg, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expectedID := fmt.Sprintf("low-%d", i)
		if msg.Priority != PriorityLow || msg.SessionID != expectedID {
			t.Fatalf("expected PriorityLow (%s), got %v (%s)", expectedID, msg.Priority, msg.SessionID)
		}
	}
}

func TestTCSCH002_SchedulerBlockingAndWakeup(t *testing.T) {
	s := NewPriorityScheduler(10, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type result struct {
		msg OutboundMessage
		err error
	}
	ch := make(chan result)
	
	go func() {
		msg, err := s.Next(ctx)
		ch <- result{msg, err}
	}()

	// Wait briefly to ensure the goroutine is actually blocked
	select {
	case res := <-ch:
		t.Fatalf("Next() returned prematurely without blocking: %v, %v", res.msg, res.err)
	case <-time.After(100 * time.Millisecond):
		// Expected to remain blocked
	}

	// Push a message to unblock it
	err := s.Push(OutboundMessage{Priority: PriorityMedium})
	if err != nil {
		t.Fatalf("unexpected error pushing: %v", err)
	}

	// Now it should wake up
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("unexpected error from Next: %v", res.err)
		}
		if res.msg.Priority != PriorityMedium {
			t.Fatalf("expected PriorityMedium, got %v", res.msg.Priority)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for Next() to unblock after push")
	}
}

func TestTCSCH002_ContextCancellation(t *testing.T) {
	s := NewPriorityScheduler(10, 100)
	
	// Context with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// This should block until the context expires, then return an error
	_, err := s.Next(ctx)
	if err == nil {
		t.Fatalf("expected error from context cancellation, got nil")
	}
	if err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("expected DeadlineExceeded or Canceled, got: %v", err)
	}
}

func TestTCSCH003_Priority0EmergencyQueue(t *testing.T) {
	s := NewPriorityScheduler(10, 2) // emergencyCap = 2
	
	// Fill emergency queue
	for i := 0; i < 2; i++ {
		err := s.Push(OutboundMessage{Priority: PriorityHighest})
		if err != nil {
			t.Fatalf("unexpected error pushing to P0: %v", err)
		}
	}
	
	// Next push must fail
	err := s.Push(OutboundMessage{Priority: PriorityHighest})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestTCSCH004_NonBlockingPriority1Overflow(t *testing.T) {
	s := NewPriorityScheduler(2, 100) // capacity = 2
	
	// Fill P1 queue
	for i := 0; i < 2; i++ {
		err := s.Push(OutboundMessage{Priority: PriorityHigh})
		if err != nil {
			t.Fatalf("unexpected error pushing to P1: %v", err)
		}
	}
	
	// Next push must fail
	err := s.Push(OutboundMessage{Priority: PriorityHigh})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestTCSCH005_AntiStarvationYieldLimit(t *testing.T) {
	s := NewPriorityScheduler(10, 100)
	
	// Pre-fill 20 P0 and 5 P3
	for i := 0; i < 20; i++ {
		err := s.Push(OutboundMessage{Priority: PriorityHighest})
		if err != nil {
			t.Fatalf("unexpected error pushing P0: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		err := s.Push(OutboundMessage{Priority: PriorityLow})
		if err != nil {
			t.Fatalf("unexpected error pushing P3: %v", err)
		}
	}

	ctx := context.Background()
	
	// Should yield exactly 10 P0
	for i := 0; i < 10; i++ {
		msg, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("unexpected error on next: %v", err)
		}
		if msg.Priority != PriorityHighest {
			t.Fatalf("expected P0, got %v at index %d", msg.Priority, i)
		}
	}
	
	// Next must be P3 (Anti-starvation)
	msg, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("unexpected error on next: %v", err)
	}
	if msg.Priority != PriorityLow {
		t.Fatalf("expected P3 due to anti-starvation, got %v", msg.Priority)
	}
	
	// Should yield remaining 10 P0
	for i := 0; i < 10; i++ {
		msg, err = s.Next(ctx)
		if err != nil {
			t.Fatalf("unexpected error on next: %v", err)
		}
		if msg.Priority != PriorityHighest {
			t.Fatalf("expected P0, got %v at index %d", msg.Priority, i)
		}
	}
	
	// Finally, drain remaining 4 P3
	for i := 0; i < 4; i++ {
		msg, err = s.Next(ctx)
		if err != nil {
			t.Fatalf("unexpected error on next: %v", err)
		}
		if msg.Priority != PriorityLow {
			t.Fatalf("expected P3, got %v at index %d", msg.Priority, i)
		}
	}
}

func TestTCSCH006_NonBlockingPriority2and3Overflow(t *testing.T) {
	s := NewPriorityScheduler(2, 100)
	
	// Priority 2
	for i := 0; i < 2; i++ {
		err := s.Push(OutboundMessage{Priority: PriorityMedium})
		if err != nil {
			t.Fatalf("unexpected error pushing to P2: %v", err)
		}
	}
	err := s.Push(OutboundMessage{Priority: PriorityMedium})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull for P2, got %v", err)
	}
	
	// Priority 3
	for i := 0; i < 2; i++ {
		err = s.Push(OutboundMessage{Priority: PriorityLow})
		if err != nil {
			t.Fatalf("unexpected error pushing to P3: %v", err)
		}
	}
	err = s.Push(OutboundMessage{Priority: PriorityLow})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull for P3, got %v", err)
	}
}

func TestPriorityScheduler_InvalidPriority(t *testing.T) {
	s := NewPriorityScheduler(2, 2)
	err := s.Push(OutboundMessage{Priority: 99})
	if err != ErrInvalidPriority {
		t.Fatalf("expected ErrInvalidPriority, got %v", err)
	}
}
