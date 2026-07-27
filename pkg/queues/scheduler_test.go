package queues

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestTCSCH001_PriorityOutboundOrdering(t *testing.T) {
	s := NewPriorityScheduler(10, 100)

	// Push 3 messages for each priority level, in reverse order to prove sorting
	priorities := []Priority{PriorityLow, PriorityMedium, PriorityHigh, PriorityHighest}
	for _, p := range priorities {
		for i := 0; i < 3; i++ {
			err := s.Push(OutboundMessage{
				Priority:  p,
				SessionID: fmt.Sprintf("p%d-%d", p, i),
			})
			if err != nil {
				t.Fatalf("unexpected error pushing priority %v: %v", p, err)
			}
		}
	}

	ctx := context.Background()

	// They should pop in strict priority order (0, 1, 2, 3),
	// and within each priority, strictly FIFO (0, 1, 2)
	orderedPriorities := []Priority{PriorityHighest, PriorityHigh, PriorityMedium, PriorityLow}
	for _, p := range orderedPriorities {
		for i := 0; i < 3; i++ {
			msg, err := s.Next(ctx)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expectedID := fmt.Sprintf("p%d-%d", p, i)
			if msg.Priority != p || msg.SessionID != expectedID {
				t.Fatalf("expected Priority %v (%s), got Priority %v (%s)", p, expectedID, msg.Priority, msg.SessionID)
			}
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

	// Pre-fill 30 P0, 1 P1, 1 P2, 1 P3
	for i := 0; i < 30; i++ {
		err := s.Push(OutboundMessage{Priority: PriorityHighest})
		if err != nil {
			t.Fatalf("unexpected error pushing P0: %v", err)
		}
	}

	for p := PriorityHigh; p <= PriorityLow; p++ {
		err := s.Push(OutboundMessage{Priority: p})
		if err != nil {
			t.Fatalf("unexpected error pushing P%d: %v", p, err)
		}
	}

	ctx := context.Background()

	// Should yield 10 P0s -> 1 P1 -> 10 P0s -> 1 P2 -> 10 P0s -> 1 P3
	expectedPattern := []Priority{PriorityHigh, PriorityMedium, PriorityLow}

	for _, expectedFallback := range expectedPattern {
		// Yield 10 P0s
		for i := 0; i < 10; i++ {
			msg, err := s.Next(ctx)
			if err != nil {
				t.Fatalf("unexpected error on next: %v", err)
			}
			if msg.Priority != PriorityHighest {
				t.Fatalf("expected P0, got %v", msg.Priority)
			}
		}

		// Yield the fallback
		msg, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("unexpected error on next fallback: %v", err)
		}
		if msg.Priority != expectedFallback {
			t.Fatalf("expected fallback %v, got %v", expectedFallback, msg.Priority)
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

func TestPriorityScheduler_InvalidCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewPriorityScheduler did not panic on 0 capacity")
		}
	}()
	_ = NewPriorityScheduler(0, 100)
}

func TestPriorityScheduler_InvalidEmergencyCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewPriorityScheduler did not panic on negative emergency capacity")
		}
	}()
	_ = NewPriorityScheduler(10, -1)
}

func TestPriorityScheduler_ConcurrencyStress(t *testing.T) {
	s := NewPriorityScheduler(100, 100)

	// Launch 50 writers and 50 readers concurrently, aggressively
	// cancelling contexts to trigger the select race condition.
	done := make(chan struct{})

	for i := 0; i < 50; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_ = s.Push(OutboundMessage{Priority: PriorityHigh})
				time.Sleep(1 * time.Millisecond) // Ensure context watchers have time to start
			}
		}()

		go func() {
			for j := 0; j < 100; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
				_, _ = s.Next(ctx)
				cancel()
			}
			done <- struct{}{}
		}()
	}

	// Wait for readers to finish
	for i := 0; i < 50; i++ {
		<-done
	}
}
