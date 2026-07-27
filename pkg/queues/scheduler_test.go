package queues

import (
	"context"
	"fmt"
	"sync"
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
	s := NewPriorityScheduler(1000, 1000)

	var wg sync.WaitGroup
	errChan := make(chan error, 10000)

	sentIDs := make(map[string]bool)
	receivedIDs := make(map[string]bool)
	var mapMu sync.Mutex

	// Launch 50 writers and 50 readers concurrently
	for i := 0; i < 50; i++ {
		wg.Add(2) // 1 writer, 1 reader

		// Writer
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				sessionID := fmt.Sprintf("w%d-m%d", writerID, j)
				err := s.Push(OutboundMessage{Priority: PriorityHigh, SessionID: sessionID})
				if err == nil {
					mapMu.Lock()
					sentIDs[sessionID] = true
					mapMu.Unlock()
				} else if err != ErrQueueFull {
					errChan <- fmt.Errorf("unexpected push error: %w", err)
				}
				time.Sleep(time.Microsecond)
			}
		}(i)

		// Reader
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				msg, err := s.Next(ctx)
				cancel()
				if err == nil {
					mapMu.Lock()
					if receivedIDs[msg.SessionID] {
						errChan <- fmt.Errorf("duplicate message received: %s", msg.SessionID)
					}
					receivedIDs[msg.SessionID] = true
					mapMu.Unlock()
				} else if err != context.DeadlineExceeded && err != context.Canceled {
					errChan <- fmt.Errorf("unexpected next error: %w", err)
				}
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrency error: %v", err)
	}

	// Drain remaining messages
	ctx := context.Background()
	for {
		ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		msg, err := s.Next(ctxTimeout)
		cancel()
		if err != nil {
			break
		}
		mapMu.Lock()
		if receivedIDs[msg.SessionID] {
			t.Errorf("duplicate message received in drain: %s", msg.SessionID)
		}
		receivedIDs[msg.SessionID] = true
		mapMu.Unlock()
	}

	mapMu.Lock()
	defer mapMu.Unlock()

	if len(sentIDs) == 0 {
		t.Fatalf("Test invalid: 0 messages were successfully pushed")
	}

	for id := range sentIDs {
		if !receivedIDs[id] {
			t.Errorf("Message %s was pushed but never received", id)
		}
	}
	for id := range receivedIDs {
		if !sentIDs[id] {
			t.Errorf("Message %s was received but never pushed", id)
		}
	}
}

func TestPriorityScheduler_CancelledConsumerWakeupRace(t *testing.T) {
	s := NewPriorityScheduler(10, 100)

	for i := 0; i < 200; i++ {
		ctxA, cancelA := context.WithCancel(context.Background())
		ctxB, cancelB := context.WithCancel(context.Background())

		aDone := make(chan error, 1)
		go func() {
			_, err := s.Next(ctxA)
			aDone <- err
		}()

		msgChan := make(chan OutboundMessage, 1)
		go func() {
			msg, _ := s.Next(ctxB)
			msgChan <- msg
		}()

		// 1. Confirm both consumers are blocked
		time.Sleep(5 * time.Millisecond)

		// 2. Ensure Consumer A’s context is cancelled just before the message is pushed.
		// Note: This test verifies that Consumer B receives the message without a swallowed wakeup,
		// but it does not fully distinguish between Push() using Broadcast() vs Signal() because
		// Consumer A's own context-watcher goroutine also calls Broadcast().
		// We use Broadcast() in Push() as defense-in-depth against scheduling races.
		cancelA()

		err := s.Push(OutboundMessage{Priority: PriorityHigh, SessionID: "wakeup-test"})
		if err != nil {
			t.Fatalf("unexpected error pushing: %v", err)
		}

		// 3. Confirm Consumer A returns context.Canceled
		if err := <-aDone; err != context.Canceled {
			t.Fatalf("Consumer A should have returned Canceled, got %v", err)
		}

		// 4. Confirm Consumer B receives the message
		select {
		case msg := <-msgChan:
			if msg.SessionID != "wakeup-test" {
				t.Fatalf("expected wakeup-test, got %v", msg.SessionID)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("Consumer B never woke up - wakeup was swallowed on iteration %d", i)
		}

		cancelB()
	}
}

func TestPriorityScheduler_PayloadCloning(t *testing.T) {
	s := NewPriorityScheduler(10, 100)

	originalBytes := []byte("hello")
	err := s.Push(OutboundMessage{
		Priority: PriorityHigh,
		Payload:  originalBytes,
	})
	if err != nil {
		t.Fatalf("unexpected error pushing message: %v", err)
	}

	// Mutate the original buffer
	originalBytes[0] = 'H'

	// Verify the queued message was cloned and not mutated
	msg, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("unexpected error on next: %v", err)
	}

	if string(msg.Payload) != "hello" {
		t.Fatalf("expected payload 'hello', got '%s'", string(msg.Payload))
	}
}
