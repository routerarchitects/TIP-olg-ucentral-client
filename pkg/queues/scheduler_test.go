package queues

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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

	var pushedCount, receivedCount int32

	// Launch 50 writers and 50 readers concurrently
	for i := 0; i < 50; i++ {
		wg.Add(2) // 1 writer, 1 reader

		// Writer
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				err := s.Push(OutboundMessage{Priority: PriorityHigh})
				if err == nil {
					atomic.AddInt32(&pushedCount, 1)
				} else if err != ErrQueueFull {
					errChan <- fmt.Errorf("unexpected push error: %w", err)
				}
				time.Sleep(time.Microsecond)
			}
		}()

		// Reader
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				_, err := s.Next(ctx)
				cancel()
				if err == nil {
					atomic.AddInt32(&receivedCount, 1)
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
		_, err := s.Next(ctxTimeout)
		cancel()
		if err != nil {
			break
		}
		atomic.AddInt32(&receivedCount, 1)
	}

	finalPushed := atomic.LoadInt32(&pushedCount)
	finalReceived := atomic.LoadInt32(&receivedCount)

	if finalPushed == 0 {
		t.Fatalf("Test invalid: 0 messages were successfully pushed")
	}
	if finalPushed != finalReceived {
		t.Fatalf("Message count mismatch: pushed %d, received %d", finalPushed, finalReceived)
	}
}

func TestPriorityScheduler_CancelledConsumerWakeupRace(t *testing.T) {
	s := NewPriorityScheduler(10, 100)

	for i := 0; i < 50; i++ {
		ctxB, cancelB := context.WithTimeout(context.Background(), 2*time.Second)
		
		var cancelFuncs []context.CancelFunc
		for j := 0; j < 5; j++ {
			ctxA, cancelA := context.WithCancel(context.Background())
			cancelFuncs = append(cancelFuncs, cancelA)
			go func(ctx context.Context) {
				_, _ = s.Next(ctx)
			}(ctxA)
		}

		msgChan := make(chan OutboundMessage, 1)
		go func() {
			msg, err := s.Next(ctxB)
			if err == nil {
				msgChan <- msg
			}
		}()

		// Give them time to block on cond.Wait()
		time.Sleep(10 * time.Millisecond)

		// Concurrently cancel all A's and push a message to trigger the race
		for _, cancel := range cancelFuncs {
			go cancel()
		}
		
		err := s.Push(OutboundMessage{Priority: PriorityHigh, SessionID: "wakeup-test"})
		if err != nil {
			t.Fatalf("unexpected error pushing: %v", err)
		}

		// Verify Consumer B receives it
		select {
		case msg := <-msgChan:
			if msg.SessionID != "wakeup-test" {
				t.Fatalf("expected wakeup-test, got %v", msg.SessionID)
			}
		case <-time.After(50 * time.Millisecond):
			s.mu.Lock()
			left := len(s.queues[PriorityHigh])
			s.mu.Unlock()
			if left > 0 {
				t.Fatalf("Consumer B never woke up and message is stuck in queue - wakeup was swallowed on iteration %d", i)
			}
			// If left == 0, one of the A consumers grabbed it before its context cancellation fully propagated.
			// This is a normal Go scheduler race and does not indicate a swallowed wakeup.
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
