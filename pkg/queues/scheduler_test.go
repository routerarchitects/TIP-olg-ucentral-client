package queues

import (
	"context"
	"testing"
	"time"
)

func TestPriorityScheduler_Ordering(t *testing.T) {
	s := NewPriorityScheduler(10, 100)
	for i := 0; i < 5; i++ {
		err := s.Push(OutboundMessage{Priority: PriorityLow})
		if err != nil {
			t.Fatal(err)
		}
	}
	err := s.Push(OutboundMessage{Priority: PriorityHighest})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	msg, err := s.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Priority != PriorityHighest {
		t.Fatalf("expected PriorityHighest, got %v", msg.Priority)
	}
}

func TestPriorityScheduler_AntiStarvation(t *testing.T) {
	s := NewPriorityScheduler(10, 100)
	for i := 0; i < 20; i++ {
		s.Push(OutboundMessage{Priority: PriorityHighest})
	}
	for i := 0; i < 5; i++ {
		s.Push(OutboundMessage{Priority: PriorityLow})
	}

	ctx := context.Background()

	// Should get 10 P0
	for i := 0; i < 10; i++ {
		msg, _ := s.Next(ctx)
		if msg.Priority != PriorityHighest {
			t.Fatalf("expected 10 P0, got %v at index %d", msg.Priority, i)
		}
	}

	// Should get 1 P3
	msg, _ := s.Next(ctx)
	if msg.Priority != PriorityLow {
		t.Fatalf("expected 1 P3 due to anti-starvation, got %v", msg.Priority)
	}

	// Should get remaining 10 P0
	for i := 0; i < 10; i++ {
		msg, _ := s.Next(ctx)
		if msg.Priority != PriorityHighest {
			t.Fatalf("expected 10 P0, got %v at index %d", msg.Priority, i)
		}
	}
}

func TestPriorityScheduler_BlockingWakeup(t *testing.T) {
	s := NewPriorityScheduler(10, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := make(chan OutboundMessage)
	go func() {
		msg, _ := s.Next(ctx)
		ch <- msg
	}()

	time.Sleep(100 * time.Millisecond)
	s.Push(OutboundMessage{Priority: PriorityMedium})

	select {
	case msg := <-ch:
		if msg.Priority != PriorityMedium {
			t.Fatalf("expected PriorityMedium")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for wakeup")
	}
}

func TestPriorityScheduler_Overflow(t *testing.T) {
	s := NewPriorityScheduler(2, 2)

	s.Push(OutboundMessage{Priority: PriorityHighest})
	s.Push(OutboundMessage{Priority: PriorityHighest})
	err := s.Push(OutboundMessage{Priority: PriorityHighest})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	s.Push(OutboundMessage{Priority: PriorityLow})
	s.Push(OutboundMessage{Priority: PriorityLow})
	err = s.Push(OutboundMessage{Priority: PriorityLow})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}
