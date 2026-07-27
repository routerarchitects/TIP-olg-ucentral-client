package queues

import (
	"context"
	"errors"
	"sync"
)

type Priority int

const (
	PriorityHighest Priority = 0 // JSON-RPC command responses
	PriorityHigh    Priority = 1 // Audit logs, crashlogs, health snapshots
	PriorityMedium  Priority = 2 // Coalesced states
	PriorityLow     Priority = 3 // Telemetry events, standard logs
)

type OutboundMessage struct {
	SessionID string
	Priority  Priority
	Payload   []byte
}

var ErrQueueFull = errors.New("queue is at maximum capacity")
var ErrInvalidPriority = errors.New("invalid message priority")

type OutboundScheduler interface {
	Push(msg OutboundMessage) error
	Next(ctx context.Context) (OutboundMessage, error)
}

type PriorityScheduler struct {
	mu            sync.Mutex
	cond          *sync.Cond
	queues        [4][]OutboundMessage
	capacity      int // maximum entries for Priority 1, 2, and 3
	emergencyCap  int // maximum entries for the Priority 0 emergency queue
	consecutiveP0 int
}

func NewPriorityScheduler(capacity int, emergencyCap int) *PriorityScheduler {
	if capacity <= 0 || emergencyCap <= 0 {
		panic("queues: capacities must be greater than 0")
	}

	s := &PriorityScheduler{
		capacity:     capacity,
		emergencyCap: emergencyCap,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *PriorityScheduler) isFull(priority Priority) bool {
	if priority == PriorityHighest {
		return len(s.queues[priority]) >= s.emergencyCap
	}
	return len(s.queues[priority]) >= s.capacity
}

// Push adds a message to the appropriate priority queue.
// Important: Push does NOT clone the message payload. To avoid data races,
// callers must not mutate the payload after it has been passed to the scheduler.
func (s *PriorityScheduler) Push(msg OutboundMessage) error {
	if msg.Priority < PriorityHighest || msg.Priority > PriorityLow {
		return ErrInvalidPriority
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isFull(msg.Priority) {
		return ErrQueueFull
	}

	s.queues[msg.Priority] = append(s.queues[msg.Priority], msg)
	s.cond.Broadcast() // Broadcast prevents a cancelled consumer from swallowing the wakeup
	return nil
}

func (s *PriorityScheduler) Next(ctx context.Context) (OutboundMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if ctx.Err() != nil {
			return OutboundMessage{}, ctx.Err()
		}

		if msg, found := s.tryDequeue(); found {
			return msg, nil
		}

		// No messages found, spin up watcher and go to sleep.
		// Note on Safety: If ctx.Done() fires, it locks s.mu and Broadcasts.
		// If Wait() returns normally, close(ctxDone) unblocks the select.
		// If both happen simultaneously, select exclusively picks one case.
		// If <-ctx.Done() is picked, the close(ctxDone) executed later is harmless
		// because the watcher has already safely exited. No deadlocks or leaks can occur.
		ctxDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				s.mu.Lock()
				s.cond.Broadcast()
				s.mu.Unlock()
			case <-ctxDone:
			}
		}()

		s.cond.Wait()
		close(ctxDone)
	}
}

// tryDequeue attempts to select a message according to priority and anti-starvation rules.
// The caller must hold s.mu.
func (s *PriorityScheduler) tryDequeue() (OutboundMessage, bool) {
	if s.consecutiveP0 >= 10 {
		// Anti-starvation: check queues 1, 2, 3
		for p := PriorityHigh; p <= PriorityLow; p++ {
			if len(s.queues[p]) > 0 {
				msg := pop(&s.queues[p])
				s.consecutiveP0 = 0
				return msg, true
			}
		}
	}

	// Normal selection
	for p := PriorityHighest; p <= PriorityLow; p++ {
		if len(s.queues[p]) > 0 {
			msg := pop(&s.queues[p])
			if p == PriorityHighest {
				s.consecutiveP0++
			} else {
				s.consecutiveP0 = 0
			}
			return msg, true
		}
	}

	return OutboundMessage{}, false
}

func pop(queue *[]OutboundMessage) OutboundMessage {
	msg := (*queue)[0]
	(*queue)[0] = OutboundMessage{} // Overwrite to prevent memory leak
	*queue = (*queue)[1:]
	return msg
}
