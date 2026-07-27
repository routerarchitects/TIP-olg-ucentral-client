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

	// Payload ownership transfers to the scheduler after a successful Push.
	// The producer must not modify or reuse the backing array afterward.
	// Ownership transfers again to the caller of Next when the message is dequeued.
	Payload []byte
}

var ErrQueueFull = errors.New("queue is at maximum capacity")
var ErrInvalidPriority = errors.New("invalid message priority")

type OutboundScheduler interface {
	// Push transfers ownership only when it returns nil.
	// On error, ownership remains with the caller.
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
	lastYield     Priority
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

// Push appends a message to the appropriate priority queue.
// It implements OutboundScheduler.Push.
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

	// Register a cancellation callback to wake up the condition variable.
	// Safety note: `stop()` does not block waiting for the callback to finish.
	// If a successful dequeue races with context cancellation, `stop()` returns
	// immediately, and the deferred `s.mu.Unlock()` executes. The callback will
	// then acquire the lock and perform a harmless spurious Broadcast().
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()

	for {
		if ctx.Err() != nil {
			return OutboundMessage{}, ctx.Err()
		}

		if msg, found := s.tryDequeue(); found {
			return msg, nil
		}

		// No messages found, wait for a signal.
		s.cond.Wait()
	}
}

// tryDequeue attempts to select a message according to priority and anti-starvation rules.
// The caller must hold s.mu.
func (s *PriorityScheduler) tryDequeue() (OutboundMessage, bool) {
	if s.consecutiveP0 >= 10 {
		// Anti-starvation: round-robin check queues 1, 2, 3
		for i := 1; i <= 3; i++ {
			p := Priority(int(s.lastYield)%3 + 1)
			s.lastYield = p
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

	if len(*queue) == 1 {
		*queue = nil // Release the backing array when empty
	} else {
		*queue = (*queue)[1:]
	}

	return msg
}
