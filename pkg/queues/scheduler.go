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
	s := &PriorityScheduler{
		capacity:     capacity,
		emergencyCap: emergencyCap,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *PriorityScheduler) Push(msg OutboundMessage) error {
	if msg.Priority < PriorityHighest || msg.Priority > PriorityLow {
		return ErrInvalidPriority
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if msg.Priority == PriorityHighest {
		if len(s.queues[msg.Priority]) >= s.emergencyCap {
			return ErrQueueFull
		}
	} else {
		if len(s.queues[msg.Priority]) >= s.capacity {
			return ErrQueueFull
		}
	}

	s.queues[msg.Priority] = append(s.queues[msg.Priority], msg)
	s.cond.Signal()
	return nil
}

func (s *PriorityScheduler) Next(ctx context.Context) (OutboundMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ctx.Err() != nil {
		return OutboundMessage{}, ctx.Err()
	}

	ctxDone := make(chan struct{})
	defer close(ctxDone)

	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-ctxDone:
		}
	}()

	for {
		if ctx.Err() != nil {
			return OutboundMessage{}, ctx.Err()
		}

		if s.consecutiveP0 >= 10 {
			// Anti-starvation: check queues 1, 2, 3
			for p := PriorityHigh; p <= PriorityLow; p++ {
				if len(s.queues[p]) > 0 {
					msg := pop(&s.queues[p])
					s.consecutiveP0 = 0
					return msg, nil
				}
			}
			// If 1, 2, 3 are empty, we can just fall through and pick from 0 if it has items
		}

		// Normal selection
		found := false
		var msg OutboundMessage
		for p := PriorityHighest; p <= PriorityLow; p++ {
			if len(s.queues[p]) > 0 {
				msg = pop(&s.queues[p])
				if p == PriorityHighest {
					s.consecutiveP0++
				} else {
					s.consecutiveP0 = 0
				}
				found = true
				break
			}
		}

		if found {
			return msg, nil
		}

		s.cond.Wait()
	}
}

func pop(queue *[]OutboundMessage) OutboundMessage {
	msg := (*queue)[0]
	(*queue)[0] = OutboundMessage{} // Overwrite to prevent memory leak
	*queue = (*queue)[1:]
	return msg
}
