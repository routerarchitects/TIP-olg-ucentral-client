package queues

import (
	"context"
	"sync"
)

// NATSDispatchBuffer buffers commands headed for NATS. Rejects immediately when full.
type NATSDispatchBuffer struct {
	ch chan []byte
}

// NewNATSDispatchBuffer creates a new NATSDispatchBuffer.
func NewNATSDispatchBuffer(capacity int) *NATSDispatchBuffer {
	return &NATSDispatchBuffer{
		ch: make(chan []byte, capacity),
	}
}

// Push adds a command payload to the dispatch buffer without blocking.
// Returns ErrQueueFull if the buffer is at capacity.
func (d *NATSDispatchBuffer) Push(payload []byte) error {
	select {
	case d.ch <- payload:
		return nil
	default:
		return ErrQueueFull
	}
}

// Pop blocks until a payload is available or the context is cancelled.
func (d *NATSDispatchBuffer) Pop(ctx context.Context) ([]byte, error) {
	select {
	case payload := <-d.ch:
		return payload, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// CommandResultQueue acts as a bounded, high-priority ingress buffer for JSON-RPC
// command execution results arriving from the downstream NATS agents.
type CommandResultQueue struct {
	mu       sync.Mutex
	items    [][]byte
	capacity int
}

// NewCommandResultQueue creates a new CommandResultQueue.
func NewCommandResultQueue(capacity int) *CommandResultQueue {
	return &CommandResultQueue{
		items:    make([][]byte, 0, capacity),
		capacity: capacity,
	}
}

// Push adds a payload to the result queue. Returns ErrQueueFull if at capacity.
func (q *CommandResultQueue) Push(payload []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) >= q.capacity {
		return ErrQueueFull
	}

	cloned := make([]byte, len(payload))
	copy(cloned, payload)

	q.items = append(q.items, cloned)
	return nil
}

// Pop retrieves and removes the oldest payload from the result queue.
func (q *CommandResultQueue) Pop() ([]byte, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return nil, false
	}

	payload := q.items[0]
	q.items[0] = nil

	if len(q.items) == 1 {
		q.items = nil
	} else {
		q.items = q.items[1:]
	}

	return payload, true
}

// Utilization returns the current usage ratio (0.0 to 1.0).
func (q *CommandResultQueue) Utilization() float64 {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.capacity == 0 {
		return 1.0
	}

	return float64(len(q.items)) / float64(q.capacity)
}
