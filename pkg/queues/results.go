package queues

import (
	"context"
	"errors"
	"sync"
)

// ErrQueueClosed is returned when a queue operation is attempted on a closed queue.
var ErrQueueClosed = errors.New("queue is closed")

// NATSDispatchBuffer buffers commands headed for NATS. Rejects immediately when full.
type NATSDispatchBuffer struct {
	ch chan []byte
}

// NewNATSDispatchBuffer creates a new NATSDispatchBuffer.
func NewNATSDispatchBuffer(capacity int) *NATSDispatchBuffer {
	if capacity <= 0 {
		panic("NATSDispatchBuffer capacity must be strictly positive")
	}
	return &NATSDispatchBuffer{
		ch: make(chan []byte, capacity),
	}
}

// Push adds a command payload to the dispatch buffer without blocking.
// Returns ErrQueueFull if the buffer is at capacity.
func (d *NATSDispatchBuffer) Push(payload []byte) error {
	// Fast-fail pre-check to avoid unnecessary allocations during sustained saturation.
	// NOTE: Per the specification's struct definition, we accept the rare race condition 
	// where the queue fills between this check and the clone, causing a wasted allocation.
	// We optimize for the common sustained-saturation case without altering the spec.
	if len(d.ch) == cap(d.ch) {
		return ErrQueueFull
	}

	cloned := make([]byte, len(payload))
	copy(cloned, payload)

	select {
	case d.ch <- cloned:
		return nil
	default:
		return ErrQueueFull
	}
}

// Pop blocks until a payload is available or the context is cancelled.
func (d *NATSDispatchBuffer) Pop(ctx context.Context) ([]byte, error) {
	select {
	case payload, ok := <-d.ch:
		if !ok {
			// This branch isn't currently reachable since we don't expose a Close(),
			// but it's defensively correct if Close() is added in the future.
			return nil, ErrQueueClosed
		}
		return payload, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// CommandResultQueue acts as a bounded, high-priority ingress buffer for JSON-RPC
// command execution results arriving from the downstream NATS agents.
// It is strictly non-blocking on consumers and network I/O, meaning it
// immediately returns ErrQueueFull when at capacity (though it uses a fast-path mutex internally).
type CommandResultQueue struct {
	mu       sync.Mutex
	items    [][]byte
	capacity int
}

// NewCommandResultQueue creates a new CommandResultQueue.
func NewCommandResultQueue(capacity int) *CommandResultQueue {
	if capacity <= 0 {
		panic("CommandResultQueue capacity must be strictly positive")
	}
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

	return float64(len(q.items)) / float64(q.capacity)
}
