package queues

import (
	"sync"
)

// TelemetryRingBuffer represents a bounded FIFO queue for low-priority telemetry
type TelemetryRingBuffer struct {
	mu       sync.Mutex
	buffer   [][]byte
	capacity int
	head     int
	tail     int
	size     int
}

// NewTelemetryRingBuffer creates a new ring buffer with the given capacity.
func NewTelemetryRingBuffer(capacity int) *TelemetryRingBuffer {
	if capacity <= 0 {
		panic("TelemetryRingBuffer capacity must be strictly positive")
	}
	return &TelemetryRingBuffer{
		buffer:   make([][]byte, capacity),
		capacity: capacity,
	}
}

// Push adds a new telemetry payload to the ring buffer.
// If the buffer is full, the oldest item is overwritten (FIFO drop).
// It returns true if an item was dropped.
func (b *TelemetryRingBuffer) Push(payload []byte) (dropped bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Clone payload to transfer ownership and prevent memory corruption.
	cloned := make([]byte, len(payload))
	copy(cloned, payload)

	if b.size == b.capacity {
		dropped = true
		// Overwrite the oldest element (at head)
		b.buffer[b.head] = cloned
		b.head = (b.head + 1) % b.capacity
		b.tail = (b.tail + 1) % b.capacity
	} else {
		b.buffer[b.tail] = cloned
		b.tail = (b.tail + 1) % b.capacity
		b.size++
	}

	return dropped
}

// Pop retrieves the oldest telemetry payload from the ring buffer.
// Returns the payload and true if an item was found, otherwise false.
func (b *TelemetryRingBuffer) Pop() ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.size == 0 {
		return nil, false
	}

	msg := b.buffer[b.head]
	b.buffer[b.head] = nil // Avoid memory leak
	b.head = (b.head + 1) % b.capacity
	b.size--

	return msg, true
}
