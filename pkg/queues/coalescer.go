package queues

import (
	"sync"
)

// StateSnapshot holds the state payload and its generation
type StateSnapshot struct {
	Payload    []byte
	Generation uint64
}

// StateCoalescer implements last-write-wins in-memory state storage with generation tracking
type StateCoalescer struct {
	mu          sync.Mutex
	latestState []byte
	generation  uint64
	hasState    bool
}

// NewStateCoalescer creates a new StateCoalescer.
func NewStateCoalescer() *StateCoalescer {
	return &StateCoalescer{}
}

// Update sets the latest state payload and increments the generation counter.
// It clones the payload to take ownership.
func (c *StateCoalescer) Update(payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cloned := make([]byte, len(payload))
	copy(cloned, payload)

	c.latestState = cloned
	c.generation++
	c.hasState = true
}

// Peek returns a copy of the current state snapshot without consuming it.
// Returns false if there is no state available.
func (c *StateCoalescer) Peek() (StateSnapshot, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.hasState {
		return StateSnapshot{}, false
	}

	return StateSnapshot{
		Payload:    c.latestState,
		Generation: c.generation,
	}, true
}

// Commit clears the state if the generation matches the provided generation.
// It returns true if the commit was successful, false if a newer update exists.
func (c *StateCoalescer) Commit(generation uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.hasState {
		return false
	}

	if c.generation != generation {
		return false // newer update exists
	}

	c.latestState = nil
	c.hasState = false
	return true
}
