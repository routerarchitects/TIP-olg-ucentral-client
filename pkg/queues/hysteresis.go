package queues

import "sync"

// UtilizationProvider defines anything that can report how full it is (0.0 to 1.0)
type UtilizationProvider interface {
	Utilization() float64
}

// HysteresisMonitor tracks backpressure state using high/low watermarks.
type HysteresisMonitor struct {
	mu           sync.Mutex
	provider     UtilizationProvider
	isThrottled  bool
	upperPercent float64
	lowerPercent float64
}

// NewHysteresisMonitor creates a new HysteresisMonitor.
func NewHysteresisMonitor(provider UtilizationProvider, upper, lower float64) *HysteresisMonitor {
	return &HysteresisMonitor{
		provider:     provider,
		upperPercent: upper,
		lowerPercent: lower,
	}
}

// IsThrottled returns the current state, flipping only when thresholds are crossed.
func (m *HysteresisMonitor) IsThrottled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	util := m.provider.Utilization()

	if m.isThrottled {
		if util <= m.lowerPercent {
			m.isThrottled = false // Recovered
		}
	} else {
		if util >= m.upperPercent {
			m.isThrottled = true // Overwhelmed
		}
	}

	return m.isThrottled
}
