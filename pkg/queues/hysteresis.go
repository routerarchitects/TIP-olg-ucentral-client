package queues

import (
	"math"
	"sync"
)

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
	if provider == nil {
		panic("HysteresisMonitor requires a non-nil UtilizationProvider")
	}
	if math.IsNaN(upper) || math.IsNaN(lower) {
		panic("HysteresisMonitor thresholds cannot be NaN")
	}
	if upper <= lower {
		panic("HysteresisMonitor upper threshold must be strictly greater than lower threshold")
	}
	if lower < 0.0 || upper > 1.0 {
		panic("HysteresisMonitor thresholds must be within [0.0, 1.0]")
	}

	return &HysteresisMonitor{
		provider:     provider,
		upperPercent: upper,
		lowerPercent: lower,
	}
}

// IsThrottled returns the current state, flipping only when thresholds are crossed.
func (m *HysteresisMonitor) IsThrottled() bool {
	// Read utilization outside the lock to avoid lock-order inversion
	// if the generic provider happens to block or call back into this monitor.
	util := m.provider.Utilization()

	m.mu.Lock()
	defer m.mu.Unlock()

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
