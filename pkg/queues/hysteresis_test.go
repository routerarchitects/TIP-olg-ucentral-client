package queues

import (
	"sync"
	"testing"
)

func TestHysteresisMonitor_ActivationAndReleaseThresholds(t *testing.T) {
	q := NewCommandResultQueue(10)
	monitor := NewHysteresisMonitor(q, 0.90, 0.50)

	// Initially, queue is empty, utilization is 0.0
	if monitor.IsThrottled() {
		t.Fatalf("expected telemetry NOT to be throttled at 0%% utilization")
	}

	// Push 8 items (80%)
	for i := 0; i < 8; i++ {
		_ = q.Push([]byte("item"))
	}

	// At 80%, should still NOT be throttled
	if monitor.IsThrottled() {
		t.Fatalf("expected telemetry NOT to be throttled at 80%% utilization")
	}

	// Push 1 item (90%)
	_ = q.Push([]byte("item"))

	// At 90%, it should activate throttling
	if !monitor.IsThrottled() {
		t.Fatalf("expected telemetry to be THROTTLED at 90%% utilization")
	}

	// Pop 3 items to get to 60%
	for i := 0; i < 3; i++ {
		q.Pop()
	}

	// At 60%, it should STILL be throttled due to hysteresis (needs <= 50%)
	if !monitor.IsThrottled() {
		t.Fatalf("expected telemetry to REMAIN THROTTLED at 60%% utilization (hysteresis)")
	}

	// Pop 1 item to get to 50%
	q.Pop()

	// At 50%, it should release throttling
	if monitor.IsThrottled() {
		t.Fatalf("expected telemetry NOT to be throttled at 50%% utilization (hysteresis release)")
	}
}

func TestHysteresisMonitor_InvalidConfig(t *testing.T) {
	q := NewCommandResultQueue(10)

	tests := []struct {
		name     string
		provider UtilizationProvider
		upper    float64
		lower    float64
	}{
		{"nil provider", nil, 0.9, 0.5},
		{"lower > upper", q, 0.5, 0.9},
		{"lower == upper", q, 0.5, 0.5},
		{"upper > 1", q, 1.1, 0.5},
		{"lower < 0", q, 0.9, -0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for %s", tt.name)
				}
			}()
			NewHysteresisMonitor(tt.provider, tt.upper, tt.lower)
		})
	}
}

type mockProvider struct {
	mu          sync.Mutex
	utilization float64
}

func (m *mockProvider) Utilization() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.utilization
}

func (m *mockProvider) SetUtilization(v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.utilization = v
}

func TestHysteresisMonitor_Concurrency(t *testing.T) {
	mp := &mockProvider{utilization: 0.5}
	monitor := NewHysteresisMonitor(mp, 0.9, 0.5)

	var wg sync.WaitGroup
	// Spin up 100 goroutines to concurrently read the throttled state
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = monitor.IsThrottled()
			}
		}()
	}

	// Concurrently change the utilization value
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				mp.SetUtilization(0.95)
			} else {
				mp.SetUtilization(0.45)
			}
		}(i)
	}

	wg.Wait()
}
