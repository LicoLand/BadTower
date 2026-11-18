//go:build station_acceptance

package acceptance

import (
	"sync"
	"time"
)

type capacityClock struct {
	mu        sync.RWMutex
	wallStart time.Time
	realStart time.Time
	offset    time.Duration
}

func newCapacityClock(start time.Time) *capacityClock {
	return &capacityClock{wallStart: start.UTC(), realStart: time.Now()}
}

func (clock *capacityClock) NowUnixMs() int64 {
	clock.mu.RLock()
	offset := clock.offset
	clock.mu.RUnlock()
	return clock.wallStart.Add(time.Since(clock.realStart) + offset).UnixMilli()
}

func (clock *capacityClock) MonotonicNow() time.Duration {
	clock.mu.RLock()
	offset := clock.offset
	clock.mu.RUnlock()
	return time.Since(clock.realStart) + offset
}

func (clock *capacityClock) Advance(delta time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.offset += delta
}
