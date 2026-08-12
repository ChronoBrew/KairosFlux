package main

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	latencies []time.Duration
	mu        sync.Mutex

	totalOps  atomic.Int64
	totalErrs atomic.Int64
	startTime time.Time
	endTime   time.Time
}

func NewStats() *Stats {
	return &Stats{
		latencies: make([]time.Duration, 0),
	}
}

func (s *Stats) Start() {
	s.startTime = time.Now()
}

func (s *Stats) Stop() {
	s.endTime = time.Now()
}

func (s *Stats) Record(duration time.Duration, err error) {
	s.totalOps.Add(1)
	if err != nil {
		s.totalErrs.Add(1)
	}
	s.mu.Lock()
	s.latencies = append(s.latencies, duration)
	s.mu.Unlock()
}

func (s *Stats) TotalOps() int64 {
	return s.totalOps.Load()
}

func (s *Stats) TotalErrs() int64 {
	return s.totalErrs.Load()
}

func (s *Stats) Duration() time.Duration {
	return s.endTime.Sub(s.startTime)
}

func (s *Stats) QPS() float64 {
	d := s.Duration()
	if d == 0 {
		return 0
	}
	return float64(s.TotalOps()) / d.Seconds()
}

func (s *Stats) AvgLatency() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.latencies) == 0 {
		return 0
	}
	var sum time.Duration
	for _, l := range s.latencies {
		sum += l
	}
	return sum / time.Duration(len(s.latencies))
}

func (s *Stats) MaxLatency() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	var max time.Duration
	for _, l := range s.latencies {
		if l > max {
			max = l
		}
	}
	return max
}

func (s *Stats) percentile(p float64) time.Duration {
	s.mu.Lock()
	sorted := make([]time.Duration, len(s.latencies))
	copy(sorted, s.latencies)
	s.mu.Unlock()

	if len(sorted) == 0 {
		return 0
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (s *Stats) P50() time.Duration { return s.percentile(0.50) }
func (s *Stats) P95() time.Duration { return s.percentile(0.95) }
func (s *Stats) P99() time.Duration { return s.percentile(0.99) }

func (s *Stats) SuccessRate() float64 {
	total := s.TotalOps()
	if total == 0 {
		return 0
	}
	return float64(total-s.TotalErrs()) / float64(total) * 100
}
