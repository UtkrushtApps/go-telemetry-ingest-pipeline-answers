package sink

import (
	"sync"

	"github.com/sensorix/telemetry-pipeline/internal/model"
)

// Sink is the destination for dispatched batches.
type Sink interface {
	Write(batch model.Batch) error
}

// MemorySink stores dispatched events in memory for test inspection.
type MemorySink struct {
	mu     sync.Mutex
	events []model.EnrichedEvent
}

// NewMemorySink returns an empty MemorySink.
func NewMemorySink() *MemorySink {
	return &MemorySink{}
}

// Write appends all events in the batch to the in-memory store.
func (s *MemorySink) Write(batch model.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, batch.Events...)
	return nil
}

// Count returns the number of events stored so far.
func (s *MemorySink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// All returns a copy of all stored events.
func (s *MemorySink) All() []model.EnrichedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.EnrichedEvent, len(s.events))
	copy(out, s.events)
	return out
}
