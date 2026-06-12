package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sensorix/telemetry-pipeline/internal/config"
	"github.com/sensorix/telemetry-pipeline/internal/engine"
	"github.com/sensorix/telemetry-pipeline/internal/model"
	"github.com/sensorix/telemetry-pipeline/internal/sink"
)

// buildEvents creates n well-formed raw events cycling through device IDs 0-9.
func buildEvents(n int) []model.RawEvent {
	events := make([]model.RawEvent, n)
	for i := 0; i < n; i++ {
		events[i] = model.RawEvent{
			DeviceID:  fmt.Sprintf("device-%d", i%10),
			Payload:   fmt.Sprintf(`{"seq":%d}`, i),
			Timestamp: time.Now().UnixMilli(),
		}
	}
	return events
}

// TestPipelineProcessesAllEvents verifies that every submitted event reaches
// the sink when the pipeline runs to completion without cancellation.
func TestPipelineProcessesAllEvents(t *testing.T) {
	cfg := config.Config{WorkerCount: 4, BatchSize: 10}
	memSink := sink.NewMemorySink()
	pipeline := engine.NewPipeline(cfg, memSink)

	events := buildEvents(40)
	ctx := context.Background()

	result, err := pipeline.Run(ctx, events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Dispatched != 40 {
		t.Errorf("expected 40 dispatched events, got %d", result.Dispatched)
	}

	if memSink.Count() != 40 {
		t.Errorf("expected 40 events in sink, got %d", memSink.Count())
	}
}

// TestPipelineDropsInvalidEvents confirms that events missing required fields
// are excluded from processing and do not reach the sink.
func TestPipelineDropsInvalidEvents(t *testing.T) {
	cfg := config.Config{WorkerCount: 2, BatchSize: 5}
	memSink := sink.NewMemorySink()
	pipeline := engine.NewPipeline(cfg, memSink)

	events := []model.RawEvent{
		{DeviceID: "device-0", Payload: `{"v":1}`, Timestamp: 1},
		{DeviceID: "", Payload: `{"v":2}`, Timestamp: 2},
		{DeviceID: "device-1", Payload: "", Timestamp: 3},
		{DeviceID: "device-2", Payload: `{"v":4}`, Timestamp: 4},
	}

	result, err := pipeline.Run(context.Background(), events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Dispatched != 2 {
		t.Errorf("expected 2 dispatched events after filtering, got %d", result.Dispatched)
	}
}

// TestPipelineRespectsContextCancellation checks that the pipeline returns
// promptly after its context is cancelled and does not hang indefinitely.
func TestPipelineRespectsContextCancellation(t *testing.T) {
	cfg := config.Config{WorkerCount: 4, BatchSize: 5}
	memSink := sink.NewMemorySink()
	pipeline := engine.NewPipeline(cfg, memSink)

	events := buildEvents(100)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		pipeline.Run(ctx, events) //nolint:errcheck
		close(done)
	}()

	select {
	case <-done:
		// pipeline returned after cancellation — correct
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not return within 2 seconds after context cancellation")
	}
}

// TestPipelineReportsErrors checks that an error produced by the sink is
// returned to the caller rather than silently discarded.
func TestPipelineReportsErrors(t *testing.T) {
	cfg := config.Config{WorkerCount: 2, BatchSize: 5}
	errorSink := &failingSink{err: errors.New("sink unavailable")}
	pipeline := engine.NewPipeline(cfg, errorSink)

	events := buildEvents(20)

	_, err := pipeline.Run(context.Background(), events)
	if err == nil {
		t.Fatal("expected an error from the failing sink, got nil")
	}
}

// TestPipelineConcurrentSafety runs the pipeline repeatedly with the race
// detector active to surface any data races in concurrent stages.
func TestPipelineConcurrentSafety(t *testing.T) {
	cfg := config.Config{WorkerCount: 8, BatchSize: 10}

	for run := 0; run < 5; run++ {
		memSink := sink.NewMemorySink()
		pipeline := engine.NewPipeline(cfg, memSink)
		events := buildEvents(50)

		result, err := pipeline.Run(context.Background(), events)
		if err != nil {
			t.Fatalf("run %d: unexpected error: %v", run, err)
		}
		if result.Dispatched != 50 {
			t.Errorf("run %d: expected 50 dispatched, got %d", run, result.Dispatched)
		}
	}
}

// failingSink is a Sink implementation that always returns an error on Write.
type failingSink struct {
	err error
}

func (f *failingSink) Write(_ model.Batch) error {
	return f.err
}
