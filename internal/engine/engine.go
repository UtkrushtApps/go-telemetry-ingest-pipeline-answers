package engine

import (
	"context"
	"sync"

	"github.com/sensorix/telemetry-pipeline/internal/config"
	pipelineerrors "github.com/sensorix/telemetry-pipeline/internal/errors"
	"github.com/sensorix/telemetry-pipeline/internal/model"
	"github.com/sensorix/telemetry-pipeline/internal/registry"
	"github.com/sensorix/telemetry-pipeline/internal/sink"
)

// Pipeline orchestrates the multi-stage telemetry ingestion workflow.
type Pipeline struct {
	cfg      config.Config
	registry *registry.DeviceRegistry
	sink     sink.Sink
}

// NewPipeline constructs a Pipeline wired to the given sink.
func NewPipeline(cfg config.Config, s sink.Sink) *Pipeline {
	return &Pipeline{
		cfg:      cfg,
		registry: registry.NewDeviceRegistry(),
		sink:     s,
	}
}

// Run submits events into the pipeline and blocks until all stages finish
// or the context is cancelled. It returns a Result describing what was
// processed and any error encountered.
//
// Behaviour:
//   - malformed events are dropped during validation
//   - unknown devices are dropped during enrichment
//   - cancellation stops admission of new events but drains already admitted work
//   - sink errors are returned to the caller and stop further admission
func (p *Pipeline) Run(ctx context.Context, events []model.RawEvent) (model.Result, error) {
	workerCount := p.cfg.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}

	batchSize := p.cfg.BatchSize
	if batchSize < 1 {
		batchSize = 1
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	validatedCh := make(chan model.RawEvent)
	enrichedCh := make(chan model.EnrichedEvent)
	batchCh := make(chan model.Batch)

	var stageWG sync.WaitGroup
	var workerWG sync.WaitGroup

	// Validation / admission stage.
	stageWG.Add(1)
	go func() {
		defer stageWG.Done()
		defer close(validatedCh)
		p.runValidation(runCtx, events, validatedCh)
	}()

	// Enrichment workers.
	workerWG.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		stageWG.Add(1)
		go func() {
			defer stageWG.Done()
			defer workerWG.Done()
			p.runEnrichmentWorker(validatedCh, enrichedCh)
		}()
	}

	// Close enriched output after all workers have finished.
	stageWG.Add(1)
	go func() {
		defer stageWG.Done()
		workerWG.Wait()
		close(enrichedCh)
	}()

	// Batcher stage.
	stageWG.Add(1)
	go func() {
		defer stageWG.Done()
		defer close(batchCh)
		p.runBatching(enrichedCh, batchSize, batchCh)
	}()

	// Dispatch runs in the caller goroutine so Run does not return until all
	// produced batches are handled and all stage goroutines can terminate.
	dispatched, dispatchErr := p.runDispatch(runCtx, batchCh, cancel)

	// Ensure every launched goroutine has exited before Run returns.
	stageWG.Wait()

	if dispatchErr != nil {
		return model.Result{Dispatched: dispatched}, dispatchErr
	}
	if ctx.Err() != nil {
		return model.Result{Dispatched: dispatched}, ctx.Err()
	}

	return model.Result{Dispatched: dispatched}, nil
}

// runValidation filters out malformed events and submits valid events to the
// next stage until input is exhausted or cancellation is requested.
func (p *Pipeline) runValidation(ctx context.Context, events []model.RawEvent, out chan<- model.RawEvent) {
	for _, e := range events {
		if e.DeviceID == "" || e.Payload == "" {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case out <- e:
		}
	}
}

// runEnrichmentWorker resolves device metadata for each validated event.
// Unknown devices are dropped.
func (p *Pipeline) runEnrichmentWorker(in <-chan model.RawEvent, out chan<- model.EnrichedEvent) {
	for ev := range in {
		info, ok := p.registry.Lookup(ev.DeviceID)
		if !ok {
			continue
		}

		out <- model.EnrichedEvent{
			RawEvent: ev,
			Facility: info.Facility,
			Region:   info.Region,
		}
	}
}

// runBatching groups enriched events into fixed-size batches and flushes a
// final partial batch when input closes.
func (p *Pipeline) runBatching(in <-chan model.EnrichedEvent, batchSize int, out chan<- model.Batch) {
	batch := make([]model.EnrichedEvent, 0, batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		events := make([]model.EnrichedEvent, len(batch))
		copy(events, batch)
		out <- model.Batch{Events: events}
		batch = batch[:0]
	}

	for ev := range in {
		batch = append(batch, ev)
		if len(batch) >= batchSize {
			flush()
		}
	}

	flush()
}

// runDispatch writes batches to the sink and returns the number of events
// successfully dispatched. On the first sink failure it records the error,
// cancels upstream admission, and keeps draining already-produced batches so
// that no goroutines are left blocked during shutdown.
func (p *Pipeline) runDispatch(ctx context.Context, batches <-chan model.Batch, cancel context.CancelFunc) (int, error) {
	dispatched := 0
	var firstErr error

	for batch := range batches {
		// Observe cancellation via ctx, but continue draining already-produced
		// batches so in-flight work does not strand upstream goroutines.
		_ = ctx.Err()

		if firstErr != nil {
			continue
		}

		if err := p.sink.Write(batch); err != nil {
			firstErr = pipelineerrors.Wrap("dispatch", err)
			cancel()
			continue
		}

		dispatched += len(batch.Events)
	}

	return dispatched, firstErr
}
