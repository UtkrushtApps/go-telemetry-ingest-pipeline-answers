# Solution Steps

1. Normalize configuration at the start of `Run` so `WorkerCount` and `BatchSize` are always at least 1; this prevents deadlocks from invalid zero values.

2. Create an internal run context with `context.WithCancel(ctx)` so the pipeline can stop upstream admission when the caller cancels or when the sink fails.

3. Replace the old slice-based enrichment logic with a channel pipeline: one channel for validated raw events, one for enriched events, and one for batches.

4. Implement validation as a producer goroutine that filters malformed events and sends only valid ones downstream using `select` on `ctx.Done()` and the output channel.

5. Start a fixed-size worker pool for enrichment instead of launching one goroutine per event; each worker reads from the validated channel, looks up device metadata, drops unknown devices, and sends enriched events to the next stage.

6. Use a `sync.WaitGroup` for enrichment workers and close the enriched output channel only after all workers exit; this guarantees proper stage shutdown.

7. Implement a batching stage that accumulates enriched events up to `BatchSize`, emits full batches, and flushes the final partial batch when input closes.

8. When emitting a batch, copy the batch slice into a fresh slice before sending it so later reuse of the backing array cannot corrupt data retained by the sink.

9. Run dispatch in the caller goroutine and write every produced batch to the sink, counting only successful writes toward `Result.Dispatched`.

10. If `sink.Write` returns an error, wrap it with the pipeline stage label, cancel the internal context to stop new admission, remember the first error, and continue draining the batch channel so no upstream goroutine remains blocked.

11. After dispatch finishes, wait for all stage goroutines to exit before returning from `Run`; this ensures there are no goroutine leaks after shutdown.

12. Return partial progress together with the first sink error when dispatch fails; otherwise, if the caller's context was cancelled or timed out, return the dispatched count plus `ctx.Err()` so cancellation is surfaced accurately.

13. Verify the implementation with `go test ./...` and `go test -race ./...` to confirm correctness, clean shutdown, and absence of data races.

