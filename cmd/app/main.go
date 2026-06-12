package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sensorix/telemetry-pipeline/internal/config"
	"github.com/sensorix/telemetry-pipeline/internal/engine"
	"github.com/sensorix/telemetry-pipeline/internal/model"
	"github.com/sensorix/telemetry-pipeline/internal/sink"
)

func main() {
	cfg := config.Default()

	memSink := sink.NewMemorySink()

	pipeline := engine.NewPipeline(cfg, memSink)

	events := make([]model.RawEvent, 0, 50)
	for i := 0; i < 50; i++ {
		events = append(events, model.RawEvent{
			DeviceID:  fmt.Sprintf("device-%d", i%10),
			Payload:   fmt.Sprintf(`{"temp": %d}`, 20+i),
			Timestamp: time.Now().UnixMilli(),
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := pipeline.Run(ctx, events)
	if err != nil {
		log.Printf("pipeline error: %v", err)
	} else {
		log.Printf("pipeline complete: dispatched=%d", result.Dispatched)
	}

	log.Printf("sink contains %d events", memSink.Count())
}
