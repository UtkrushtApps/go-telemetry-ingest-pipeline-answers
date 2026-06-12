package sink_test

import (
	"testing"

	"github.com/sensorix/telemetry-pipeline/internal/model"
	"github.com/sensorix/telemetry-pipeline/internal/sink"
)

func TestMemorySinkWriteAndCount(t *testing.T) {
	s := sink.NewMemorySink()

	batch := model.Batch{
		Events: []model.EnrichedEvent{
			{RawEvent: model.RawEvent{DeviceID: "device-0", Payload: `{"v":1}`}},
			{RawEvent: model.RawEvent{DeviceID: "device-1", Payload: `{"v":2}`}},
		},
	}

	if err := s.Write(batch); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}

	if got := s.Count(); got != 2 {
		t.Errorf("expected count 2, got %d", got)
	}

	all := s.All()
	if len(all) != 2 {
		t.Errorf("expected 2 events from All(), got %d", len(all))
	}
}
