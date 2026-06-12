package model

// RawEvent is a telemetry reading arriving from an IoT device.
type RawEvent struct {
	DeviceID  string
	Payload   string
	Timestamp int64
}

// EnrichedEvent is a RawEvent augmented with resolved device metadata.
type EnrichedEvent struct {
	RawEvent
	Facility string
	Region   string
}

// Batch is a slice of EnrichedEvents grouped for dispatch.
type Batch struct {
	Events []EnrichedEvent
}

// Result summarises what the pipeline achieved in a single run.
type Result struct {
	Dispatched int
}
