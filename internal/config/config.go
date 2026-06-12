package config

// Config holds tuneable parameters for the ingestion pipeline.
type Config struct {
	// WorkerCount is the number of concurrent enrichment workers.
	WorkerCount int

	// BatchSize is the maximum number of events grouped into one dispatch batch.
	BatchSize int
}

// Default returns a Config suitable for local development and testing.
func Default() Config {
	return Config{
		WorkerCount: 4,
		BatchSize:   10,
	}
}
