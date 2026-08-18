package worker

import (
	"context"
)

// Worker defines the interface that all worker types must implement
type Worker interface {
	// Start begins the worker's processing with the provided context
	Start(ctx context.Context) error
}

// Config contains common configuration for workers
type Config struct {
	Concurrency int
	LogLevel    string
} 