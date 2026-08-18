package logger

import (
	"sync"
)

var (
	instance Logger
	once     sync.Once
)

// Get returns the singleton logger instance
func Get() Logger {
	once.Do(func() {
		instance = NewLogger("info")
	})
	return instance
}

// SetLogger sets the singleton logger instance
func SetLogger(logger Logger) {
	instance = logger
}
