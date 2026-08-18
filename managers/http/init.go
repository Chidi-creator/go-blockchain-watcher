package http

import (
	"sync"

	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

var (
	instance     *RequestManager
	instanceOnce sync.Once
)

// Initialize creates and initializes the RequestManager singleton
func Initialize(log logger.Logger) *RequestManager {
	instanceOnce.Do(func() {
		instance = NewRequestManager(log)
	})
	return instance
}

// GetRequestManager returns the singleton instance of RequestManager
func GetRequestManager() *RequestManager {
	if instance == nil {
		panic("RequestManager not initialized. Call Initialize() first.")
	}
	return instance
}
