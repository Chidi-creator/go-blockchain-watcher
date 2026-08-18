package events

import (
	"sync"

	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

// EventHandler is a function that handles an event
type EventHandler func(data interface{})

// EventManager manages event subscriptions and publishing
type EventManager struct {
	handlers map[string][]EventHandler
	mutex    sync.RWMutex
	logger   logger.Logger
}

// NewEventManager creates a new event manager
func NewEventManager(logger logger.Logger) *EventManager {
	return &EventManager{
		handlers: make(map[string][]EventHandler),
		mutex:    sync.RWMutex{},
		logger:   logger,
	}
}

// Subscribe registers a handler for an event
func (em *EventManager) Subscribe(eventName string, handler EventHandler) {
	em.mutex.Lock()
	defer em.mutex.Unlock()

	em.handlers[eventName] = append(em.handlers[eventName], handler)
	em.logger.Info("Subscribed to event", "event", eventName)
}

// Publish sends an event to all subscribers
func (em *EventManager) Publish(eventName string, data interface{}) {
	em.mutex.RLock()
	handlers := em.handlers[eventName]
	em.mutex.RUnlock()

	for _, handler := range handlers {
		go func(h EventHandler) {
			defer func() {
				if r := recover(); r != nil {
					em.logger.Error("Panic in event handler", "event", eventName, "panic", r)
				}
			}()
			h(data)
		}(handler)
	}

	em.logger.Info("Published event", "event", eventName, "handlerCount", len(handlers))
}

// UnsubscribeAll removes all handlers for an event
func (em *EventManager) UnsubscribeAll(eventName string) {
	em.mutex.Lock()
	defer em.mutex.Unlock()

	delete(em.handlers, eventName)
	em.logger.Info("Unsubscribed all handlers from event", "event", eventName)
}
