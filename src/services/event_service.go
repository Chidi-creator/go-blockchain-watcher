package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

// EventType represents the type of event
type EventType string

const (
	// EventAccountsImported represents accounts import event
	EventAccountsImported EventType = "accounts_imported"
	// EventBalanceUpdated represents balance update event
	EventBalanceUpdated EventType = "balance_updated"
	// Add more event types as needed
)

// EventStatus represents the status of an event
type EventStatus string

const (
	// EventStatusSuccess represents a successful event
	EventStatusSuccess EventStatus = "success"
	// EventStatusError represents a failed event
	EventStatusError EventStatus = "error"
	// EventStatusPending represents a pending event
	EventStatusPending EventStatus = "pending"
)

// EventPayload represents the payload for an event
type EventPayload struct {
	Type      EventType   `json:"type"`
	Status    EventStatus `json:"status"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data,omitempty"`
	Error     string      `json:"error,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// EventService handles event communication with external systems
type EventService struct {
	httpClient *http.Client
	logger     logger.Logger
}

// NewEventService creates a new event service
func NewEventService(logger logger.Logger) *EventService {
	return &EventService{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: logger,
	}
}

// SendEvent sends an event to the specified callback URL
func (s *EventService) SendEvent(ctx context.Context, callbackURL string, eventType EventType, status EventStatus, message string, data interface{}, errorMsg string) error {
	if callbackURL == "" {
		s.logger.Warn("No callback URL provided for event", "eventType", eventType)
		return fmt.Errorf("no callback URL provided")
	}

	payload := EventPayload{
		Type:      eventType,
		Status:    status,
		Message:   message,
		Data:      data,
		Error:     errorMsg,
		Timestamp: time.Now(),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		s.logger.Error("Failed to marshal event payload", "eventType", eventType, "error", err)
		return err
	}

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, "POST", callbackURL, bytes.NewBuffer(jsonData))
	if err != nil {
		s.logger.Error("Failed to create event request", "eventType", eventType, "error", err)
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add event type header for easier processing on Node.js side
	req.Header.Set("X-Event-Type", string(eventType))

	// Send request
	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Error("Failed to send event", "eventType", eventType, "url", callbackURL, "error", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		s.logger.Error("Event delivery failed with status code", "eventType", eventType, "statusCode", resp.StatusCode)
		return fmt.Errorf("event delivery failed with status code: %d", resp.StatusCode)
	}

	s.logger.Info("Event sent successfully", "eventType", eventType, "url", callbackURL, "statusCode", resp.StatusCode)
	return nil
}
