package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

// RequestManager handles HTTP requests to external services
type RequestManager struct {
	client *http.Client
	logger logger.Logger
}

// RequestOptions contains options for making HTTP requests
type RequestOptions struct {
	Headers map[string]string
	Timeout time.Duration
	Retries int
	Body    io.Reader
	Method  string
	URL     string
}

// NewRequestManager creates a new request manager
func NewRequestManager(log logger.Logger) *RequestManager {
	return &RequestManager{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: log,
	}
}

// Get performs an HTTP GET request
func (rm *RequestManager) Get(ctx context.Context, serverName string, endpoint string, options *RequestOptions) ([]byte, error) {
	url := rm.getServerURL(serverName, endpoint)
	return rm.makeRequest(ctx, http.MethodGet, url, nil, options, nil)
}

func (rm *RequestManager) MakeBitcoinRequest(ctx context.Context, url string, options *RequestOptions) ([]byte, error) {
	return rm.makeRequest(ctx, http.MethodGet, url, nil, options, nil)
}

func (rm *RequestManager) MakeSolanaRequest(ctx context.Context, url string, options *RequestOptions, apiKey string) ([]byte, error) {

	if apiKey != "" {
		options.Headers["x-api-key"] = apiKey
	}
	return rm.makeRequest(ctx, http.MethodPost, url, nil, options, nil)
}

// Post performs an HTTP POST request
func (rm *RequestManager) Post(ctx context.Context, serverName string, endpoint string, data interface{}, options *RequestOptions) ([]byte, error) {
	url := rm.getServerURL(serverName, endpoint)
	return rm.makeRequest(ctx, http.MethodPost, url, data, options, nil)
}

// makeRequest performs the actual HTTP request with retry logic
func (rm *RequestManager) makeRequest(ctx context.Context, method string, url string, data interface{}, options *RequestOptions, apiKey any) ([]byte, error) {

	rm.logger.Info("Making request", "method", method, "url", url, "data", data, "apiKey", apiKey)

	var reqBody io.Reader = nil
	if data != nil {
		jsonData, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("error marshaling request data: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	// Set default headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Set custom headers if provided
	if options != nil && options.Headers != nil {
		for key, value := range options.Headers {
			req.Header.Set(key, value)
		}
	}

	// Set custom timeout if provided
	client := rm.client
	if options != nil && options.Timeout > 0 {
		client = &http.Client{
			Timeout: options.Timeout,
		}
	}

	// Set default retries
	retries := 2
	if options != nil && options.Retries >= 0 {
		retries = options.Retries
	}

	var resp *http.Response
	var respBody []byte

	// Retry logic
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			rm.logger.Info("Retrying request", "url", url, "attempt", attempt, "maxRetries", retries)
			// Wait before retry with exponential backoff
			time.Sleep(time.Duration(attempt*500) * time.Millisecond)
		}

		resp, err = client.Do(req)
		if err != nil {
			rm.logger.Error("HTTP request failed", "error", err, "url", url)
			continue // Retry on connection errors
		}

		// Read response body
		respBody, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			rm.logger.Error("Failed to read response body", "error", err, "url", url)
			continue // Retry on read errors
		}

		// Check for non-successful status code
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			rm.logger.Error("HTTP request returned non-success status",
				"statusCode", resp.StatusCode,
				"url", url,
				"response", string(respBody))

			// Only retry on server errors (5xx)
			if resp.StatusCode < 500 {
				return respBody, fmt.Errorf("HTTP error: %d %s", resp.StatusCode, resp.Status)
			}
			continue
		}

		// Success!
		return respBody, nil
	}

	// If we've exhausted all retries
	if err != nil {
		return nil, fmt.Errorf("request failed after %d retries: %w", retries, err)
	}

	return respBody, fmt.Errorf("request failed after %d retries with status: %d", retries, resp.StatusCode)
}

// getServerURL constructs the full URL for a server request
func (rm *RequestManager) getServerURL(serverName string, endpoint string) string {
	var baseURL string

	switch serverName {
	case "NODE_SERVER":
		cfg, err := config.Load()
		if err != nil {
			rm.logger.Error("Failed to load config", "error", err)
			return ""
		}
		// Check if NodeServer is configured
		if cfg.NodeServer == nil {
			rm.logger.Error("NodeServer configuration is missing")
			return ""
		}
		baseURL = cfg.NodeServer.URL
	default:
		baseURL = serverName // Allow direct URL passing
	}

	// Ensure endpoint starts with '/'
	if len(endpoint) > 0 && endpoint[0] != '/' {
		endpoint = "/" + endpoint
	}

	return baseURL + endpoint
}
