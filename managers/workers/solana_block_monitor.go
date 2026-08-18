package workers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/events"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/src/types"
)

// SolanaRpcURL is the default RPC URL for Solana
const SolanaRpcURL = "https://rpc.ankr.com/solana"

// The URL should be constructed with the API key at runtime

// SolanaTransaction represents a Solana transaction
type SolanaTransaction struct {
	Signature  string      `json:"signature"`
	Successful bool        `json:"successful"`
	Fee        int64       `json:"fee"`
	Meta       interface{} `json:"meta,omitempty"`
	// Additional fields that may be present in Solana's response
	Version     *interface{} `json:"version,omitempty"`
	Transaction *interface{} `json:"transaction,omitempty"`
}

// SolanaBlock represents a Solana block
type SolanaBlock struct {
	Blockhash    string              `json:"blockhash"`
	BlockTime    int64               `json:"blockTime"`
	BlockHeight  uint64              `json:"blockHeight"`
	Transactions []SolanaTransaction `json:"transactions"`
}

// CustomSolanaBlock is a structure that correctly maps Ankr's response format
type CustomSolanaBlock struct {
	Blockhash           string            `json:"blockhash"`
	BlockTime           int64             `json:"blockTime"`
	BlockHeight         uint64            `json:"blockHeight"`
	PreviousBlockhash   string            `json:"previousBlockhash"`
	ParentSlot          uint64            `json:"parentSlot"`
	Transactions        []json.RawMessage `json:"transactions"`
	Rewards             []interface{}     `json:"rewards,omitempty"`
	BlockProductionTime *interface{}      `json:"blockProductionTime,omitempty"`
}

// SolanaRpcRequest represents a Solana JSON-RPC request
type SolanaRpcRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params,omitempty"`
}

// SolanaRpcResponse represents a Solana JSON-RPC response
type SolanaRpcResponse struct {
	Jsonrpc string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// SolanaBlockMonitor handles Solana block monitoring
type SolanaBlockMonitor struct {
	cacheManager *cache.CacheManager
	eventManager *events.EventManager
	httpClient   *http.Client
	rpcURL       string
	logger       logger.Logger
}

// NewSolanaBlockMonitor creates a new Solana block monitor
func NewSolanaBlockMonitor(
	cacheManager *cache.CacheManager,
	eventManager *events.EventManager,
	logger logger.Logger,
	rpcURL string,
) *SolanaBlockMonitor {
	// Use default URL if none provided
	if rpcURL == "" {
		rpcURL = SolanaRpcURL
	}

	// Make sure base Ankr URL is correct
	if !strings.Contains(rpcURL, "ankr.com") {
		logger.Warn("[SolanaBlockMonitor] RPC URL doesn't appear to be an Ankr endpoint", "url", rpcURL)
	}

	// Handle different ways the URL might be formed
	apiKeyFromURL := ""

	// Try to detect if URL already has API key
	if strings.Contains(rpcURL, "/solana/") && strings.Count(rpcURL, "/") > 3 {
		parts := strings.Split(rpcURL, "/")
		if len(parts) > 4 && len(parts[len(parts)-1]) > 0 {
			logger.Info("[SolanaBlockMonitor] URL appears to contain API key",
				"urlPattern", strings.Join(parts[:len(parts)-1], "/")+"/****")
			apiKeyFromURL = parts[len(parts)-1]
		}
	}

	// Check for API key in environment
	apiKeyFromEnv := os.Getenv("SOLANA_ANKR_API_KEY")
	if apiKeyFromEnv != "" {
		// If we have both env and URL keys, log a warning
		if apiKeyFromURL != "" && apiKeyFromURL != apiKeyFromEnv {
			logger.Warn("[SolanaBlockMonitor] Found different API keys in URL and environment. Environment key will take precedence.")
		}
		logger.Info("[SolanaBlockMonitor] Using Ankr API key from environment")

		// If URL doesn't contain the key, add it
		// Some Ankr endpoints prefer URL with key
		if apiKeyFromURL == "" && !strings.Contains(rpcURL, apiKeyFromEnv) {
			// Check if URL should have trailing slash before adding key
			if !strings.HasSuffix(rpcURL, "/") {
				rpcURL = rpcURL + "/"
			}
			rpcURL = rpcURL + apiKeyFromEnv
			logger.Info("[SolanaBlockMonitor] Added API key to URL")
		}
	} else if apiKeyFromURL == "" {
		logger.Warn("[SolanaBlockMonitor] No API key found in URL or environment variables. " +
			"Ankr API requests may fail with authentication errors.")
	}

	return &SolanaBlockMonitor{
		cacheManager: cacheManager,
		eventManager: eventManager,
		httpClient: &http.Client{
			Timeout: 45 * time.Second, // Increased timeout for Ankr API
			Transport: &http.Transport{
				MaxIdleConns:        50,
				MaxIdleConnsPerHost: 50,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
				DisableKeepAlives:   false,
				ForceAttemptHTTP2:   true,
			},
		},
		rpcURL: rpcURL,
		logger: logger,
	}
}

// ProcessBlockMonitorJob processes a Solana block monitor job
func (sm *SolanaBlockMonitor) ProcessBlockMonitorJob(ctx context.Context, jobData map[string]interface{}) error {
	sm.logger.Info("[DEBUG TRACE] Starting to process job data", "data", jobData)

	// Check if this is a backup job meant to ensure the main monitor job exists
	if action, ok := jobData["action"].(string); ok && action == "ensure_monitor_job" {
		sm.logger.Info("[DEBUG TRACE] Processing ensure_monitor_job action")
		return sm.handleEnsureMonitorJob(ctx, jobData)
	}

	// Create a non-cancellable context for cache operations
	cacheCtx := context.Background()

	// Get block state from cache
	sm.logger.Info("[DEBUG TRACE] Fetching block state from cache")
	cacheResp, err := sm.cacheManager.Get(cacheCtx, constants.SolanaBlockIdentifier)
	if err != nil {
		sm.logger.Error("[DEBUG TRACE] Error fetching block state from cache", "error", err)
		return err
	}

	var lastProcessedSlot uint64
	if cacheResp.Data != nil {
		// Try to parse the cached slot
		switch v := cacheResp.Data.(type) {
		case float64:
			lastProcessedSlot = uint64(v)
		case int:
			lastProcessedSlot = uint64(v)
		case uint64:
			lastProcessedSlot = v
		case json.Number:
			parsed, err := v.Int64()
			if err == nil {
				lastProcessedSlot = uint64(parsed)
			}
		default:
			sm.logger.Info("[DEBUG TRACE] Unknown type for cached slot",
				"type", fmt.Sprintf("%T", cacheResp.Data),
				"value", cacheResp.Data)
		}
	}

	// Get current slot - use timeout but with a fresh context
	slotCtx, slotCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer slotCancel()

	sm.logger.Info("[DEBUG TRACE] Fetching current slot")
	currentSlot, err := sm.getCurrentSlot(slotCtx)
	if err != nil {
		sm.logger.Error("[DEBUG TRACE] Error fetching current slot", "error", err)
		return err
	}

	sm.logger.Info("[DEBUG TRACE] Current slot vs last processed",
		"current", currentSlot,
		"lastProcessed", lastProcessedSlot)

	if lastProcessedSlot == 0 {
		// No block state found, initialize with current slot
		sm.logger.Info("[DEBUG TRACE] No block state found, initializing", "slot", currentSlot)

		err = sm.cacheManager.Set(cacheCtx, constants.SolanaBlockIdentifier, currentSlot, 120)
		if err != nil {
			sm.logger.Error("[DEBUG TRACE] Error saving initial slot", "error", err)
			return err
		}

		return nil
	}

	// Check if there are new blocks
	if currentSlot > lastProcessedSlot {
		sm.logger.Info("[DEBUG TRACE] New blocks detected, starting processing",
			"newBlockCount", currentSlot-lastProcessedSlot)

		// Process blocks in batches to avoid too many simultaneous requests
		batchSize := uint64(10)
		batches := (currentSlot - lastProcessedSlot) / batchSize
		if (currentSlot-lastProcessedSlot)%batchSize > 0 {
			batches++
		}

		// Create a fresh context for processing that's independent of the job context
		// but has a reasonable timeout to avoid blocking indefinitely
		processCtx, processCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer processCancel()

		for batchIndex := uint64(0); batchIndex < batches; batchIndex++ {
			// Check if processing context was canceled (timeout)
			if processCtx.Err() != nil {
				sm.logger.Info("[DEBUG TRACE] Processing interrupted", "reason", processCtx.Err())
				break
			}

			startSlot := lastProcessedSlot + 1 + batchIndex*batchSize
			endSlot := startSlot + batchSize - 1
			if endSlot > currentSlot {
				endSlot = currentSlot
			}

			sm.logger.Info("[DEBUG TRACE] Processing batch of slots",
				"startSlot", startSlot, "endSlot", endSlot)

			for slot := startSlot; slot <= endSlot; slot++ {
				// Add delay between requests to avoid rate limiting
				if slot > startSlot {
					time.Sleep(100 * time.Millisecond)
				}

				// Skip processing if overall timeout has been reached
				if processCtx.Err() != nil {
					sm.logger.Info("[DEBUG TRACE] Slot processing timeout reached", "slot", slot)
					// Update currentSlot to the last fully processed slot
					if slot > startSlot {
						currentSlot = slot - 1
					} else {
						currentSlot = lastProcessedSlot // No progress made
					}
					break
				}

				sm.logger.Info("[DEBUG TRACE] Processing slot", "slot", slot)
				err := sm.processSlot(processCtx, slot)
				if err != nil {
					sm.logger.Error("[DEBUG TRACE] Error processing slot", "slot", slot, "error", err)
					continue
				} else {
					sm.logger.Info("[DEBUG TRACE] Successfully processed slot", "slot", slot)
				}
			}
		}

		// Update last processed slot - use a fresh context for cache operation
		cacheUpdateCtx := context.Background()
		sm.logger.Info("[DEBUG TRACE] Updating last processed slot", "slot", currentSlot)
		err = sm.cacheManager.Set(cacheUpdateCtx, constants.SolanaBlockIdentifier, currentSlot, 120)
		if err != nil {
			sm.logger.Error("[DEBUG TRACE] Error updating last processed slot", "error", err)
			return err
		}

		sm.logger.Info("[DEBUG TRACE] Updated last processed slot", "slot", currentSlot)
	} else {
		sm.logger.Info("[DEBUG TRACE] No new blocks to process")
	}

	// Worker completed successfully
	return nil
}

// getCurrentSlot fetches the current Solana slot
func (sm *SolanaBlockMonitor) getCurrentSlot(ctx context.Context) (uint64, error) {
	request := SolanaRpcRequest{
		Jsonrpc: "2.0",
		ID:      1,
		Method:  "getSlot",
		Params:  []interface{}{map[string]string{"commitment": "confirmed"}},
	}

	reqBody, err := json.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("error marshaling request: %w", err)
	}

	resp, err := sm.makeRequest(ctx, reqBody)
	if err != nil {
		return 0, err
	}

	var slot uint64
	err = json.Unmarshal(resp.Result, &slot)
	if err != nil {
		return 0, fmt.Errorf("error unmarshaling slot: %w", err)
	}

	return slot, nil
}

// processSlot processes a Solana slot (block)
func (sm *SolanaBlockMonitor) processSlot(ctx context.Context, slot uint64) error {
	sm.logger.Info("[DEBUG TRACE] Processing slot", "slot", slot)

	// Create a fresh context with timeout, independent of parent
	// This ensures that cancellation of the parent context doesn't affect this operation
	slotCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Fetch block
	sm.logger.Info("[DEBUG TRACE] Fetching block for slot", "slot", slot)
	block, err := sm.getBlock(slotCtx, slot)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "context deadline exceeded") {
			return fmt.Errorf("timeout fetching block for slot %d: %w", slot, err)
		}
		if strings.Contains(err.Error(), "429") {
			return fmt.Errorf("rate limited when fetching block for slot %d: %w", slot, err)
		}
		return fmt.Errorf("error fetching block for slot %d: %w", slot, err)
	}

	if block == nil {
		sm.logger.Info("[DEBUG TRACE] No block data found for slot", "slot", slot)
		return nil
	}

	sm.logger.Info("[DEBUG TRACE] Successfully fetched block",
		"slot", slot,
		"hash", block.Blockhash,
		"txCount", len(block.Transactions))

	// Only check the fresh slotCtx, not the parent ctx which may have been canceled
	if slotCtx.Err() != nil {
		return fmt.Errorf("context timeout while processing slot %d: %w", slot, slotCtx.Err())
	}

	// Filter out invalid transactions (those with empty signatures)
	validTransactions := make([]SolanaTransaction, 0)
	for _, tx := range block.Transactions {
		if tx.Signature != "" {
			validTransactions = append(validTransactions, tx)
		}
	}

	if len(validTransactions) == 0 {
		// If no valid transactions, we still continue but log a warning
		sm.logger.Warn("[DEBUG TRACE] No valid transactions found in block", "slot", slot)
	} else {
		sm.logger.Info("[DEBUG TRACE] Found valid transactions",
			"validCount", len(validTransactions),
			"totalCount", len(block.Transactions))
	}

	// Convert valid transactions to the format expected by the event
	transactions := make([]map[string]interface{}, len(validTransactions))
	for i, tx := range validTransactions {
		transactions[i] = map[string]interface{}{
			"signature":  tx.Signature,
			"successful": tx.Successful,
			"fee":        tx.Fee,
		}
	}

	// Create block data for event
	blockData := types.BlockData{
		ChainSymbol:  "SOL",
		BlockNumber:  slot,
		BlockHash:    block.Blockhash,
		Timestamp:    block.BlockTime,
		Transactions: transactions,
	}

	// Log the exact data structure we're about to publish
	eventDataJSON, err := json.Marshal(blockData)
	if err == nil {
		sm.logger.Info("[DEBUG TRACE] Event data being published",
			"slot", slot,
			"dataJSON", string(eventDataJSON))
	} else {
		sm.logger.Warn("[DEBUG TRACE] Could not marshal event data",
			"slot", slot,
			"error", err)
	}

	// Emit block found event using the event manager
	sm.logger.Info("[DEBUG TRACE] About to emit block found event",
		"slot", slot,
		"txCount", len(transactions))
	sm.eventManager.Publish("handleBlockFound", blockData)
	sm.logger.Info("[DEBUG TRACE] Successfully published handleBlockFound event",
		"slot", slot,
		"txCount", len(transactions))

	return nil
}

// getBlock fetches a Solana block by slot number
func (sm *SolanaBlockMonitor) getBlock(ctx context.Context, slot uint64) (*SolanaBlock, error) {
	request := SolanaRpcRequest{
		Jsonrpc: "2.0",
		ID:      1,
		Method:  "getBlock",
		Params: []interface{}{
			slot,
			map[string]interface{}{
				"maxSupportedTransactionVersion": 0,
				"encoding":                       "json",
				"commitment":                     "confirmed",
				"transactionDetails":             "full",
			},
		},
	}

	reqBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %w", err)
	}

	resp, err := sm.makeRequest(ctx, reqBody)
	if err != nil {
		if resp != nil && resp.Error != nil && resp.Error.Code == -32004 {
			// Block not found or skipped, not an error
			return nil, nil
		}
		return nil, err
	}

	// Check if result is null
	if string(resp.Result) == "null" {
		return nil, nil
	}

	// First try using the standard format
	var block SolanaBlock
	err = json.Unmarshal(resp.Result, &block)

	// Log the raw JSON for debugging
	if err != nil || len(block.Transactions) == 0 {
		truncatedResult := string(resp.Result)
		if len(truncatedResult) > 200 {
			truncatedResult = truncatedResult[:200] + "..."
		}
		sm.logger.Debug("[SolanaBlockMonitor] Raw block data", "prefix", truncatedResult)
	}

	// If standard format has issues, try the custom format
	if err != nil || (len(block.Transactions) > 0 && allTransactionsEmpty(block.Transactions)) {
		sm.logger.Info("[SolanaBlockMonitor] Trying alternative block format parsing", "error", err)

		var customBlock CustomSolanaBlock
		if err := json.Unmarshal(resp.Result, &customBlock); err != nil {
			return nil, fmt.Errorf("error unmarshaling block with custom format: %w", err)
		}

		// Now convert from custom format to standard format
		block.Blockhash = customBlock.Blockhash
		block.BlockTime = customBlock.BlockTime
		block.BlockHeight = customBlock.BlockHeight

		// Process raw transactions
		transactions := make([]SolanaTransaction, 0, len(customBlock.Transactions))
		for _, rawTx := range customBlock.Transactions {
			// Parse the transaction structure
			var txData map[string]interface{}
			if err := json.Unmarshal(rawTx, &txData); err != nil {
				sm.logger.Warn("[SolanaBlockMonitor] Failed to parse transaction", "error", err)
				continue
			}

			tx := SolanaTransaction{}

			// Extract signature
			if meta, ok := txData["transaction"].(map[string]interface{}); ok {
				if signatures, ok := meta["signatures"].([]interface{}); ok && len(signatures) > 0 {
					if sig, ok := signatures[0].(string); ok {
						tx.Signature = sig
					}
				}
			}

			// Extract fee
			if meta, ok := txData["meta"].(map[string]interface{}); ok {
				if fee, ok := meta["fee"].(float64); ok {
					tx.Fee = int64(fee)
				}
			}

			// Extract status
			if meta, ok := txData["meta"].(map[string]interface{}); ok {
				status, ok := meta["err"]
				tx.Successful = !ok || status == nil // No error means success
			}

			// Add transaction if we have at least a signature
			if tx.Signature != "" {
				transactions = append(transactions, tx)
			}
		}

		if len(transactions) > 0 {
			block.Transactions = transactions
			sm.logger.Info("[SolanaBlockMonitor] Successfully parsed block with alternative format",
				"txCount", len(transactions))
		} else {
			sm.logger.Warn("[SolanaBlockMonitor] Failed to extract valid transactions")
		}
	}

	// If we still don't have valid transactions, log a warning
	if len(block.Transactions) == 0 {
		sm.logger.Warn("[SolanaBlockMonitor] No transactions found in block")
	} else if allTransactionsEmpty(block.Transactions) {
		sm.logger.Warn("[SolanaBlockMonitor] All transactions are empty", "count", len(block.Transactions))
	}

	return &block, nil
}

// allTransactionsEmpty checks if all transactions have empty signatures
func allTransactionsEmpty(transactions []SolanaTransaction) bool {
	for _, tx := range transactions {
		if tx.Signature != "" {
			return false
		}
	}
	return len(transactions) > 0 // Only true if we have transactions and they're all empty
}

// makeRequest sends a JSON-RPC request to the Solana RPC endpoint
func (sm *SolanaBlockMonitor) makeRequest(ctx context.Context, reqBody []byte) (*SolanaRpcResponse, error) {
	// Extract API key if it's in the URL
	rpcURL := sm.rpcURL
	apiKey := ""

	// Log the initial URL (partially masked for security)
	if len(rpcURL) > 20 {
		// maskedURL := rpcURL
		// if strings.Contains(maskedURL, "/solana/") && strings.Count(maskedURL, "/") > 3 {
		// 	parts := strings.Split(maskedURL, "/")
		// 	if len(parts) > 4 && len(parts[len(parts)-1]) > 8 {
		// 		parts[len(parts)-1] = parts[len(parts)-1][:4] + "..." + parts[len(parts)-1][len(parts[len(parts)-1])-4:]
		// 		maskedURL = strings.Join(parts, "/")
		// 	}
		// }
		sm.logger.Debug("[SolanaBlockMonitor] Using RPC URL")
	}

	// First try to get API key from environment
	envApiKey := os.Getenv("SOLANA_ANKR_API_KEY")
	if envApiKey != "" {
		apiKey = envApiKey
		sm.logger.Debug("[SolanaBlockMonitor] Using API key from environment")
	}

	// If no environment API key, extract from URL if present
	if apiKey == "" && strings.Contains(rpcURL, "/solana/") && strings.Count(rpcURL, "/") > 3 {
		parts := strings.Split(rpcURL, "/")
		if len(parts) > 4 {
			apiKey = parts[len(parts)-1]
			// Keep the API key in the URL for Ankr compatibility
			// Some Ankr endpoints prefer URL-based authentication
			sm.logger.Debug("[SolanaBlockMonitor] Extracted API key from URL")
		}
	}

	// Create the request
	req, err := http.NewRequestWithContext(ctx, "POST", rpcURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	// Set common headers
	req.Header.Set("Content-Type", "application/json")

	// Always add API key as header if available, regardless of whether it's also in the URL
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
		sm.logger.Debug("[SolanaBlockMonitor] Added API key to request header")
	} else {
		sm.logger.Warn("[SolanaBlockMonitor] No API key available, requests may fail")
	}

	// Set the request body
	req.Body = io.NopCloser(bytes.NewReader(reqBody))

	// Add request method to logs
	var method string
	var requestMap map[string]interface{}
	if err := json.Unmarshal(reqBody, &requestMap); err == nil {
		if m, ok := requestMap["method"].(string); ok {
			method = m
		}
	}

	// Implement retry logic
	maxRetries := 3
	var resp *http.Response
	var rpcResp SolanaRpcResponse

	for retry := 0; retry <= maxRetries; retry++ {
		if retry > 0 {
			// Add exponential backoff
			backoffTime := time.Duration(retry*retry) * 500 * time.Millisecond
			sm.logger.Info("[SolanaBlockMonitor] Retrying request", "method", method, "retry", retry, "backoff", backoffTime)
			time.Sleep(backoffTime)

			// Create a fresh request body for the retry
			req.Body = io.NopCloser(bytes.NewReader(reqBody))
		}

		// Execute the request
		resp, err = sm.httpClient.Do(req)
		if err != nil {
			if retry == maxRetries {
				return nil, fmt.Errorf("error making request (after %d retries): %w", retry, err)
			}
			sm.logger.Warn("[SolanaBlockMonitor] Request failed, will retry", "method", method, "error", err, "retry", retry+1)
			continue
		}

		// We need to close the body regardless of the outcome
		defer resp.Body.Close()

		// Capture response body for logging in case of errors
		respBodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			if retry == maxRetries {
				return nil, fmt.Errorf("error reading response (after %d retries): %w", retry, err)
			}
			sm.logger.Warn("[SolanaBlockMonitor] Failed to read response, will retry", "method", method, "error", err, "retry", retry+1)
			continue
		}

		// Check for rate limiting
		if resp.StatusCode == 429 {
			if retry == maxRetries {
				return nil, fmt.Errorf("rate limited by Ankr API (after %d retries)", retry)
			}
			sm.logger.Warn("[SolanaBlockMonitor] Rate limited by Ankr API, will retry", "method", method, "retry", retry+1)
			continue
		}

		// Special handling for auth errors
		if resp.StatusCode == 403 || resp.StatusCode == 401 {
			errorMsg := string(respBodyBytes)
			if len(errorMsg) > 100 {
				errorMsg = errorMsg[:100] + "..."
			}

			if retry == maxRetries {
				sm.logger.Error("[SolanaBlockMonitor] Authentication error with Ankr API",
					"method", method,
					"statusCode", resp.StatusCode,
					"response", errorMsg)
				return nil, fmt.Errorf("authentication error with Ankr API (status %d): %s", resp.StatusCode, errorMsg)
			}

			sm.logger.Warn("[SolanaBlockMonitor] Authentication error, will retry",
				"method", method,
				"statusCode", resp.StatusCode,
				"response", errorMsg,
				"retry", retry+1)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			errorMsg := string(respBodyBytes)
			if len(errorMsg) > 100 {
				errorMsg = errorMsg[:100] + "..."
			}

			if retry == maxRetries {
				sm.logger.Error("[SolanaBlockMonitor] Unexpected status code",
					"method", method,
					"statusCode", resp.StatusCode,
					"response", errorMsg)
				return nil, fmt.Errorf("unexpected status code: %d (after %d retries): %s", resp.StatusCode, retry, errorMsg)
			}

			sm.logger.Warn("[SolanaBlockMonitor] Unexpected status code, will retry",
				"method", method,
				"statusCode", resp.StatusCode,
				"response", errorMsg,
				"retry", retry+1)
			continue
		}

		// Create new reader for the JSON decoder
		respBodyReader := bytes.NewReader(respBodyBytes)

		// Decode response
		err = json.NewDecoder(respBodyReader).Decode(&rpcResp)
		if err != nil {
			if retry == maxRetries {
				return nil, fmt.Errorf("error decoding response: %w", err)
			}
			sm.logger.Warn("[SolanaBlockMonitor] Failed to decode response, will retry", "method", method, "error", err, "retry", retry+1)
			continue
		}

		// Check for RPC error
		if rpcResp.Error != nil {
			// Some RPC errors are permanent and shouldn't be retried
			if rpcResp.Error.Code == -32004 { // Block not found or skipped
				return &rpcResp, fmt.Errorf("RPC error: %s (code: %d)", rpcResp.Error.Message, rpcResp.Error.Code)
			}

			if retry == maxRetries {
				return &rpcResp, fmt.Errorf("RPC error: %s (code: %d)", rpcResp.Error.Message, rpcResp.Error.Code)
			}

			sm.logger.Warn("[SolanaBlockMonitor] RPC error, will retry",
				"method", method,
				"code", rpcResp.Error.Code,
				"message", rpcResp.Error.Message,
				"retry", retry+1)
			continue
		}

		// If we got here, we succeeded
		sm.logger.Debug("[SolanaBlockMonitor] Request successful", "method", method)
		break
	}

	return &rpcResp, nil
}

// StartBlockMonitor starts monitoring Solana blocks
func (sm *SolanaBlockMonitor) StartBlockMonitor(ctx context.Context, queueService interface{}) error {
	type QueueServiceWithSchedule interface {
		ScheduleJob(ctx context.Context, queueName string, data map[string]interface{}, options map[string]interface{}) (string, error)
	}

	qs, ok := queueService.(QueueServiceWithSchedule)
	if !ok {
		return fmt.Errorf("queue service does not implement required methods")
	}

	// Create fresh contexts for initialization operations
	// This ensures these operations complete regardless of parent context state
	slotCtx, slotCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer slotCancel()

	// Get current slot to start monitoring from
	currentSlot, err := sm.getCurrentSlot(slotCtx)
	if err != nil {
		sm.logger.Error("[SolanaBlockMonitor] Error fetching current slot", "error", err)
		return err
	}

	// Use fresh context for cache operations
	cacheCtx := context.Background()

	// Set initial block height in cache
	err = sm.cacheManager.Set(cacheCtx, constants.SolanaBlockIdentifier, currentSlot, 120)
	if err != nil {
		sm.logger.Error("[SolanaBlockMonitor] Error saving initial slot", "error", err)
		return err
	}

	// Create job data - include everything needed for job execution in the data
	// This makes the job less dependent on retrieving the repeat job definition
	jobData := map[string]interface{}{
		"lastProcessedBlockHeight": currentSlot,
		"monitorConfig": map[string]interface{}{
			"interval":        float64(5000), // 5 seconds
			"rpcUrl":          sm.rpcURL,     // Include RPC URL in job data
			"processingBatch": float64(10),   // Batch size for processing
			"cacheExpiry":     float64(3600), // Cache expiry in seconds
			"cacheKey":        constants.SolanaBlockIdentifier,
			"isRepeatingJob":  true,
		},
	}

	// Create job options with repeat - using float64 for every to avoid type conversion issues
	// Store essential data in options and data to create redundancy
	options := map[string]interface{}{
		"jobId": "solana-block-monitor",
		"repeat": map[string]interface{}{
			"every": float64(5000), // 5 seconds (Solana has ~400ms block time)
		},
		"removeOnComplete": false,
		"removeOnFail":     false,
		// Add more job persistence settings
		"attempts": float64(5), // Retry up to 5 times if the job fails
		"backoff": map[string]interface{}{
			"type":  "exponential",
			"delay": float64(1000), // Start with 1 second delay
		},
	}

	// Use a fresh context for job scheduling to ensure it completes
	// regardless of parent context state
	scheduleCtx := context.Background()

	// Schedule the job
	jobID, err := qs.ScheduleJob(scheduleCtx, constants.QueueSolanaBlockMonitor, jobData, options)
	if err != nil {
		sm.logger.Error("[SolanaBlockMonitor] Failed to schedule block monitor job", "error", err)
		return err
	}

	// Also schedule a backup self-rescheduling job that will recreate the main job if needed
	// This adds redundancy in case the automatic rescheduling fails
	backupJobId := fmt.Sprintf("%s-backup", jobID)
	backupOptions := map[string]interface{}{
		"jobId":            backupJobId,
		"delay":            float64(30000), // Run after 30 seconds
		"removeOnComplete": false,
		"removeOnFail":     false,
	}

	backupData := map[string]interface{}{
		"action":        "ensure_monitor_job",
		"originalJobId": jobID,
		"targetQueue":   constants.QueueSolanaBlockMonitor,
	}

	// Use the same persistent context for backup job
	_, backupErr := qs.ScheduleJob(scheduleCtx, constants.QueueSolanaBlockMonitor, backupData, backupOptions)
	if backupErr != nil {
		sm.logger.Warn("[SolanaBlockMonitor] Failed to schedule backup monitor job", "error", backupErr)
		// Continue even if backup job fails
	}

	sm.logger.Info("[SolanaBlockMonitor] Block monitor job scheduled", "jobId", jobID, "startingSlot", currentSlot)
	return nil
}

// handleEnsureMonitorJob is a helper function to ensure the main monitor job exists
// This is a backup mechanism in case the repeat job fails to reschedule
func (sm *SolanaBlockMonitor) handleEnsureMonitorJob(ctx context.Context, jobData map[string]interface{}) error {
	type QueueServiceWithSchedule interface {
		ScheduleJob(ctx context.Context, queueName string, data map[string]interface{}, options map[string]interface{}) (string, error)
		GetQueueStatus(ctx context.Context, queueName string) (map[string]int64, error)
	}

	// Extract the queue service from context
	queueSvc, ok := ctx.Value("queueService").(QueueServiceWithSchedule)
	if !ok {
		// This is expected since we don't have the queue service in context
		// Just log and reschedule this backup job itself
		sm.logger.Info("[SolanaBlockMonitor] Backup job running, rescheduling itself")

		// Return nil so the job completes and is rescheduled if it's a repeat job
		return nil
	}

	sm.logger.Info("[SolanaBlockMonitor] Running backup job to ensure monitor job exists")

	// Reschedule this backup job to run again in 30 seconds
	backupOpts := map[string]interface{}{
		"delay":            float64(30000),
		"removeOnComplete": false,
		"removeOnFail":     false,
	}

	_, err := queueSvc.ScheduleJob(ctx, constants.QueueSolanaBlockMonitor, jobData, backupOpts)
	if err != nil {
		sm.logger.Error("[SolanaBlockMonitor] Failed to reschedule backup job", "error", err)
	}

	return nil
}

// RegisterSolanaBlockMonitor registers the Solana block monitor with the queue service
func RegisterSolanaBlockMonitor(ctx context.Context, queueService interface{}, cacheManager *cache.CacheManager, eventManager *events.EventManager, logger logger.Logger, rpcURL string) error {
	// Cast the queue service to the correct type that has RegisterHandler method
	type QueueServiceWithRegister interface {
		RegisterHandler(queueName string, handler func(ctx context.Context, data map[string]interface{}) error)
	}

	qs, ok := queueService.(QueueServiceWithRegister)
	if !ok {
		return fmt.Errorf("queue service does not implement required methods")
	}

	monitor := NewSolanaBlockMonitor(cacheManager, eventManager, logger, rpcURL)

	// Register the handler
	qs.RegisterHandler(constants.QueueSolanaBlockMonitor, monitor.ProcessBlockMonitorJob)

	logger.Info("Solana block monitor registered", "queue", constants.QueueSolanaBlockMonitor)
	return nil
}
