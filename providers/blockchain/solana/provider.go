package solana

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/managers/queue"
	"bitbucket.org/zapspace/zap-go-server/models"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain/bitcoin"
)

// Provider implements the ChainProvider interface for Solana
type Provider struct {
	logger          logger.Logger
	rpcURL          string
	wsURL           string
	commitmentLevel string
}

// NewProvider creates a new Solana provider
func NewProvider(logger logger.Logger, cfg config.SolanaConfig) (bitcoin.ChainProvider, error) {
	return &Provider{
		logger:          logger,
		rpcURL:          cfg.RPCEndpoint,
		wsURL:           cfg.WebSocketURL,
		commitmentLevel: cfg.CommitmentLevel,
	}, nil
}

// GetBalance retrieves the balance for a Solana address
func (p *Provider) GetBalance(ctx context.Context, address string, currency *models.Currency) (float64, error) {
	p.logger.Info("Getting Solana balance", "address", address, "currency", currency.Symbol)

	// TokenAddress for SPL tokens should be passed via the blockchain service wrapper
	// For the mock implementation, we'll check if there is a token address for this currency
	// in the context, which would have been added by the blockchain service

	// Get token address from context if available
	var tokenAddress string
	if ctx.Value("tokenAddress") != nil {
		if tokenAddrStr, ok := ctx.Value("tokenAddress").(string); ok && tokenAddrStr != "" {
			tokenAddress = tokenAddrStr
			p.logger.Info("Found token address in context", "tokenAddress", tokenAddress)
		}
	}

	// If we have a valid token address, get token balance
	if tokenAddress != "" {
		return p.getTokenBalance(ctx, address, tokenAddress)
	}

	// For native SOL balance
	return p.getNativeBalance(ctx, address)
}

// getNativeBalance retrieves the native SOL balance for an address
func (p *Provider) getNativeBalance(ctx context.Context, address string) (float64, error) {
	p.logger.Info("Getting native SOL balance", "address", address)

	// Create request to get balance
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getBalance",
		"params": []interface{}{
			address,
			map[string]string{
				"commitment": p.commitmentLevel,
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Set up a properly configured HTTP client
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			DisableKeepAlives:   false,
		},
	}

	// Use the URL directly from the provider, which should already have API key configured
	rpcURL := p.rpcURL

	// Get API key for both URL and header
	apiKey := ""

	// Extract from URL if present
	if strings.Contains(rpcURL, "/solana/") && strings.Count(rpcURL, "/") > 3 {
		parts := strings.Split(rpcURL, "/")
		if len(parts) > 4 && parts[len(parts)-1] != "" {
			apiKey = parts[len(parts)-1]
			// Keep API key in URL (don't remove it)
			p.logger.Debug("Found API key in URL")
		}
	}

	// If not in URL, try environment
	if apiKey == "" {
		apiKey = os.Getenv("SOLANA_ANKR_API_KEY")
		if apiKey != "" {
			p.logger.Debug("Using API key from environment")
		}
	}

	// Make the RPC request
	req, err := http.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return 0, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")

	// Always add API key to header if available, regardless of whether it's in URL
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}

	// Implement retry logic
	maxRetries := 3
	var resp *http.Response
	var responseBody []byte

	for retry := 0; retry <= maxRetries; retry++ {
		if retry > 0 {
			// Add exponential backoff
			backoffTime := time.Duration(retry*retry) * 500 * time.Millisecond
			p.logger.Info("Retrying Solana RPC request", "retry", retry, "backoff", backoffTime)
			time.Sleep(backoffTime)
		}

		resp, err = client.Do(req)
		if err != nil {
			if retry == maxRetries {
				return 0, fmt.Errorf("failed to make RPC request (after %d retries): %w", retry, err)
			}
			p.logger.Warn("Request failed, will retry", "error", err, "retry", retry+1)
			continue
		}

		// Read response body
		responseBody, err = io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			if retry == maxRetries {
				return 0, fmt.Errorf("failed to read response body (after %d retries): %w", retry, err)
			}
			p.logger.Warn("Failed to read response body, will retry", "error", err, "retry", retry+1)
			continue
		}

		// Special handling for auth errors to make them clearer
		if resp.StatusCode == 403 || resp.StatusCode == 401 {
			errorMsg := string(responseBody)
			if len(errorMsg) > 100 {
				errorMsg = errorMsg[:100] + "..."
			}

			if retry == maxRetries {
				p.logger.Error("Authentication error with Ankr API",
					"statusCode", resp.StatusCode,
					"response", errorMsg)
				return 0, fmt.Errorf("authentication error with Ankr API (status %d): %s", resp.StatusCode, errorMsg)
			}

			p.logger.Warn("Authentication error, will retry",
				"statusCode", resp.StatusCode,
				"response", errorMsg,
				"retry", retry+1)
			continue
		}

		// Check for rate limiting
		if resp.StatusCode == 429 {
			if retry == maxRetries {
				return 0, fmt.Errorf("rate limited by Ankr API (after %d retries)", retry)
			}
			p.logger.Warn("Rate limited by Ankr API, will retry", "retry", retry+1)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			errorMsg := string(responseBody)
			if len(errorMsg) > 100 {
				errorMsg = errorMsg[:100] + "..."
			}

			if retry == maxRetries {
				p.logger.Error("Unexpected status code",
					"statusCode", resp.StatusCode,
					"response", errorMsg)
				return 0, fmt.Errorf("unexpected status code: %d (after %d retries): %s", resp.StatusCode, retry, errorMsg)
			}

			p.logger.Warn("Unexpected status code, will retry",
				"statusCode", resp.StatusCode,
				"response", errorMsg,
				"retry", retry+1)
			continue
		}

		// If we got here, we succeeded
		break
	}

	// Parse the response
	var rpcResp struct {
		Result struct {
			Value int64 `json:"value"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(responseBody, &rpcResp); err != nil {
		return 0, fmt.Errorf("failed to decode RPC response: %w", err)
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		return 0, fmt.Errorf("RPC error: %s (code: %d)", rpcResp.Error.Message, rpcResp.Error.Code)
	}

	// Convert from lamports (smallest unit) to SOL
	// 1 SOL = 1,000,000,000 lamports (9 decimals)
	balanceSOL := float64(rpcResp.Result.Value) / math.Pow10(constants.SolDecimals)

	return balanceSOL, nil
}

// getTokenBalance retrieves the SPL token balance for an address
func (p *Provider) getTokenBalance(ctx context.Context, address string, tokenAddress string) (float64, error) {
	p.logger.Info("Getting SPL token balance",
		"address", address,
		"tokenAddress", tokenAddress)

	// 1. Create a request to get token accounts by owner
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTokenAccountsByOwner",
		"params": []interface{}{
			address,
			map[string]string{
				"mint": tokenAddress,
			},
			map[string]string{
				"encoding": "jsonParsed",
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP client
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Make the RPC request
	req, err := http.NewRequestWithContext(ctx, "POST", p.rpcURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return 0, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Execute the request
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to make RPC request: %w", err)
	}
	defer resp.Body.Close()

	// Parse the response
	var rpcResp struct {
		Result struct {
			Value []struct {
				Account struct {
					Data struct {
						Parsed struct {
							Info struct {
								TokenAmount struct {
									Amount   string  `json:"amount"`
									Decimals int     `json:"decimals"`
									UIAmount float64 `json:"uiAmount"`
								} `json:"tokenAmount"`
							} `json:"info"`
						} `json:"parsed"`
					} `json:"data"`
				} `json:"account"`
			} `json:"value"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return 0, fmt.Errorf("failed to decode RPC response: %w", err)
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		return 0, fmt.Errorf("RPC error: %s (code: %d)", rpcResp.Error.Message, rpcResp.Error.Code)
	}

	// Extract the balance
	if len(rpcResp.Result.Value) == 0 {
		// No token accounts found - balance is zero
		return 0, nil
	}

	// Return the UI amount which is already formatted with correct decimals
	return rpcResp.Result.Value[0].Account.Data.Parsed.Info.TokenAmount.UIAmount, nil
}

// ValidateAddress checks if a Solana address is valid
func (p *Provider) ValidateAddress(address string) bool {
	// Simple validation check - would be replaced with actual validation logic
	return len(address) >= 32 && len(address) <= 44
}

// GetTokenDetails gets information about a Solana SPL token
func (p *Provider) GetTokenDetails(ctx context.Context, tokenAddress string) (*models.TokenDetails, error) {
	p.logger.Info("Getting Solana token details", "address", tokenAddress)

	// 1. Get token metadata using getTokenSupply method
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTokenSupply",
		"params": []interface{}{
			tokenAddress,
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP client
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Make the RPC request
	req, err := http.NewRequestWithContext(ctx, "POST", p.rpcURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Execute the request
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make RPC request: %w", err)
	}
	defer resp.Body.Close()

	// Parse the response
	var rpcResp struct {
		Result struct {
			Value struct {
				Amount   string `json:"amount"`
				Decimals int    `json:"decimals"`
			} `json:"value"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("failed to decode RPC response: %w", err)
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s (code: %d)", rpcResp.Error.Message, rpcResp.Error.Code)
	}

	// Get token metadata from on-chain metadata program if available
	// For production code, we would fetch more details from the metadata program
	// For simplicity, we'll generate basic information here

	// Try to get tokenMetadata if possible
	metadataProgramID := "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
	tokenMetadata, err := p.getTokenMetadata(ctx, tokenAddress, metadataProgramID)
	if err != nil {
		p.logger.Warn("Could not get token metadata, using basic info", "error", err)

		// Use basic info when metadata is not available
		return &models.TokenDetails{
			Name:         fmt.Sprintf("SPL Token %s", tokenAddress[:8]),
			Symbol:       fmt.Sprintf("SPL-%s", tokenAddress[:4]),
			Decimals:     rpcResp.Result.Value.Decimals,
			TotalSupply:  rpcResp.Result.Value.Amount,
			TokenAddress: tokenAddress,
			ChainID:      "SOL",
		}, nil
	}

	return &models.TokenDetails{
		Name:         tokenMetadata.Name,
		Symbol:       tokenMetadata.Symbol,
		Decimals:     rpcResp.Result.Value.Decimals,
		TotalSupply:  rpcResp.Result.Value.Amount,
		TokenAddress: tokenAddress,
		ChainID:      "SOL",
	}, nil
}

// getTokenMetadata attempts to fetch token metadata from the Solana Metadata program
func (p *Provider) getTokenMetadata(ctx context.Context, tokenAddress, metadataProgramID string) (*struct {
	Name   string `json:"name"`
	Symbol string `json:"symbol"`
}, error) {
	// Get PDA for token metadata
	// In production, we would derive the PDA address properly
	// This is a simplified approach
	metadataAddress, err := p.getMetadataAddress(tokenAddress, metadataProgramID)
	if err != nil {
		return nil, fmt.Errorf("failed to derive metadata address: %w", err)
	}

	// Get account info
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getAccountInfo",
		"params": []interface{}{
			metadataAddress,
			map[string]string{
				"encoding": "base64",
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP client
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Make the RPC request
	req, err := http.NewRequestWithContext(ctx, "POST", p.rpcURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Execute the request
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make RPC request: %w", err)
	}
	defer resp.Body.Close()

	// Parse the response
	var rpcResp struct {
		Result struct {
			Value struct {
				Data []string `json:"data"`
			} `json:"value"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("failed to decode RPC response: %w", err)
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s (code: %d)", rpcResp.Error.Message, rpcResp.Error.Code)
	}

	// If account not found
	if len(rpcResp.Result.Value.Data) == 0 {
		return nil, fmt.Errorf("metadata account not found")
	}

	// In a production implementation, we would:
	// 1. Decode the base64 data
	// 2. Parse the binary layout according to the Metadata program's schema
	// 3. Extract name, symbol and other metadata

	// For simplicity, we'll return a basic structure
	// In production, this should properly decode the account data
	return &struct {
		Name   string `json:"name"`
		Symbol string `json:"symbol"`
	}{
		Name:   fmt.Sprintf("Token %s", tokenAddress[:8]),
		Symbol: fmt.Sprintf("TKN%s", tokenAddress[:4]),
	}, nil
}

// getMetadataAddress derives the metadata account address for a token
// In production, this would use proper PDA derivation
func (p *Provider) getMetadataAddress(tokenAddress, metadataProgramID string) (string, error) {
	// This is a simplified version - in production code:
	// 1. We would use proper seed derivation with "metadata" prefix
	// 2. Use proper public key operations
	// 3. Find the correct program derived address (PDA)
	fmt.Println("metadataProgramID", metadataProgramID)

	// Example code for deriving metadata PDA (placeholder):
	metadataAddress := fmt.Sprintf("%s_metadata", tokenAddress)

	return metadataAddress, nil
}

// WatchAddress sets up a watch for a Solana address
func (p *Provider) WatchAddress(ctx context.Context, address string, onUpdate func(update bitcoin.BlockUpdate)) {
	p.logger.Info("Setting up watch for Solana address", "address", address)
	// Implementation would connect to Solana node and set up WebSocket subscription
}

// CreateTransaction creates a transaction on Solana
func (p *Provider) CreateTransaction(ctx context.Context, params bitcoin.TransactionParams) (string, error) {
	p.logger.Info("Creating Solana transaction",
		"from", params.FromAddress,
		"to", params.ToAddress,
		"amount", params.Amount)
	return "", fmt.Errorf("not implemented")
}

// CreateDepositAddress creates a new Solana deposit address
func (p *Provider) CreateDepositAddress(ctx context.Context) (*bitcoin.DepositAddress, error) {
	p.logger.Info("Creating new Solana deposit address")

	// In a real implementation, this would:
	// 1. Generate a new Ed25519 key pair
	// 2. The public key serves as the Solana address
	// 3. Securely store or encrypt the private key

	// This is a mock implementation
	mockAddress := "DYw8jCTfwHNRJhhmFcbXvVDTqWMEVFBX6ZKUmG5CNSKK"
	mockPrivateKey := "encrypted_private_key_mock"
	mockDerivationPath := "m/44'/501'/0'/0'"

	return &bitcoin.DepositAddress{
		Address:        mockAddress,
		PrivateKey:     mockPrivateKey,
		DerivationPath: mockDerivationPath,
	}, nil
}

// StartBlockMonitor starts monitoring Solana blocks
func (p *Provider) StartBlockMonitor(ctx context.Context) error {
	p.logger.Info("Starting Solana block monitor")

	// Get current slot (block) height to start monitoring from
	currentSlot, err := p.getCurrentSlot(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current slot: %w", err)
	}

	p.logger.Info("Starting block monitor", "slot", currentSlot)

	// Access the queue manager from context or dependency injection
	queueManager, ok := ctx.Value("queueManager").(*queue.QueueManager)
	if !ok {
		return fmt.Errorf("queue manager not found in context")
	}

	// Create job data
	jobData := map[string]interface{}{
		"lastProcessedSlot": currentSlot,
		"chainSymbol":       "solana",
	}

	// Schedule job with queue manager
	options := map[string]interface{}{
		"repeat": map[string]interface{}{
			"every": 3000, // Check every 3 seconds (in milliseconds) - Solana has fast block times
		},
		"jobId":            "solana-block-monitor",
		"removeOnComplete": false,
		"removeOnFail":     false,
	}

	jobId, err := queueManager.AddJob(ctx, constants.QueueSolanaBlockMonitor, jobData, options)
	if err != nil {
		p.logger.Error("Failed to schedule Solana block monitor job", "error", err)
		return fmt.Errorf("failed to schedule Solana block monitor job: %w", err)
	}

	p.logger.Info("Solana block monitor job scheduled", "jobId", jobId)
	return nil
}

// StopBlockMonitor stops the block monitoring process
func (p *Provider) StopBlockMonitor(ctx context.Context) error {
	p.logger.Info("Stopping Solana block monitor")

	// Access the queue manager from context or dependency injection
	queueManager, ok := ctx.Value("queueManager").(*queue.QueueManager)
	if !ok {
		return fmt.Errorf("queue manager not found in context")
	}

	// Get the job status before clearing
	status, err := queueManager.GetQueueStatus(ctx, constants.QueueSolanaBlockMonitor)
	if err != nil {
		p.logger.Warn("Failed to get queue status", "error", err)
		// Continue with clearing attempt even if status check fails
	} else {
		p.logger.Info("Solana block monitor queue status before stopping",
			"activeJobs", status["active"],
			"waitingJobs", status["waiting"],
			"completedJobs", status["completed"],
			"failedJobs", status["failed"])
	}

	// Clear the queue to stop processing
	err = queueManager.ClearQueue(ctx, constants.QueueSolanaBlockMonitor)
	if err != nil {
		p.logger.Error("Failed to clear Solana block monitor queue", "error", err)
		return fmt.Errorf("failed to clear Solana block monitor queue: %w", err)
	}

	p.logger.Info("Solana block monitor stopped successfully")
	return nil
}

// getCurrentSlot gets the current slot (block height) from the Solana chain
func (p *Provider) getCurrentSlot(ctx context.Context) (uint64, error) {
	p.logger.Info("Getting current Solana slot")

	// In a real implementation, this would call the Solana RPC
	// Example RPC call: {"jsonrpc":"2.0","id":1,"method":"getSlot"}

	// Create a simple HTTP client for the RPC call
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Prepare the request body
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getSlot",
		"params":  []interface{}{},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Make the RPC request
	req, err := http.NewRequestWithContext(ctx, "POST", p.rpcURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return 0, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		p.logger.Error("Failed to get current slot from RPC", "error", err)
		// Fallback to mock value
		return 150000000, nil
	}
	defer resp.Body.Close()

	// Parse the response
	var rpcResp struct {
		Result uint64 `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		p.logger.Error("Failed to decode RPC response", "error", err)
		// Fallback to mock value
		return 150000000, nil
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		p.logger.Error("RPC error", "code", rpcResp.Error.Code, "message", rpcResp.Error.Message)
		// Fallback to mock value
		return 150000000, nil
	}

	return rpcResp.Result, nil
}

// GetChainInfo gets current blockchain information
func (p *Provider) GetChainInfo(ctx context.Context) (*bitcoin.ChainInfo, error) {
	p.logger.Info("Getting Solana chain info")

	// Mock implementation - would be replaced with actual RPC call
	return &bitcoin.ChainInfo{
		Height: 150000000,
		Hash:   "4uQeVj5tqViQh7yWWGStvkEG1Zmhx6uasJtWCJziofM",
		Time:   time.Now(),
		Chain:  "solana",
	}, nil
}

// GetBlockDetails gets detailed information about a specific block
func (p *Provider) GetBlockDetails(ctx context.Context, blockHash string) (*bitcoin.BlockDetails, error) {
	p.logger.Info("Getting Solana block details", "hash", blockHash)

	// Mock implementation - would be replaced with actual RPC call
	return &bitcoin.BlockDetails{
		Hash:              blockHash,
		Height:            150000000,
		Time:              time.Now(),
		PreviousBlockHash: "8LuwGrQAfgzC5v5kPPh1K9v9xWF5Z2FEhrLRVESWAvLq",
		Transactions:      []string{"tx1", "tx2", "tx3"},
	}, nil
}

// GetTreasuryAddress returns the configured treasury address
func (p *Provider) GetTreasuryAddress() (string, error) {
	// In a real implementation, this would be fetched from configuration
	return "DYw8jCTfwHNRJhhmFcbXvVDTqWMEVFBX6ZKUmG5CNSKK", nil
}
