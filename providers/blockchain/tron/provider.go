package tron

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/managers/queue"
	"bitbucket.org/zapspace/zap-go-server/models"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain/bitcoin"
)

// Provider implements the ChainProvider interface for Tron
// This is designed to mimic the TronWeb approach from Node.js
type Provider struct {
	logger      logger.Logger
	apiKey      string
	fullNodeURL string
	httpClient  *http.Client
}

// NewProvider creates a new Tron provider with TronWeb-like functionality
func NewProvider(logger logger.Logger, cfg config.TronConfig) (bitcoin.ChainProvider, error) {
	return &Provider{
		logger:      logger,
		apiKey:      cfg.ApiKey,
		fullNodeURL: cfg.FullNodeURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

// GetBalance retrieves the balance for a Tron address
func (p *Provider) GetBalance(ctx context.Context, address string, currency *models.Currency) (float64, error) {
	p.logger.Info("Getting Tron balance", "address", address, "currency", currency.Symbol)

	// Get token address from context if available
	var tokenAddress string
	if ctx.Value(constants.TokenAddressKey) != nil {
		if tokenAddrStr, ok := ctx.Value(constants.TokenAddressKey).(string); ok && tokenAddrStr != "" {
			tokenAddress = tokenAddrStr
			p.logger.Info("Found token address in context", "tokenAddress", tokenAddress)
		} else {
			p.logger.Warn("TokenAddress in context is not a valid string",
				"type", fmt.Sprintf("%T", ctx.Value(constants.TokenAddressKey)))
		}
	} else {
		p.logger.Warn("No token address in context")
	}

	// Get decimals from context if available
	var decimalsFromCtx int
	if ctx.Value(constants.DecimalsKey) != nil {
		if dec, ok := ctx.Value(constants.DecimalsKey).(int); ok && dec > 0 {
			decimalsFromCtx = dec
			p.logger.Info("Found decimals in context", "decimals", decimalsFromCtx)
		} else {
			p.logger.Warn("Decimals in context is not a valid int",
				"type", fmt.Sprintf("%T", ctx.Value(constants.DecimalsKey)),
				"value", ctx.Value(constants.DecimalsKey))
		}
	} else {
		p.logger.Warn("No decimals in context")
	}

	fmt.Println("tokenAddress", tokenAddress)
	fmt.Println("+++++++++++++++++++++++++++++++++++++")

	// If we have a valid token address, get token balance (TRC20 token)
	if tokenAddress != "" {
		return p.getTokenBalance(ctx, address, tokenAddress)
	}

	// For native TRX balance
	return p.getNativeBalance(ctx, address)
}

// getNativeBalance retrieves the native TRX balance (similar to TronWeb's trx.getBalance)
func (p *Provider) getNativeBalance(ctx context.Context, address string) (float64, error) {
	p.logger.Info("Getting native TRX balance", "address", address)

	// Endpoint to get account info
	endpoint := fmt.Sprintf("%s/wallet/getaccount", p.fullNodeURL)

	// Create request
	reqBody := map[string]interface{}{
		"address": address,
		"visible": true, // This makes the API return base58 addresses instead of hex
	}

	respBody, err := p.makeRequest(ctx, endpoint, reqBody)
	if err != nil {
		return 0, fmt.Errorf("failed to get account balance: %w", err)
	}

	// Parse response
	var accountResp struct {
		Balance int64  `json:"balance"`
		Error   string `json:"Error,omitempty"`
	}

	if err := json.Unmarshal(respBody, &accountResp); err != nil {
		return 0, fmt.Errorf("failed to parse account response: %w", err)
	}

	// Check for error in response
	if accountResp.Error != "" {
		// If account doesn't exist, the balance is 0
		if accountResp.Error == "Account not exists" {
			return 0, nil
		}
		return 0, fmt.Errorf("API error: %s", accountResp.Error)
	}

	// Convert from SUN (smallest unit) to TRX (1 TRX = 1,000,000 SUN)
	balanceTRX := float64(accountResp.Balance) / 1000000.0

	return balanceTRX, nil
}

// getTokenBalance retrieves the TRC20 token balance (similar to TronWeb's contract call)
func (p *Provider) getTokenBalance(ctx context.Context, address string, tokenAddress string) (float64, error) {
	p.logger.Info("Getting TRC20 token balance",
		"address", address,
		"tokenAddress", tokenAddress)

	// USDT special handling - Known token decimals for common tokens
	decimals := constants.DefaultTronTokenDecimals // Default for TRC20 tokens

	// First check if we have decimals in the context
	if ctx.Value(constants.DecimalsKey) != nil {
		if dec, ok := ctx.Value(constants.DecimalsKey).(int); ok && dec > 0 {
			decimals = dec
			p.logger.Info("Using decimals from context", "decimals", decimals)
		}
	} else if tokenAddress == constants.TronUSDT {
		// This is USDT on Tron, which has 6 decimals
		p.logger.Info("Detected USDT token, using 6 decimals")
		decimals = constants.TronUSDTDecimals
	} else {
		// For other tokens, try to get token details
		tokenDetails, err := p.GetTokenDetails(ctx, tokenAddress)
		if err != nil {
			p.logger.Warn("Failed to get token details, using default decimals", "error", err)
		} else {
			decimals = tokenDetails.Decimals
		}
	}

	// For USDT and other TRC20 tokens, we need to properly format the address
	// Parameter needs to be the hex representation of the address without 0x or 41 prefix, padded to 64 chars
	parameter := ""

	// Special case handling for USDT as it's a common token
	if address[:1] == "T" {
		// This looks like a base58 address, need to get hex version
		hexAddr, err := p.addressToHex(address, ctx)
		if err != nil {
			p.logger.Error("Failed to convert address to hex", "address", address, "error", err)
			// Try another approach - hex is usually 41 + 40 chars
			if len(address) == 34 {
				// Use a direct request for conversion
				endpoint := fmt.Sprintf("%s/wallet/validateaddress", p.fullNodeURL)
				validResp, err := p.makeRequest(ctx, endpoint, map[string]interface{}{
					"address": address,
					"visible": true,
				})
				if err == nil {
					var validResult struct {
						Result     bool   `json:"result"`
						AddressHex string `json:"addressHex"`
					}
					if json.Unmarshal(validResp, &validResult) == nil && validResult.Result && validResult.AddressHex != "" {
						hexAddr = validResult.AddressHex
						p.logger.Info("Successfully converted address using validateaddress", "hexAddr", hexAddr)
					}
				}
			}

			if hexAddr == "" {
				// Still failed, return error
				return 0, fmt.Errorf("failed to convert address to hex format: %w", err)
			}
		}

		// Remove 41 prefix if present (Tron address type)
		if len(hexAddr) > 2 && hexAddr[:2] == "41" {
			hexAddr = hexAddr[2:]
		}

		// Pad to 64 chars as required for contract parameter
		parameter = fmt.Sprintf("%064s", hexAddr)
		p.logger.Info("Formatted address parameter", "parameter", parameter)
	} else {
		// Already in hex format
		cleanAddr := address
		if len(cleanAddr) > 2 && cleanAddr[:2] == "41" {
			cleanAddr = cleanAddr[2:]
		}
		parameter = fmt.Sprintf("%064s", cleanAddr)
	}

	p.logger.Info("Making contract call for balanceOf",
		"owner_address", address,
		"contract_address", tokenAddress,
		"parameter", parameter)

	// Direct API call for token balance
	endpoint := fmt.Sprintf("%s/wallet/triggerconstantcontract", p.fullNodeURL)

	// Prepare request
	reqBody := map[string]interface{}{
		"owner_address":     address,
		"contract_address":  tokenAddress,
		"function_selector": "balanceOf(address)",
		"parameter":         parameter,
	}

	// Make direct API call
	respBytes, err := p.makeRequest(ctx, endpoint, reqBody)
	if err != nil {
		return 0, fmt.Errorf("contract call failed: %w", err)
	}

	var response struct {
		Result struct {
			Result bool `json:"result"`
		} `json:"result"`
		ConstantResult []string `json:"constant_result"`
		Energy_used    int64    `json:"energy_used"`
		Error          string   `json:"Error,omitempty"`
	}

	if err := json.Unmarshal(respBytes, &response); err != nil {
		return 0, fmt.Errorf("failed to decode contract response: %w", err)
	}

	// Log the full response for debugging
	debugJSON, _ := json.Marshal(response)
	p.logger.Info("Raw contract response", "response", string(debugJSON))

	// Check for error in response
	if response.Error != "" {
		return 0, fmt.Errorf("contract error: %s", response.Error)
	}

	// Check if result is success
	if !response.Result.Result {
		// Try a fallback approach for USDT
		if tokenAddress == constants.TronUSDT {
			p.logger.Info("Using fallback approach for USDT")
			return 0, nil // Return 0 as balance for USDT if contract call failed
		}
		return 0, fmt.Errorf("contract call returned false result")
	}

	// Check for empty result
	if len(response.ConstantResult) == 0 {
		return 0, nil // Return 0 as balance
	}

	// Parse the balance from hex
	balance := new(big.Int)
	balanceHex := response.ConstantResult[0]
	if _, success := balance.SetString(balanceHex, 16); !success {
		p.logger.Error("Failed to parse balance hex", "hexValue", balanceHex)
		return 0, fmt.Errorf("failed to parse balance hex: %s", balanceHex)
	}

	// Convert using token decimals
	divisor := new(big.Float).SetInt(new(big.Int).Exp(
		big.NewInt(10),
		big.NewInt(int64(decimals)),
		nil,
	))
	floatBalance := new(big.Float).Quo(new(big.Float).SetInt(balance), divisor)

	// Convert to float64
	resultBalance, _ := floatBalance.Float64()
	return resultBalance, nil
}

// addressToHex converts a base58 Tron address to hex format
func (p *Provider) addressToHex(address string, ctx context.Context) (string, error) {
	// If it already looks like hex, just return it
	if len(address) > 2 && (address[:2] == "0x" || address[:2] == "41") {
		return address, nil
	}

	// For base58 addresses, we need to convert to hex
	// Call the API to do this conversion
	endpoint := fmt.Sprintf("%s/wallet/validateaddress", p.fullNodeURL)

	reqBody := map[string]interface{}{
		"address": address,
	}

	respBytes, err := p.makeRequest(ctx, endpoint, reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to validate/convert address: %w", err)
	}

	var response struct {
		Result     bool   `json:"result"`
		Message    string `json:"message"`
		HexAddress string `json:"hex_address"`
	}

	if err := json.Unmarshal(respBytes, &response); err != nil {
		return "", fmt.Errorf("failed to parse address validation response: %w", err)
	}

	if !response.Result {
		return "", fmt.Errorf("invalid address: %s", response.Message)
	}

	return response.HexAddress, nil
}

// makeRequest is a helper for making HTTP requests to the Tron API
func (p *Provider) makeRequest(ctx context.Context, endpoint string, payload interface{}) ([]byte, error) {
	reqJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(reqJSON))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	if p.apiKey != "" {
		if p.apiKey == "your-api-key-here" {
			p.logger.Error("Invalid Tron API key detected. Please update your TRON_API_KEY in .env with a valid key from https://www.trongrid.io/")
			return nil, fmt.Errorf("invalid Tron API key: please set a valid TronGrid API key in your environment")
		}
		req.Header.Set("TRON-PRO-API-KEY", p.apiKey)
	} else {
		p.logger.Warn("No Tron API key provided. This may result in rate limiting or blocked requests")
	}

	// Make the request
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		// Enhanced error handling for authentication issues
		if resp.StatusCode == http.StatusUnauthorized {
			p.logger.Error("Tron API returned 401 Unauthorized. Check your API key", "endpoint", endpoint)
			return nil, fmt.Errorf("authentication failed (401 Unauthorized): verify your TRON_API_KEY in .env")
		} else if resp.StatusCode == http.StatusTooManyRequests {
			p.logger.Error("Tron API rate limit exceeded (429)", "endpoint", endpoint)
			return nil, fmt.Errorf("rate limit exceeded (429): consider upgrading your TronGrid API plan")
		}

		return nil, fmt.Errorf("API returned non-200 status: %d", resp.StatusCode)
	}

	// Read and return body
	respBody := new(bytes.Buffer)
	_, err = respBody.ReadFrom(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return respBody.Bytes(), nil
}

// ValidateAddress checks if a Tron address is valid
func (p *Provider) ValidateAddress(address string) bool {
	// Simple validation for Base58 Tron addresses
	return len(address) == 34 && address[0] == 'T'
}

// GetTokenDetails gets information about a Tron TRC20 token
// Similar to TronWeb's contract.method().call() for standard ERC20/TRC20 methods
func (p *Provider) GetTokenDetails(ctx context.Context, tokenAddress string) (*models.TokenDetails, error) {
	p.logger.Info("Getting Tron token details", "address", tokenAddress)

	// Special handling for known tokens
	if tokenAddress == constants.TronUSDT {
		// This is USDT on Tron
		p.logger.Info("Using predefined details for USDT token")
		return &models.TokenDetails{
			Name:         constants.USDTName,
			Symbol:       constants.USDTSymbol,
			Decimals:     constants.TronUSDTDecimals,
			TotalSupply:  "33638290583.795235", // This will change over time
			TokenAddress: tokenAddress,
			ChainID:      "TRX",
		}, nil
	} else if tokenAddress == constants.TronUSDC {
		p.logger.Info("Using predefined details for USDC token")
		return &models.TokenDetails{
			Name:         constants.USDCName,
			Symbol:       constants.USDCSymbol,
			Decimals:     constants.TronUSDCDecimals,
			TokenAddress: tokenAddress,
			ChainID:      "TRX",
		}, nil
	}

	// For other tokens, try to get details from contract
	// Call name() function
	nameHex, err := p.callContract(ctx, tokenAddress, tokenAddress, "name()", "")
	if err != nil {
		p.logger.Warn("Failed to get token name", "error", err)
		// Continue with default name if this fails
		nameHex = ""
	}

	// Call symbol() function
	symbolHex, err := p.callContract(ctx, tokenAddress, tokenAddress, "symbol()", "")
	if err != nil {
		p.logger.Warn("Failed to get token symbol", "error", err)
		// Continue with default symbol if this fails
		symbolHex = ""
	}

	// Call decimals() function
	decimalsHex, err := p.callContract(ctx, tokenAddress, tokenAddress, "decimals()", "")
	if err != nil {
		p.logger.Warn("Failed to get token decimals", "error", err)
		// Default to constants.DefaultTronTokenDecimals for Tron tokens if we can't get it
		decimalsHex = fmt.Sprintf("%d", constants.DefaultTronTokenDecimals)
	}

	// Call totalSupply() function
	totalSupplyHex, err := p.callContract(ctx, tokenAddress, tokenAddress, "totalSupply()", "")
	if err != nil {
		p.logger.Warn("Failed to get token total supply", "error", err)
		// Continue with empty total supply if this fails
		totalSupplyHex = ""
	}

	// Parse name from hex (if available)
	name := fmt.Sprintf("TRC20 %s", tokenAddress[:8])
	if nameHex != "" {
		nameBytes, err := hex.DecodeString(nameHex)
		if err == nil && len(nameBytes) > 0 {
			name = string(nameBytes)
		}
	}

	// Parse symbol from hex (if available)
	symbol := fmt.Sprintf("TRC%s", tokenAddress[:4])
	if symbolHex != "" {
		symbolBytes, err := hex.DecodeString(symbolHex)
		if err == nil && len(symbolBytes) > 0 {
			symbol = string(symbolBytes)
		}
	}

	// Parse decimals from hex (if available)
	decimals := constants.DefaultTronTokenDecimals // Default for most TRC20 tokens
	if decimalsHex != "" {
		decimalsInt, err := strconv.ParseInt(decimalsHex, 16, 64)
		if err == nil {
			decimals = int(decimalsInt)
		}
	}

	// Parse total supply from hex (if available)
	totalSupply := "0"
	if totalSupplyHex != "" {
		bigTotalSupply := new(big.Int)
		_, success := bigTotalSupply.SetString(totalSupplyHex, 16)
		if success {
			totalSupply = bigTotalSupply.String()
		}
	}

	p.logger.Info("Token details retrieved",
		"name", name,
		"symbol", symbol,
		"decimals", decimals)

	return &models.TokenDetails{
		Name:         name,
		Symbol:       symbol,
		Decimals:     decimals,
		TotalSupply:  totalSupply,
		TokenAddress: tokenAddress,
		ChainID:      "TRX",
	}, nil
}

// callContract is a helper method to call TRC20 contract methods (similar to TronWeb's contract().at().method().call())
func (p *Provider) callContract(ctx context.Context, ownerAddress, contractAddress, functionSelector, parameter string) (string, error) {
	endpoint := fmt.Sprintf("%s/wallet/triggerconstantcontract", p.fullNodeURL)

	// Prepare request
	reqBody := map[string]interface{}{
		"owner_address":     ownerAddress,
		"contract_address":  contractAddress,
		"function_selector": functionSelector,
	}

	// Add parameter if provided
	if parameter != "" {
		reqBody["parameter"] = parameter
	}

	respBody, err := p.makeRequest(ctx, endpoint, reqBody)
	if err != nil {
		return "", fmt.Errorf("contract call failed: %w", err)
	}

	// Parse response
	var response struct {
		Result struct {
			Result bool `json:"result"`
		} `json:"result"`
		ConstantResult []string `json:"constant_result"`
		Error          string   `json:"Error,omitempty"`
	}

	if err := json.Unmarshal(respBody, &response); err != nil {
		return "", fmt.Errorf("failed to parse contract call response: %w", err)
	}

	// Check for errors
	if response.Error != "" {
		return "", fmt.Errorf("contract call error: %s", response.Error)
	}

	if !response.Result.Result {
		return "", fmt.Errorf("contract call returned false result")
	}

	// Get result from constant_result
	if len(response.ConstantResult) == 0 {
		return "", nil
	}

	return response.ConstantResult[0], nil
}

// WatchAddress sets up a watch for a Tron address
func (p *Provider) WatchAddress(ctx context.Context, address string, onUpdate func(update bitcoin.BlockUpdate)) {
	p.logger.Info("Setting up watch for Tron address", "address", address)
	// Implementation would poll Tron API to check for new transactions
}

// CreateTransaction creates a transaction on Tron
func (p *Provider) CreateTransaction(ctx context.Context, params bitcoin.TransactionParams) (string, error) {
	p.logger.Info("Creating Tron transaction",
		"from", params.FromAddress,
		"to", params.ToAddress,
		"amount", params.Amount)
	return "", fmt.Errorf("not implemented")
}

// CreateDepositAddress creates a new Tron deposit address
func (p *Provider) CreateDepositAddress(ctx context.Context) (*bitcoin.DepositAddress, error) {
	p.logger.Info("Creating new Tron deposit address")

	// In a real implementation, this would:
	// 1. Generate a new key pair
	// 2. Derive a Tron address from the public key
	// 3. Securely store or encrypt the private key

	// This is a mock implementation
	mockAddress := "TMuA6YqfCeX8EhbfYEg5y7S4DqzSJireY9"
	mockPrivateKey := "encrypted_private_key_mock"
	mockDerivationPath := "m/44'/195'/0'/0/0"

	return &bitcoin.DepositAddress{
		Address:        mockAddress,
		PrivateKey:     mockPrivateKey,
		DerivationPath: mockDerivationPath,
	}, nil
}

// StartBlockMonitor starts monitoring Tron blocks
func (p *Provider) StartBlockMonitor(ctx context.Context) error {
	p.logger.Info("Starting Tron block monitor")

	// Get current block height to start monitoring from
	currentBlock, err := p.getCurrentBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block number: %w", err)
	}

	p.logger.Info("Starting block monitor", "height", currentBlock)

	// Access the queue manager from context or dependency injection
	queueManager, ok := ctx.Value("queueManager").(*queue.QueueManager)
	if !ok {
		return fmt.Errorf("queue manager not found in context")
	}

	// Create job data
	jobData := map[string]interface{}{
		"lastProcessedBlockHeight": currentBlock,
		"chainSymbol":              "tron",
	}

	// Schedule job with queue manager
	options := map[string]interface{}{
		"repeat": map[string]interface{}{
			"every": 3000, // Check every 3 seconds (in milliseconds)
		},
		"jobId":            "tron-block-monitor",
		"removeOnComplete": false,
		"removeOnFail":     false,
	}

	jobId, err := queueManager.AddJob(ctx, constants.QueueTronBlockMonitor, jobData, options)
	if err != nil {
		p.logger.Error("Failed to schedule Tron block monitor job", "error", err)
		return fmt.Errorf("failed to schedule Tron block monitor job: %w", err)
	}

	p.logger.Info("Tron block monitor job scheduled", "jobId", jobId)
	return nil
}

// StopBlockMonitor stops the block monitoring process
func (p *Provider) StopBlockMonitor(ctx context.Context) error {
	p.logger.Info("Stopping Tron block monitor")

	// Access the queue manager from context or dependency injection
	queueManager, ok := ctx.Value("queueManager").(*queue.QueueManager)
	if !ok {
		return fmt.Errorf("queue manager not found in context")
	}

	// Get the job status before clearing
	status, err := queueManager.GetQueueStatus(ctx, constants.QueueTronBlockMonitor)
	if err != nil {
		p.logger.Warn("Failed to get queue status", "error", err)
		// Continue with clearing attempt even if status check fails
	} else {
		p.logger.Info("Tron block monitor queue status before stopping",
			"activeJobs", status["active"],
			"waitingJobs", status["waiting"],
			"completedJobs", status["completed"],
			"failedJobs", status["failed"])
	}

	// Clear the queue to stop processing
	err = queueManager.ClearQueue(ctx, constants.QueueTronBlockMonitor)
	if err != nil {
		p.logger.Error("Failed to clear Tron block monitor queue", "error", err)
		return fmt.Errorf("failed to clear Tron block monitor queue: %w", err)
	}

	p.logger.Info("Tron block monitor stopped successfully")
	return nil
}

// getCurrentBlockNumber gets the current block number from the Tron blockchain
func (p *Provider) getCurrentBlockNumber(ctx context.Context) (uint64, error) {
	p.logger.Info("Getting current Tron block number")

	endpoint := fmt.Sprintf("%s/wallet/getnowblock", p.fullNodeURL)

	respBody, err := p.makeRequest(ctx, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get current block: %w", err)
	}

	// Parse response
	var blockResp struct {
		BlockHeader struct {
			RawData struct {
				Number uint64 `json:"number"`
			} `json:"raw_data"`
		} `json:"block_header"`
		Error string `json:"Error,omitempty"`
	}

	if err := json.Unmarshal(respBody, &blockResp); err != nil {
		return 0, fmt.Errorf("failed to parse block response: %w", err)
	}

	// Check for error
	if blockResp.Error != "" {
		return 0, fmt.Errorf("API error: %s", blockResp.Error)
	}

	return blockResp.BlockHeader.RawData.Number, nil
}

// GetChainInfo gets current blockchain information
func (p *Provider) GetChainInfo(ctx context.Context) (*bitcoin.ChainInfo, error) {
	p.logger.Info("Getting Tron chain info")

	// Get current block
	blockNum, err := p.getCurrentBlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current block number: %w", err)
	}

	// Get block details to get the hash
	endpoint := fmt.Sprintf("%s/wallet/getblockbynum", p.fullNodeURL)

	reqBody := map[string]interface{}{
		"num": blockNum,
	}

	respBody, err := p.makeRequest(ctx, endpoint, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to get block details: %w", err)
	}

	// Parse response
	var blockResp struct {
		BlockID string `json:"blockID"`
		Error   string `json:"Error,omitempty"`
	}

	if err := json.Unmarshal(respBody, &blockResp); err != nil {
		return nil, fmt.Errorf("failed to parse block response: %w", err)
	}

	// Check for error
	if blockResp.Error != "" {
		return nil, fmt.Errorf("API error: %s", blockResp.Error)
	}

	return &bitcoin.ChainInfo{
		Height: uint64(blockNum),
		Hash:   blockResp.BlockID,
		Time:   time.Now(), // Block time isn't directly available, use current time
		Chain:  "tron",
	}, nil
}

// GetBlockDetails gets detailed information about a specific block
func (p *Provider) GetBlockDetails(ctx context.Context, blockHash string) (*bitcoin.BlockDetails, error) {
	p.logger.Info("Getting Tron block details", "hash", blockHash)

	// Get block by hash
	endpoint := fmt.Sprintf("%s/wallet/getblockbyid", p.fullNodeURL)

	reqBody := map[string]interface{}{
		"value": blockHash,
	}

	respBody, err := p.makeRequest(ctx, endpoint, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to get block details: %w", err)
	}

	// Parse response
	var blockResp struct {
		BlockHeader struct {
			RawData struct {
				Number     uint64 `json:"number"`
				ParentHash string `json:"parentHash"`
				Timestamp  int64  `json:"timestamp"`
			} `json:"raw_data"`
		} `json:"block_header"`
		Transactions []struct {
			TxID string `json:"txID"`
		} `json:"transactions"`
		BlockID string `json:"blockID"`
		Error   string `json:"Error,omitempty"`
	}

	if err := json.Unmarshal(respBody, &blockResp); err != nil {
		return nil, fmt.Errorf("failed to parse block response: %w", err)
	}

	// Check for error
	if blockResp.Error != "" {
		return nil, fmt.Errorf("API error: %s", blockResp.Error)
	}

	// Extract transaction IDs
	txIDs := make([]string, 0, len(blockResp.Transactions))
	for _, tx := range blockResp.Transactions {
		txIDs = append(txIDs, tx.TxID)
	}

	// Create block time from timestamp
	blockTime := time.Unix(blockResp.BlockHeader.RawData.Timestamp/1000, 0)

	return &bitcoin.BlockDetails{
		Hash:              blockHash,
		Height:            blockResp.BlockHeader.RawData.Number,
		Time:              blockTime,
		PreviousBlockHash: blockResp.BlockHeader.RawData.ParentHash,
		Transactions:      txIDs,
	}, nil
}

// GetTreasuryAddress returns the configured treasury address
func (p *Provider) GetTreasuryAddress() (string, error) {
	// In a real implementation, this would be fetched from configuration
	return "TMuA6YqfCeX8EhbfYEg5y7S4DqzSJireY9", nil
}
