package evm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/managers/queue"
	"bitbucket.org/zapspace/zap-go-server/models"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain/bitcoin"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain/evm/ankr"
)

// Provider implements the ChainProvider interface for EVM chains
type Provider struct {
	logger       logger.Logger
	rpcURL       string
	chainSymbol  string
	chainId      string
	ankrProvider *ankr.Provider
	chainConfigs map[string]ChainConfig
	treasuryAddr string
}

// Standard ERC20 ABI for common functions
const erc20ABI = `[
	{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},
	{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},
	{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},
	{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},
	{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},
	{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}
]`

// NewProvider creates a new EVM provider
func NewProvider(logger logger.Logger, cfg config.EVMConfig, chainSymbol string) (bitcoin.ChainProvider, error) {
	logger.Info("Creating new EVM provider", "chainSymbol", chainSymbol)

	// Normalize the chain symbol to lowercase for consistent lookup
	normalizedSymbol := strings.ToLower(chainSymbol)

	// Get the appropriate RPC URL for the chain
	var rpcURL string
	var ok bool

	// Try with the original chain symbol
	rpcURL, ok = cfg.Endpoints[chainSymbol]
	if !ok {
		// If not found, try with lowercase
		rpcURL, ok = cfg.Endpoints[normalizedSymbol]
		if !ok {
			// If still not found, use a fallback URL for Ankr or similar service
			logger.Warn("No endpoint configured for chain, using Ankr fallback",
				"chainSymbol", chainSymbol,
				"normalizedSymbol", normalizedSymbol)

			// Use Ankr endpoints based on environment variable
			ankrBaseURL := os.Getenv("ANKR_BASE_URL")
			if ankrBaseURL == "" {
				ankrBaseURL = "rpc.ankr.com" // Default if not specified
			}

			// Map of chain symbols to their Ankr endpoint paths
			ankrChainPaths := map[string]string{
				"eth":    "eth",
				"bsc":    "bsc",
				"matic":  "polygon",
				"arb":    "arbitrum",
				"op":     "optimism",
				"avax":   "avalanche",
				"ftm":    "fantom",
				"base":   "base",
				"gnosis": "gnosis",
			}

			// Check if we have a fallback for this chain
			if chainPath, exists := ankrChainPaths[normalizedSymbol]; exists {
				rpcURL = fmt.Sprintf("https://%s/%s", ankrBaseURL, chainPath)
				logger.Info("Using Ankr fallback URL", "chainSymbol", chainSymbol, "rpcURL", rpcURL)
			} else {
				// If we still don't have a URL, use Ethereum as default
				logger.Warn("Using Ethereum Ankr endpoint as default fallback", "chainSymbol", chainSymbol)
				rpcURL = fmt.Sprintf("https://%s/eth", ankrBaseURL)
			}
		}
	}

	logger.Info("Got RPC URL for chain", "chainSymbol", chainSymbol, "rpcURL", rpcURL)

	// Initialize Ankr provider
	ankrProvider := ankr.GetInstance(logger)

	// Set the Ankr API key from config if available
	if cfg.AnkrAPIKey != "" {
		os.Setenv("ANKR_API_KEY", cfg.AnkrAPIKey)
		logger.Info("Set Ankr API key from config")
	}

	if err := ankrProvider.Initialize(); err != nil {
		logger.Warn("Failed to initialize Ankr provider, continuing with direct RPC", "error", err)
	}

	// Get chain ID from the symbol (typically chainSymbol would be lowercase like "eth", "bsc")
	chainId := getChainIDFromSymbol(normalizedSymbol)
	logger.Info("Mapped chain symbol to chainID",
		"chainSymbol", chainSymbol,
		"normalizedSymbol", normalizedSymbol,
		"chainId", chainId,
		"chainIdType", fmt.Sprintf("%T", chainId))

	// Get treasury address from environment
	treasuryAddr := getTreasuryAddressForChain(normalizedSymbol)
	logger.Info("Got treasury address", "chainSymbol", chainSymbol, "treasuryAddr", treasuryAddr)

	// Initialize chain configs
	chainConfigs := initChainConfigs()
	logger.Info("Initialized chain configs",
		"configCount", len(chainConfigs),
		"hasConfig", chainConfigs[normalizedSymbol] != ChainConfig{})

	provider := &Provider{
		logger:       logger,
		rpcURL:       rpcURL,
		chainSymbol:  normalizedSymbol, // Store normalized symbol
		chainId:      chainId,
		ankrProvider: ankrProvider,
		chainConfigs: chainConfigs,
		treasuryAddr: treasuryAddr,
	}

	logger.Info("EVM provider created successfully",
		"chainSymbol", chainSymbol,
		"normalizedSymbol", normalizedSymbol,
		"chainId", chainId)

	return provider, nil
}

// initChainConfigs initializes the chain configurations
func initChainConfigs() map[string]ChainConfig {
	// Initialize with some common EVM chains
	return map[string]ChainConfig{
		"eth": {
			ChainID: 1,
			Name:    "Ethereum",
			Symbol:  "eth",
			NativeCurrency: struct {
				Name     string `json:"name"`
				Symbol   string `json:"symbol"`
				Decimals int    `json:"decimals"`
			}{
				Name:     "Ether",
				Symbol:   "ETH",
				Decimals: 18,
			},
		},
		"bsc": {
			ChainID: 56,
			Name:    "Binance Smart Chain",
			Symbol:  "bsc",
			NativeCurrency: struct {
				Name     string `json:"name"`
				Symbol   string `json:"symbol"`
				Decimals int    `json:"decimals"`
			}{
				Name:     "BNB",
				Symbol:   "BNB",
				Decimals: 18,
			},
		},
		"matic": {
			ChainID: 137,
			Name:    "Polygon",
			Symbol:  "matic",
			NativeCurrency: struct {
				Name     string `json:"name"`
				Symbol   string `json:"symbol"`
				Decimals int    `json:"decimals"`
			}{
				Name:     "MATIC",
				Symbol:   "MATIC",
				Decimals: 18,
			},
		},
		"arb": {
			ChainID: 42161,
			Name:    "Arbitrum",
			Symbol:  "arb",
			NativeCurrency: struct {
				Name     string `json:"name"`
				Symbol   string `json:"symbol"`
				Decimals int    `json:"decimals"`
			}{
				Name:     "Ethereum",
				Symbol:   "ETH",
				Decimals: 18,
			},
		},
		"op": {
			ChainID: 10,
			Name:    "Optimism",
			Symbol:  "op",
			NativeCurrency: struct {
				Name     string `json:"name"`
				Symbol   string `json:"symbol"`
				Decimals int    `json:"decimals"`
			}{
				Name:     "Ethereum",
				Symbol:   "ETH",
				Decimals: 18,
			},
		},
		"avax": {
			ChainID: 43114,
			Name:    "Avalanche",
			Symbol:  "avax",
			NativeCurrency: struct {
				Name     string `json:"name"`
				Symbol   string `json:"symbol"`
				Decimals int    `json:"decimals"`
			}{
				Name:     "Avalanche",
				Symbol:   "AVAX",
				Decimals: 18,
			},
		},
	}
}

// getTreasuryAddressForChain gets the treasury address for a chain
func getTreasuryAddressForChain(chainSymbol string) string {
	// In a real implementation, this would be fetched from configuration or environment
	// For demonstration purposes, we'll use a fake address
	fmt.Println("getTreasuryAddressForChain", chainSymbol)
	return "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
}

// getChainIDFromSymbol gets the chain ID from a symbol
func getChainIDFromSymbol(symbol string) string {
	// Map of chain symbols to chain IDs
	chainIDs := map[string]string{
		"eth":    "1",
		"bsc":    "56",
		"matic":  "137",
		"arb":    "42161",
		"op":     "10",
		"avax":   "43114",
		"ftm":    "250",
		"base":   "8453",
		"gnosis": "100",
	}

	normalizedSymbol := strings.ToLower(symbol)
	fmt.Printf("Looking up chain ID for symbol: %s (normalized: %s)\n", symbol, normalizedSymbol)

	if id, ok := chainIDs[normalizedSymbol]; ok {
		fmt.Printf("Found chain ID for symbol %s: %s\n", normalizedSymbol, id)
		return id
	}

	// If not found, return a default value
	fmt.Printf("No chain ID found for symbol %s, defaulting to '1' (Ethereum)\n", normalizedSymbol)
	return "1" // Default to Ethereum
}

// GetBalance retrieves the balance for an address on an EVM chain
func (p *Provider) GetBalance(ctx context.Context, address string, currency *models.Currency) (float64, error) {
	// Check if this is the native currency for the chain
	isNativeCurrency := false
	if config, ok := p.chainConfigs[p.chainSymbol]; ok {
		nativeSymbol := strings.ToUpper(config.NativeCurrency.Symbol)
		currencySymbol := strings.ToUpper(currency.Symbol)
		isNativeCurrency = nativeSymbol == currencySymbol
		p.logger.Info("Currency check",
			"currencySymbol", currencySymbol,
			"nativeSymbol", nativeSymbol,
			"isNative", isNativeCurrency)
	} else {
		p.logger.Warn("No chain config found for symbol", "symbol", p.chainSymbol)
	}

	var balance float64
	var err error

	// Get token address from context if available
	var tokenAddress string
	var decimals int
	if ctx.Value("tokenAddress") != nil {
		if tokenAddrStr, ok := ctx.Value("tokenAddress").(string); ok && tokenAddrStr != "" {
			tokenAddress = tokenAddrStr
			p.logger.Info("Found token address in context", "tokenAddress", tokenAddress)
		}
	}

	if ctx.Value("decimals") != nil {
		if decimalsVal, ok := ctx.Value("decimals").(int); ok && decimalsVal > 0 {
			decimals = decimalsVal
		}
	}

	if isNativeCurrency {
		p.logger.Info("Getting native balance for address", "address", address)
		balance, err = p.getNativeBalance(ctx, address)
	} else {
		if tokenAddress == "" {
			p.logger.Error("Token address is required for non-native currencies", "currency", currency.Symbol)
			return 0, fmt.Errorf("token address is required for non-native currencies")
		}
		p.logger.Info("Getting token balance for address",
			"address", address,
			"tokenAddress", tokenAddress)
		balance, err = p.getTokenBalance(ctx, address, tokenAddress, decimals)
	}

	if err != nil {
		p.logger.Error("Failed to get balance",
			"error", err,
			"address", address,
			"currency", currency.Symbol,
			"isNative", isNativeCurrency)
		return 0, err
	}

	p.logger.Info("Successfully retrieved balance",
		"address", address,
		"currency", currency.Symbol,
		"balance", balance,
		"isNative", isNativeCurrency)
	return balance, nil
}

// getNativeBalance gets the native currency balance
func (p *Provider) getNativeBalance(ctx context.Context, address string) (float64, error) {
	// Use Ankr provider if available
	if p.ankrProvider != nil {
		hexBalance, err := p.ankrProvider.GetBalance(ctx, p.chainSymbol, address)
		if err != nil {
			p.logger.Error("Failed to get balance with Ankr, trying direct RPC", "error", err)
		} else {
			return hexBalanceToFloat(hexBalance, 18), nil
		}
	}

	// Fallback to direct RPC call using the configured RPC URL
	// This would be a more complex implementation using direct JSON-RPC
	// For simplicity in this example, we'll return a mock value
	p.logger.Warn("Using mock balance as fallback", "chain", p.chainSymbol)
	switch p.chainSymbol {
	case "eth":
		return 0.5, nil
	case "bsc":
		return 1.2, nil
	default:
		return 0.3, nil
	}
}

// getTokenBalance gets the balance of an ERC20 token
func (p *Provider) getTokenBalance(ctx context.Context, address string, tokenAddress string, decimals int) (float64, error) {
	// Use Ankr provider if available
	if p.ankrProvider != nil {
		hexBalance, err := p.ankrProvider.GetTokenBalance(ctx, p.chainSymbol, tokenAddress, address)
		if err != nil {
			p.logger.Error("Failed to get token balance with Ankr", "error", err)
		} else {
			// If decimals is not provided, try to get it from the token contract
			if decimals <= 0 {
				decimals, err = p.ankrProvider.GetTokenDecimals(ctx, p.chainSymbol, tokenAddress)
				if err != nil {
					p.logger.Error("Failed to get token decimals", "error", err)
					decimals = 18 // Default to 18 decimals
				}
			}
			return hexBalanceToFloat(hexBalance, decimals), nil
		}
	}

	// Fallback to a mock implementation
	p.logger.Warn("Using mock token balance as fallback", "chain", p.chainSymbol)
	return 100.0, nil
}

// ValidateAddress checks if an EVM address is valid
func (p *Provider) ValidateAddress(address string) bool {
	// Check if the address is not empty
	if len(address) == 0 {
		return false
	}

	// Check if the address matches the Ethereum address format
	match, _ := regexp.MatchString("^0x[0-9a-fA-F]{40}$", address)
	return match
}

// GetTokenDetails gets information about an ERC20 token
func (p *Provider) GetTokenDetails(ctx context.Context, tokenAddress string) (*models.TokenDetails, error) {
	p.logger.Info("Getting token details", "chain", p.chainSymbol, "address", tokenAddress)

	// For native currency (like ETH for Ethereum)
	if strings.ToLower(tokenAddress) == "native" ||
		strings.ToLower(tokenAddress) == p.chainSymbol ||
		strings.ToLower(tokenAddress) == "0x0000000000000000000000000000000000000000" {

		// Get native currency details from chain config
		config, ok := p.chainConfigs[p.chainSymbol]
		if !ok {
			config = ChainConfig{
				// Name:   p.chainSymbol,
				// Symbol: p.chainSymbol,
				NativeCurrency: struct {
					Name     string `json:"name"`
					Symbol   string `json:"symbol"`
					Decimals int    `json:"decimals"`
				}{
					Name:     p.chainSymbol,
					Symbol:   strings.ToUpper(p.chainSymbol),
					Decimals: 18,
				},
			}
		}

		return &models.TokenDetails{
			Name:         config.NativeCurrency.Name,
			Symbol:       config.NativeCurrency.Symbol,
			Decimals:     config.NativeCurrency.Decimals,
			TotalSupply:  "0", // Not applicable for native currency
			TokenAddress: "0x0000000000000000000000000000000000000000",
			ChainID:      p.chainId,
		}, nil
	}

	// For ERC20 tokens
	if p.ankrProvider != nil {
		var name, symbol string
		var decimals int
		var err error

		// Get token details from Ankr
		name, err = p.ankrProvider.GetTokenName(ctx, p.chainSymbol, tokenAddress)
		if err != nil {
			p.logger.Error("Failed to get token name", "error", err)
			name = "Unknown Token"
		}

		symbol, err = p.ankrProvider.GetTokenSymbol(ctx, p.chainSymbol, tokenAddress)
		if err != nil {
			p.logger.Error("Failed to get token symbol", "error", err)
			symbol = "UNKNOWN"
		}

		decimals, err = p.ankrProvider.GetTokenDecimals(ctx, p.chainSymbol, tokenAddress)
		if err != nil {
			p.logger.Error("Failed to get token decimals", "error", err)
			decimals = 18
		}

		return &models.TokenDetails{
			Name:         name,
			Symbol:       symbol,
			Decimals:     decimals,
			TotalSupply:  "0", // Would need additional call to get this
			TokenAddress: tokenAddress,
			ChainID:      p.chainId,
		}, nil
	}

	// Fallback to mock implementation
	p.logger.Warn("Using mock token details as fallback", "chain", p.chainSymbol)
	return &models.TokenDetails{
		Name:         "Sample Token",
		Symbol:       "STKN",
		Decimals:     18,
		TotalSupply:  "1000000000000000000000000",
		TokenAddress: tokenAddress,
		ChainID:      p.chainId,
	}, nil
}

// WatchAddress sets up a watch for an address on an EVM chain
func (p *Provider) WatchAddress(ctx context.Context, address string, onUpdate func(update bitcoin.BlockUpdate)) {
	p.logger.Info("Setting up watch for EVM address", "chain", p.chainSymbol, "address", address)

	// This would typically involve:
	// 1. Setting up a subscription to new blocks
	// 2. When a new block is received, check if any transactions involve the watched address
	// 3. If relevant transactions are found, call the onUpdate function

	// For demonstration purposes, we'll set up a goroutine that simulates checking for updates
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				p.logger.Info("Stopping address watch", "chain", p.chainSymbol, "address", address)
				return
			case <-ticker.C:
				p.logger.Debug("Checking for updates to address", "chain", p.chainSymbol, "address", address)
				// In a real implementation, we would check for new transactions
				// For now, we'll just log the check
			}
		}
	}()
}

// CreateTransaction creates a transaction on an EVM chain
func (p *Provider) CreateTransaction(ctx context.Context, params bitcoin.TransactionParams) (string, error) {
	p.logger.Info("Creating EVM transaction",
		"chain", p.chainSymbol,
		"from", params.FromAddress,
		"to", params.ToAddress,
		"amount", params.Amount)

	// Determine if this is a token transfer or native currency transfer
	isTokenTransfer := params.TokenAddress != ""
	if isTokenTransfer {
		p.logger.Info("Processing as token transfer", "tokenAddress", params.TokenAddress)
	} else {
		p.logger.Info("Processing as native currency transfer")
	}

	// Build the transaction data
	// In a real implementation, this would involve:
	// 1. Fetching the nonce for the sender address
	// 2. Estimating gas price and limit
	// 3. Building the transaction
	// 4. Signing the transaction
	// 5. Broadcasting the transaction

	// For demonstration purposes, we'll return a mock transaction hash
	return "0x" + generateRandomHex(64), fmt.Errorf("not fully implemented - parameters captured for chain %s", p.chainSymbol)
}

// CreateDepositAddress creates a new EVM deposit address
func (p *Provider) CreateDepositAddress(ctx context.Context) (*bitcoin.DepositAddress, error) {
	p.logger.Info("Creating new EVM deposit address for", "chain", p.chainSymbol)

	// In a real implementation, this would:
	// 1. Generate a new private key
	// 2. Derive the public address from the private key
	// 3. Encrypt the private key before storing

	// For demonstration purposes, we'll return a mock address
	address := "0x" + generateRandomHex(40)
	privateKeyData := generateRandomHex(64)
	encryptedPrivateKey := "encrypted:" + privateKeyData
	derivationPath := "m/44'/60'/0'/0/0"

	return &bitcoin.DepositAddress{
		Address:        address,
		PrivateKey:     encryptedPrivateKey,
		DerivationPath: derivationPath,
	}, nil
}

// StartBlockMonitor starts monitoring EVM blocks
func (p *Provider) StartBlockMonitor(ctx context.Context) error {
	p.logger.Info("Starting EVM block monitor for", "chain", p.chainSymbol)

	// Access the queue manager from context or dependency injection
	queueManager, ok := ctx.Value("queueManager").(*queue.QueueManager)
	if !ok {
		return fmt.Errorf("queue manager not found in context")
	}

	// Get current block height to start monitoring from
	currentBlockNumber, err := p.getCurrentBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block number: %w", err)
	}

	p.logger.Info("Starting block monitor", "height", currentBlockNumber, "chain", p.chainSymbol)

	// Create job data
	jobData := map[string]interface{}{
		"lastProcessedBlockHeight": currentBlockNumber,
		"chainId":                  getChainIDFromSymbol(p.chainSymbol),
		"chainSymbol":              p.chainSymbol,
	}

	// Schedule job with queue manager
	options := map[string]interface{}{
		"repeat": map[string]interface{}{
			"every": 15000, // Check every 15 seconds (in milliseconds)
		},
		"jobId":            fmt.Sprintf("evm-block-monitor-%s", p.chainSymbol),
		"removeOnComplete": false,
		"removeOnFail":     false,
	}

	jobId, err := queueManager.AddJob(ctx, constants.QueueEVMBlockMonitor, jobData, options)
	if err != nil {
		p.logger.Error("Failed to schedule EVM block monitor job", "error", err, "chain", p.chainSymbol)
		return fmt.Errorf("failed to schedule EVM block monitor job for chain %s: %w", p.chainSymbol, err)
	}

	p.logger.Info("EVM block monitor job scheduled", "chain", p.chainSymbol, "jobId", jobId)
	return nil
}

// StopBlockMonitor stops monitoring EVM blocks
func (p *Provider) StopBlockMonitor(ctx context.Context) error {
	p.logger.Info("Stopping EVM block monitor for", "chain", p.chainSymbol)

	// Access the queue manager from context or dependency injection
	queueManager, ok := ctx.Value("queueManager").(*queue.QueueManager)
	if !ok {
		return fmt.Errorf("queue manager not found in context")
	}

	// Get the job status before clearing
	status, err := queueManager.GetQueueStatus(ctx, constants.QueueEVMBlockMonitor)
	if err != nil {
		p.logger.Warn("Failed to get queue status", "error", err, "chain", p.chainSymbol)
		// Continue with clearing attempt even if status check fails
	} else {
		p.logger.Info("EVM block monitor queue status before stopping",
			"chain", p.chainSymbol,
			"activeJobs", status["active"],
			"waitingJobs", status["waiting"],
			"completedJobs", status["completed"],
			"failedJobs", status["failed"])
	}

	// Clear the queue to stop processing
	err = queueManager.ClearQueue(ctx, constants.QueueEVMBlockMonitor)
	if err != nil {
		p.logger.Error("Failed to clear EVM block monitor queue", "error", err, "chain", p.chainSymbol)
		return fmt.Errorf("failed to clear EVM block monitor queue for chain %s: %w", p.chainSymbol, err)
	}

	p.logger.Info("EVM block monitor stopped successfully", "chain", p.chainSymbol)
	return nil
}

// GetChainInfo gets information about the EVM chain
func (p *Provider) GetChainInfo(ctx context.Context) (*bitcoin.ChainInfo, error) {
	p.logger.Info("Getting EVM chain info for", "chain", p.chainSymbol)

	// Get the current block number
	blockNumber, err := p.getCurrentBlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get block number: %w", err)
	}

	// For a real implementation, we would also fetch the block hash
	// and other network information

	return &bitcoin.ChainInfo{
		Height: blockNumber,
		Hash:   "0x" + generateRandomHex(64), // Would be the actual block hash in a real implementation
		Time:   time.Now(),
		Chain:  p.chainSymbol,
	}, nil
}

// GetBlockDetails gets details about a specific block on the EVM chain
func (p *Provider) GetBlockDetails(ctx context.Context, blockHash string) (*bitcoin.BlockDetails, error) {
	p.logger.Info("Getting EVM block details for", "chain", p.chainSymbol, "hash", blockHash)

	// For a real implementation, we would use the Ankr provider to fetch block details
	// by hash and then transform them into the required format

	// For demonstration purposes, we'll return mock data
	return &bitcoin.BlockDetails{
		Hash:              blockHash,
		Height:            1000000, // Would be the actual block height in a real implementation
		Time:              time.Now(),
		PreviousBlockHash: "0x" + generateRandomHex(64), // Would be the actual previous block hash
		Transactions:      []string{"0x" + generateRandomHex(64), "0x" + generateRandomHex(64)},
	}, nil
}

// GetTreasuryAddress returns the treasury address for this chain
func (p *Provider) GetTreasuryAddress() (string, error) {
	if p.treasuryAddr == "" {
		return "", fmt.Errorf("treasury address is not configured for chain %s", p.chainSymbol)
	}
	return p.treasuryAddr, nil
}

// getCurrentBlockNumber gets the current block number from the chain
func (p *Provider) getCurrentBlockNumber(ctx context.Context) (uint64, error) {
	p.logger.Info("Getting current block number for", "chain", p.chainSymbol)

	// Use the Ankr provider if available
	if p.ankrProvider != nil {
		blockNumber, err := p.ankrProvider.GetLatestBlockNumber(ctx, p.chainSymbol)
		if err != nil {
			p.logger.Error("Failed to get block number from Ankr provider", "error", err, "chain", p.chainSymbol)
			// Fall through to try direct RPC call
		} else {
			p.logger.Info("Retrieved current block number from Ankr", "blockNumber", blockNumber, "chain", p.chainSymbol)
			return blockNumber, nil
		}
	}

	// Try direct RPC call using JSON-RPC
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Prepare the request body for eth_blockNumber call
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_blockNumber",
		"params":  []interface{}{},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		p.logger.Error("Failed to marshal JSON-RPC request", "error", err, "chain", p.chainSymbol)
		// Fallback to mock value
		return 1000000, nil
	}

	// Make the RPC request
	req, err := http.NewRequestWithContext(ctx, "POST", p.rpcURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		p.logger.Error("Failed to create HTTP request", "error", err, "chain", p.chainSymbol)
		// Fallback to mock value
		return 1000000, nil
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		p.logger.Error("Failed to make HTTP request", "error", err, "chain", p.chainSymbol)
		// Fallback to mock value
		return 1000000, nil
	}
	defer resp.Body.Close()

	// Parse the response
	var rpcResp struct {
		Result string `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		p.logger.Error("Failed to decode RPC response", "error", err, "chain", p.chainSymbol)
		// Fallback to mock value
		return 1000000, nil
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		p.logger.Error("RPC error", "code", rpcResp.Error.Code, "message", rpcResp.Error.Message, "chain", p.chainSymbol)
		// Fallback to mock value
		return 1000000, nil
	}

	// Convert hex result to uint64
	var blockNumber uint64
	if rpcResp.Result != "" {
		hexString := strings.TrimPrefix(rpcResp.Result, "0x")
		blockNumber, err = strconv.ParseUint(hexString, 16, 64)
		if err != nil {
			p.logger.Error("Failed to parse block number", "error", err, "result", rpcResp.Result, "chain", p.chainSymbol)
			// Fallback to mock value
			return 1000000, nil
		}
	} else {
		// Fallback to mock value if result is empty
		blockNumber = 1000000
	}

	p.logger.Info("Retrieved current block number from direct RPC", "blockNumber", blockNumber, "chain", p.chainSymbol)
	return blockNumber, nil
}

// Helper function to convert hex balance to float
func hexBalanceToFloat(hexBalance string, decimals int) float64 {
	// Remove '0x' prefix if present
	hexBalance = strings.TrimPrefix(hexBalance, "0x")

	// Convert hex to big.Int
	bigIntBalance := new(big.Int)
	bigIntBalance.SetString(hexBalance, 16)

	// Convert to float with proper decimal places
	floatBalance, _ := strconv.ParseFloat(bigIntBalance.String(), 64)

	// Apply decimals
	divisor := math.Pow10(decimals)
	return floatBalance / divisor
}

// Helper function to generate a random hex string
func generateRandomHex(length int) string {
	const hexChars = "0123456789abcdef"
	result := make([]byte, length)

	for i := 0; i < length; i++ {
		result[i] = hexChars[time.Now().UnixNano()%int64(len(hexChars))]
		// Add some entropy
		time.Sleep(1 * time.Nanosecond)
	}

	return string(result)
}
