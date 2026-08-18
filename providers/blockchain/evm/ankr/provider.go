package ankr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

const (
	// DefaultAnkrBaseURL is the default base URL for Ankr's API
	DefaultAnkrBaseURL = "rpc.ankr.com"
)

// Provider implements Ankr RPC functionality
type Provider struct {
	logger        logger.Logger
	apiKey        string
	baseURL       string
	httpClient    *http.Client
	rpcEndpoints  map[string]string
	mutex         sync.RWMutex
	requestID     int
	initializing  bool
	initialized   bool
	requestIDLock sync.Mutex
}

var (
	instance *Provider
	once     sync.Once
)

// GetInstance returns a singleton instance of the Ankr provider
func GetInstance(logger logger.Logger) *Provider {
	once.Do(func() {
		instance = &Provider{
			logger:       logger,
			apiKey:       os.Getenv("ANKR_API_KEY"),
			baseURL:      getEnvOrDefault("ANKR_BASE_URL", DefaultAnkrBaseURL),
			rpcEndpoints: make(map[string]string),
			httpClient: &http.Client{
				Timeout: 10 * time.Second,
			},
			requestID: 1,
		}
	})
	return instance
}

// getEnvOrDefault gets an environment variable or returns the default value
func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// Initialize sets up the Ankr provider
func (p *Provider) Initialize() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.initializing {
		return nil
	}

	if p.initialized {
		return nil
	}

	p.initializing = true

	p.logger.Info("Initializing Ankr Provider",
		"baseURL", p.baseURL,
		"hasAPIKey", p.apiKey != "")

	// Initialize chain endpoints
	p.setupRPCEndpoints()

	p.initialized = true
	p.initializing = false
	return nil
}

// setupRPCEndpoints initializes the map of chain symbols to RPC paths
func (p *Provider) setupRPCEndpoints() {
	// Define a map of chain symbol -> endpoint path
	// This is similar to the Node.js implementation's rpcMap
	rpcPaths := map[string]string{
		"eth":     "eth",
		"bsc":     "bsc",
		"matic":   "polygon",
		"avax":    "avalanche",
		"ftm":     "fantom",
		"arb":     "arbitrum",
		"op":      "optimism",
		"base":    "base",
		"gnosis":  "gnosis",
		"celo":    "celo",
		"fuse":    "fuse",
		"harmony": "harmony",
		"sol":     "solana",
		"btc":     "bitcoin",
		"doge":    "dogecoin",
		"ltc":     "litecoin",
		"ton":     "premium-http/ton_api_v2",
		"trx":     "premium-http/tron",
		"all":     "multichain",
	}

	// Get overrides from environment variables (format: ANKR_RPC_PATHS=eth:custom/path,bsc:other/path)
	if pathsStr := os.Getenv("ANKR_RPC_PATHS"); pathsStr != "" {
		p.logger.Info("Found custom RPC paths in environment", "ANKR_RPC_PATHS", pathsStr)

		pairs := strings.Split(pathsStr, ",")
		for _, pair := range pairs {
			parts := strings.SplitN(pair, ":", 2)
			if len(parts) == 2 {
				chain := strings.ToLower(strings.TrimSpace(parts[0]))
				path := strings.TrimSpace(parts[1])
				rpcPaths[chain] = path
				p.logger.Info("Added custom RPC path", "chain", chain, "path", path)
			}
		}
	}

	// Build the full URLs from the paths
	for chain, path := range rpcPaths {
		p.rpcEndpoints[chain] = fmt.Sprintf("https://%s/%s", p.baseURL, path)
	}

	// Add aliases for chains with different symbols
	aliases := map[string]string{
		"ethereum":   "eth",
		"binance":    "bsc",
		"smartchain": "bsc",
		"bnb":        "bsc",
		"polygon":    "matic",
		"avalanche":  "avax",
		"fantom":     "ftm",
		"arbitrum":   "arb",
		"optimism":   "op",
		"solana":     "sol",
		"bitcoin":    "btc",
		"litecoin":   "ltc",
		"dogecoin":   "doge",
		"tron":       "trx",
	}

	// Add aliases to the main map
	for alias, target := range aliases {
		if url, exists := p.rpcEndpoints[target]; exists {
			p.rpcEndpoints[alias] = url
		}
	}

	p.logger.Info("Ankr RPC endpoints initialized",
		"baseURL", p.baseURL,
		"count", len(p.rpcEndpoints))
}

// GetRPCURL returns the RPC URL for a given chain symbol
func (p *Provider) GetRPCURL(chainSymbol string) string {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Normalize to lowercase
	normalizedSymbol := strings.ToLower(chainSymbol)

	// Check if we have a specific RPC URL for this chain
	if url, ok := p.rpcEndpoints[normalizedSymbol]; ok {
		// Add API key if available
		if p.apiKey != "" {
			url = fmt.Sprintf("%s/%s", url, p.apiKey)
		}

		p.logger.Debug("Found specific RPC URL for chain",
			"chainSymbol", chainSymbol,
			"normalizedSymbol", normalizedSymbol,
			"url", maskAPIKey(url, p.apiKey))
		return url
	}

	// Log that we're falling back to the multichain endpoint
	p.logger.Warn("No specific RPC URL found for chain, using multichain endpoint",
		"chainSymbol", chainSymbol,
		"normalizedSymbol", normalizedSymbol)

	// If we have an API key, use the multichain endpoint with the API key
	if p.apiKey != "" {
		multiEndpoint := fmt.Sprintf("https://%s/multichain/%s", p.baseURL, p.apiKey)
		p.logger.Info("Using multichain endpoint with API key",
			"endpoint", maskAPIKey(multiEndpoint, p.apiKey))
		return multiEndpoint
	}

	// If we don't have an API key, use Ethereum as fallback
	p.logger.Warn("No API key configured, using Ethereum endpoint as fallback",
		"chainSymbol", chainSymbol)
	return fmt.Sprintf("https://%s/eth", p.baseURL) // No API key available
}

// maskAPIKey masks the API key in a URL for logging purposes
func maskAPIKey(url, apiKey string) string {
	if apiKey == "" {
		return url
	}

	// Only show first 4 and last 4 characters of the API key
	if len(apiKey) > 8 {
		maskedKey := apiKey[:4] + "..." + apiKey[len(apiKey)-4:]
		return strings.Replace(url, apiKey, maskedKey, 1)
	}

	return strings.Replace(url, apiKey, "***", 1)
}

// Call makes a JSON-RPC call to the Ankr API
func (p *Provider) Call(ctx context.Context, chainSymbol, method string, params []interface{}) (json.RawMessage, error) {
	// Log the call attempt
	p.logger.Debug("Making Ankr JSON-RPC call",
		"chain", chainSymbol,
		"method", method,
		"paramsCount", len(params))

	// Get the RPC URL for this chain
	rpcURL := p.GetRPCURL(chainSymbol)
	if rpcURL == "" {
		p.logger.Error("Failed to get RPC URL", "chain", chainSymbol)
		return nil, fmt.Errorf("no RPC URL available for chain: %s", chainSymbol)
	}

	// Make the call with a timeout context
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	result, err := p.callRPC(callCtx, rpcURL, method, params)
	if err != nil {
		p.logger.Error("RPC call failed",
			"chain", chainSymbol,
			"method", method,
			"url", maskAPIKey(rpcURL, p.apiKey),
			"error", err)

		// For certain chains, we can fall back to the Ethereum endpoint as a last resort
		if chainSymbol != "eth" && !strings.Contains(rpcURL, "/eth/") {
			fallbackURL := fmt.Sprintf("https://%s/eth", p.baseURL)
			if p.apiKey != "" {
				fallbackURL = fmt.Sprintf("%s/%s", fallbackURL, p.apiKey)
			}

			p.logger.Warn("Attempting fallback to Ethereum endpoint",
				"chain", chainSymbol,
				"method", method,
				"fallbackURL", maskAPIKey(fallbackURL, p.apiKey))

			result, err = p.callRPC(callCtx, fallbackURL, method, params)
			if err != nil {
				p.logger.Error("Fallback RPC call failed",
					"chain", chainSymbol,
					"method", method,
					"error", err)
				return nil, err
			}
			p.logger.Info("Fallback RPC call succeeded", "chain", chainSymbol, "method", method)
			return result, nil
		}

		return nil, err
	}

	p.logger.Debug("RPC call successful",
		"chain", chainSymbol,
		"method", method,
		"resultSize", len(result))
	return result, nil
}

// callRPC is a helper method to make a JSON-RPC call
func (p *Provider) callRPC(ctx context.Context, rpcURL, method string, params []interface{}) (json.RawMessage, error) {
	// Get a unique request ID
	id := p.getNextRequestID()

	// Create the request payload
	req := AnkrRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      id,
	}

	// Marshal the request to JSON
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %w", err)
	}

	// Create a new HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("error creating HTTP request: %w", err)
	}

	// Set headers
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	// Make the request
	resp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("error making HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	// Check the HTTP status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	// Parse the response
	var rpcResp AnkrRPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return nil, fmt.Errorf("error unmarshaling response: %w", err)
	}

	// Check if there's an error in the response
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %d - %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return rpcResp.Result, nil
}

// getNextRequestID returns the next request ID in a thread-safe manner
func (p *Provider) getNextRequestID() int {
	p.requestIDLock.Lock()
	defer p.requestIDLock.Unlock()
	id := p.requestID
	p.requestID++
	return id
}

// GetBalance gets the balance for an address on a specific chain
func (p *Provider) GetBalance(ctx context.Context, chainSymbol, address string) (string, error) {
	// Call eth_getBalance to get the balance in wei
	result, err := p.Call(ctx, chainSymbol, "eth_getBalance", []interface{}{address, "latest"})
	if err != nil {
		return "0", fmt.Errorf("error getting balance: %w", err)
	}

	// Extract the balance from the result
	var hexBalance string
	if err := json.Unmarshal(result, &hexBalance); err != nil {
		return "0", fmt.Errorf("error unmarshaling balance: %w", err)
	}

	return hexBalance, nil
}

// GetTokenBalance gets the balance of a token for an address on a specific chain
func (p *Provider) GetTokenBalance(ctx context.Context, chainSymbol, tokenAddress, userAddress string) (string, error) {
	// ERC20 balanceOf function signature + address parameter (padded to 32 bytes)
	data := fmt.Sprintf("0x70a08231000000000000000000000000%s", userAddress[2:])

	// Call eth_call to get the token balance
	result, err := p.Call(ctx, chainSymbol, "eth_call", []interface{}{
		map[string]string{
			"to":   tokenAddress,
			"data": data,
		},
		"latest",
	})
	if err != nil {
		return "0", fmt.Errorf("error getting token balance: %w", err)
	}

	// Extract the balance from the result
	var hexBalance string
	if err := json.Unmarshal(result, &hexBalance); err != nil {
		return "0", fmt.Errorf("error unmarshaling token balance: %w", err)
	}

	return hexBalance, nil
}

// GetTokenDecimals gets the decimals for a token
func (p *Provider) GetTokenDecimals(ctx context.Context, chainSymbol, tokenAddress string) (int, error) {
	// ERC20 decimals function signature
	data := "0x313ce567"

	// Call eth_call to get the token decimals
	result, err := p.Call(ctx, chainSymbol, "eth_call", []interface{}{
		map[string]string{
			"to":   tokenAddress,
			"data": data,
		},
		"latest",
	})
	if err != nil {
		return 0, fmt.Errorf("error getting token decimals: %w", err)
	}

	// Extract the decimals from the result
	var hexDecimals string
	if err := json.Unmarshal(result, &hexDecimals); err != nil {
		return 0, fmt.Errorf("error unmarshaling token decimals: %w", err)
	}

	// Convert hex to int
	var decimals int
	fmt.Sscanf(hexDecimals, "0x%x", &decimals)

	return decimals, nil
}

// GetTokenSymbol gets the symbol for a token
func (p *Provider) GetTokenSymbol(ctx context.Context, chainSymbol, tokenAddress string) (string, error) {
	// ERC20 symbol function signature
	data := "0x95d89b41"

	// Call eth_call to get the token symbol
	result, err := p.Call(ctx, chainSymbol, "eth_call", []interface{}{
		map[string]string{
			"to":   tokenAddress,
			"data": data,
		},
		"latest",
	})
	if err != nil {
		return "", fmt.Errorf("error getting token symbol: %w", err)
	}

	// Extract the symbol from the result (requires additional parsing)
	var hexResult string
	if err := json.Unmarshal(result, &hexResult); err != nil {
		return "", fmt.Errorf("error unmarshaling token symbol: %w", err)
	}

	// Parse the hex string to get the actual symbol
	symbolBytes, err := decodeERC20String(hexResult)
	if err != nil {
		return "", fmt.Errorf("error decoding token symbol: %w", err)
	}

	return string(symbolBytes), nil
}

// GetTokenName gets the name for a token
func (p *Provider) GetTokenName(ctx context.Context, chainSymbol, tokenAddress string) (string, error) {
	// ERC20 name function signature
	data := "0x06fdde03"

	// Call eth_call to get the token name
	result, err := p.Call(ctx, chainSymbol, "eth_call", []interface{}{
		map[string]string{
			"to":   tokenAddress,
			"data": data,
		},
		"latest",
	})
	if err != nil {
		return "", fmt.Errorf("error getting token name: %w", err)
	}

	// Extract the name from the result
	var hexResult string
	if err := json.Unmarshal(result, &hexResult); err != nil {
		return "", fmt.Errorf("error unmarshaling token name: %w", err)
	}

	// Parse the hex string to get the actual name
	nameBytes, err := decodeERC20String(hexResult)
	if err != nil {
		return "", fmt.Errorf("error decoding token name: %w", err)
	}

	return string(nameBytes), nil
}

// GetLatestBlockNumber gets the latest block number for a chain
func (p *Provider) GetLatestBlockNumber(ctx context.Context, chainSymbol string) (uint64, error) {
	// Call eth_blockNumber to get the latest block number
	result, err := p.Call(ctx, chainSymbol, "eth_blockNumber", []interface{}{})
	if err != nil {
		return 0, fmt.Errorf("error getting latest block number: %w", err)
	}

	// Extract the block number from the result
	var hexBlockNumber string
	if err := json.Unmarshal(result, &hexBlockNumber); err != nil {
		return 0, fmt.Errorf("error unmarshaling block number: %w", err)
	}

	// Convert hex to uint64
	var blockNumber uint64
	fmt.Sscanf(hexBlockNumber, "0x%x", &blockNumber)

	return blockNumber, nil
}

// GetBlockByNumber gets a block by its number
func (p *Provider) GetBlockByNumber(ctx context.Context, chainSymbol string, blockNumber uint64, fullTransactions bool) (*EVMBlock, error) {
	// Format the block number as hex
	hexBlockNumber := fmt.Sprintf("0x%x", blockNumber)

	// Call eth_getBlockByNumber to get the block
	result, err := p.Call(ctx, chainSymbol, "eth_getBlockByNumber", []interface{}{hexBlockNumber, fullTransactions})
	if err != nil {
		return nil, fmt.Errorf("error getting block: %w", err)
	}

	// Check if result is null
	if string(result) == "null" {
		return nil, fmt.Errorf("block not found")
	}

	// Parse the block from the result
	var block EVMBlock
	if err := json.Unmarshal(result, &block); err != nil {
		return nil, fmt.Errorf("error unmarshaling block: %w", err)
	}

	return &block, nil
}

// decodeERC20String decodes an ERC20 string value from a hex string
// This is a simplified implementation and may need improvements for some tokens
func decodeERC20String(hexString string) ([]byte, error) {
	// Ensure we have a valid hex string (at least 0x + 64 chars)
	if len(hexString) < 66 {
		return nil, fmt.Errorf("invalid hex string length: %d", len(hexString))
	}

	// The first 32 bytes contain the string length and offset
	// We'll skip the parsing here and just extract the string part

	// For a simple case, try to extract the string starting from byte 64
	// This is a simplified approach and may not work for all tokens
	rawHex := hexString[2:] // Remove '0x' prefix

	// Find the start of actual data (after the offset info)
	dataStart := 64
	if len(rawHex) <= dataStart {
		return nil, fmt.Errorf("hex string too short")
	}

	// Parse the string length from the first 32 bytes
	var strLength int
	fmt.Sscanf(rawHex[:64], "%x", &strLength)

	// Ensure we have enough data
	if 2*strLength+dataStart > len(rawHex) {
		return nil, fmt.Errorf("insufficient data for string length %d", strLength)
	}

	// Extract the actual string bytes
	strHex := rawHex[dataStart : dataStart+2*strLength]

	// Convert hex string to bytes
	strBytes := make([]byte, strLength)
	for i := 0; i < strLength; i++ {
		fmt.Sscanf(strHex[i*2:i*2+2], "%x", &strBytes[i])
	}

	return strBytes, nil
}
