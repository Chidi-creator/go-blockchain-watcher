package evm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

// AnkrClient handles API requests to the Ankr API
type AnkrClient struct {
	apiURL     string
	apiKey     string
	httpClient *http.Client
	logger     logger.Logger
}

// NewAnkrClient creates a new Ankr API client
func NewAnkrClient(apiURL, apiKey string, httpClient *http.Client, logger logger.Logger) *AnkrClient {
	return &AnkrClient{
		apiURL:     apiURL,
		apiKey:     apiKey,
		httpClient: httpClient,
		logger:     logger,
	}
}

// GetBlockByNumber retrieves a block by its number
func (c *AnkrClient) GetBlockByNumber(ctx context.Context, blockNumber *big.Int, fullTransactions bool) (*AnkrBlock, error) {
	blockNumberHex := fmt.Sprintf("0x%x", blockNumber)
	c.logger.Debug("Getting block by number", "blockNumber", blockNumberHex)

	// Create JSON-RPC request
	req := AnkrRPCRequest{
		JsonRPC: "2.0",
		Method:  "eth_getBlockByNumber",
		Params:  []interface{}{blockNumberHex, fullTransactions},
		ID:      1,
	}

	// Make request
	var result AnkrBlock
	err := c.doRequest(ctx, req, &result)
	if err != nil {
		return nil, fmt.Errorf("error getting block by number: %w", err)
	}

	return &result, nil
}

// GetLatestBlockNumber retrieves the latest block number
func (c *AnkrClient) GetLatestBlockNumber(ctx context.Context) (*big.Int, error) {
	c.logger.Debug("Getting latest block number")

	// Create JSON-RPC request
	req := AnkrRPCRequest{
		JsonRPC: "2.0",
		Method:  "eth_blockNumber",
		Params:  []interface{}{},
		ID:      1,
	}

	// Make request
	var result string
	err := c.doRequest(ctx, req, &result)
	if err != nil {
		return nil, fmt.Errorf("error getting latest block number: %w", err)
	}

	// Parse the hex result
	blockNumber, success := new(big.Int).SetString(result[2:], 16)
	if !success {
		return nil, fmt.Errorf("failed to parse block number: %s", result)
	}

	return blockNumber, nil
}

// GetBalance retrieves the balance for an address
func (c *AnkrClient) GetBalance(ctx context.Context, address string, blockNumber *big.Int) (*big.Int, error) {
	blockNumberHex := "latest"
	if blockNumber != nil {
		blockNumberHex = fmt.Sprintf("0x%x", blockNumber)
	}

	c.logger.Debug("Getting balance", "address", address, "blockNumber", blockNumberHex)

	// Create JSON-RPC request
	req := AnkrRPCRequest{
		JsonRPC: "2.0",
		Method:  "eth_getBalance",
		Params:  []interface{}{address, blockNumberHex},
		ID:      1,
	}

	// Make request
	var result string
	err := c.doRequest(ctx, req, &result)
	if err != nil {
		return nil, fmt.Errorf("error getting balance: %w", err)
	}

	// Parse the hex result
	balance, success := new(big.Int).SetString(result[2:], 16)
	if !success {
		return nil, fmt.Errorf("failed to parse balance: %s", result)
	}

	return balance, nil
}

// GetTransactionByHash retrieves a transaction by its hash
func (c *AnkrClient) GetTransactionByHash(ctx context.Context, txHash string) (*AnkrTransaction, error) {
	c.logger.Debug("Getting transaction by hash", "txHash", txHash)

	// Create JSON-RPC request
	req := AnkrRPCRequest{
		JsonRPC: "2.0",
		Method:  "eth_getTransactionByHash",
		Params:  []interface{}{txHash},
		ID:      1,
	}

	// Make request
	var result AnkrTransaction
	err := c.doRequest(ctx, req, &result)
	if err != nil {
		return nil, fmt.Errorf("error getting transaction by hash: %w", err)
	}

	return &result, nil
}

// GetPastLogs retrieves logs matching the given filter
func (c *AnkrClient) GetPastLogs(ctx context.Context, fromBlock, toBlock *big.Int, addresses []string, topics [][]string) ([]interface{}, error) {
	fromBlockHex := "latest"
	if fromBlock != nil {
		fromBlockHex = fmt.Sprintf("0x%x", fromBlock)
	}

	toBlockHex := "latest"
	if toBlock != nil {
		toBlockHex = fmt.Sprintf("0x%x", toBlock)
	}

	c.logger.Debug("Getting past logs",
		"fromBlock", fromBlockHex,
		"toBlock", toBlockHex,
		"addresses", addresses)

	// Create filter object
	filter := map[string]interface{}{
		"fromBlock": fromBlockHex,
		"toBlock":   toBlockHex,
	}

	if len(addresses) > 0 {
		filter["address"] = addresses
	}

	if len(topics) > 0 {
		filter["topics"] = topics
	}

	// Create JSON-RPC request
	req := AnkrRPCRequest{
		JsonRPC: "2.0",
		Method:  "eth_getLogs",
		Params:  []interface{}{filter},
		ID:      1,
	}

	// Make request
	var result []interface{}
	err := c.doRequest(ctx, req, &result)
	if err != nil {
		return nil, fmt.Errorf("error getting past logs: %w", err)
	}

	return result, nil
}

// Generic request handler
func (c *AnkrClient) doRequest(ctx context.Context, requestObj AnkrRPCRequest, result interface{}) error {
	// Marshal request to JSON
	reqBody, err := json.Marshal(requestObj)
	if err != nil {
		return fmt.Errorf("error marshaling request: %w", err)
	}

	// Create HTTP request
	url := c.apiURL
	// Only append API key if it's not already in the URL and we have one
	if c.apiKey != "" && !strings.Contains(url, c.apiKey) {
		// For Ankr API, key should be added as a path segment for some endpoints
		if strings.Contains(url, "ankr.com") {
			if !strings.HasSuffix(url, "/") {
				url += "/"
			}
			url += c.apiKey
		}
	}

	// ll
	c.logger.Debug("Making API request to URL", "url", maskAPIKey(url, c.apiKey))

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Execute request with retry
	var resp *http.Response
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		resp, err = c.httpClient.Do(req)
		if err == nil {
			break
		}

		if i < maxRetries-1 {
			c.logger.Warn("Retrying Ankr API request", "error", err, "attempt", i+1)
			time.Sleep(time.Duration(i+1) * 500 * time.Millisecond) // Exponential backoff
		}
	}

	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	// Check for HTTP errors
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response body: %w", err)
	}

	// Parse response
	var rpcResp AnkrRPCResponse
	if err := json.Unmarshal(bodyBytes, &rpcResp); err != nil {
		return fmt.Errorf("error unmarshaling response: %w", err)
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		return fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	// Parse result into the provided result object
	if err := json.Unmarshal(rpcResp.Result, result); err != nil {
		return fmt.Errorf("error unmarshaling result: %w", err)
	}

	return nil
}

// maskAPIKey masks the API key in URLs for logging
func maskAPIKey(url, apiKey string) string {
	if apiKey == "" {
		return url
	}
	return strings.Replace(url, apiKey, "***", -1)
}

// HexToUint64 converts a hex string to uint64
func HexToUint64(hex string) (uint64, error) {
	if len(hex) < 3 || hex[:2] != "0x" {
		return 0, fmt.Errorf("invalid hex string: %s", hex)
	}

	return strconv.ParseUint(hex[2:], 16, 64)
}

// HexToBigInt converts a hex string to big.Int
func HexToBigInt(hex string) (*big.Int, error) {
	if len(hex) < 3 || hex[:2] != "0x" {
		return nil, fmt.Errorf("invalid hex string: %s", hex)
	}

	n := new(big.Int)
	n, ok := n.SetString(hex[2:], 16)
	if !ok {
		return nil, fmt.Errorf("failed to parse hex string: %s", hex)
	}

	return n, nil
}
