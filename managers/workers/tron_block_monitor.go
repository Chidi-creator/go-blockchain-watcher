package workers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/events"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/src/types"
)

// TronAPIURL is the default API URL for Tron
const TronAPIURL = "https://api.trongrid.io"

// TronBlock represents a Tron block
type TronBlock struct {
	BlockID      string            `json:"blockID"`
	BlockHeader  TronBlockHeader   `json:"block_header"`
	Transactions []TronTransaction `json:"transactions"`
}

// TronBlockHeader represents a Tron block header
type TronBlockHeader struct {
	RawData          TronBlockRawData `json:"raw_data"`
	WitnessSignature string           `json:"witness_signature"`
}

// TronBlockRawData represents raw data of a Tron block
type TronBlockRawData struct {
	Number         uint64 `json:"number"`
	TxTrieRoot     string `json:"txTrieRoot"`
	WitnessAddress string `json:"witness_address"`
	ParentHash     string `json:"parentHash"`
	Version        int    `json:"version"`
	Timestamp      int64  `json:"timestamp"`
}

// TronTransaction represents a Tron transaction
type TronTransaction struct {
	TxID      string              `json:"txID"`
	RawData   TronTransactionData `json:"raw_data"`
	Signature []string            `json:"signature"`
}

// TronTransactionData represents the data of a Tron transaction
type TronTransactionData struct {
	Contract      []TronContract `json:"contract"`
	RefBlockBytes string         `json:"ref_block_bytes"`
	RefBlockHash  string         `json:"ref_block_hash"`
	Expiration    int64          `json:"expiration"`
	Timestamp     int64          `json:"timestamp"`
}

// TronContract represents a contract in a Tron transaction
type TronContract struct {
	Type           string          `json:"type"`
	Parameter      json.RawMessage `json:"parameter"`
	ParameterValue struct {
		OwnerAddress    string `json:"owner_address"`
		ToAddress       string `json:"to_address,omitempty"`
		Amount          uint64 `json:"amount,omitempty"`
		AssetName       string `json:"asset_name,omitempty"`
		ContractAddress string `json:"contract_address,omitempty"`
	} `json:"-"`
}

// TronBlockMonitor handles Tron block monitoring
type TronBlockMonitor struct {
	cacheManager *cache.CacheManager
	eventManager *events.EventManager
	httpClient   *http.Client
	apiURL       string
	apiKey       string
	logger       logger.Logger
}

// NewTronBlockMonitor creates a new Tron block monitor
func NewTronBlockMonitor(
	cacheManager *cache.CacheManager,
	eventManager *events.EventManager,
	logger logger.Logger,
	apiURL string,
	apiKey string,
) *TronBlockMonitor {
	if apiURL == "" {
		apiURL = TronAPIURL
	}

	return &TronBlockMonitor{
		cacheManager: cacheManager,
		eventManager: eventManager,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		apiURL: apiURL,
		apiKey: apiKey,
		logger: logger,
	}
}

// ProcessBlockMonitorJob processes a Tron block monitor job
func (tm *TronBlockMonitor) ProcessBlockMonitorJob(ctx context.Context, jobData map[string]interface{}) error {
	tm.logger.Info("[TronBlockMonitor] Processing job data", "data", jobData)

	// Get block state from cache
	cacheResp, err := tm.cacheManager.Get(ctx, constants.TronBlockIdentifier)
	if err != nil {
		tm.logger.Error("[TronBlockMonitor] Error fetching block state from cache", "error", err)
		return err
	}

	var lastProcessedBlockHeight uint64
	if cacheResp.Data != nil {
		// Try to parse the cached height
		switch v := cacheResp.Data.(type) {
		case float64:
			lastProcessedBlockHeight = uint64(v)
		case int:
			lastProcessedBlockHeight = uint64(v)
		case uint64:
			lastProcessedBlockHeight = v
		case json.Number:
			parsed, err := v.Int64()
			if err == nil {
				lastProcessedBlockHeight = uint64(parsed)
			}
		default:
			tm.logger.Info("[TronBlockMonitor] Unknown type for cached block height", "type", fmt.Sprintf("%T", cacheResp.Data))
		}
	}

	// Get current block
	currentBlock, err := tm.getCurrentBlock(ctx)
	if err != nil {
		tm.logger.Error("[TronBlockMonitor] Error fetching current block", "error", err)
		return err
	}

	currentHeight := currentBlock.BlockHeader.RawData.Number

	if lastProcessedBlockHeight == 0 {
		// No block state found, initialize with current block height
		tm.logger.Info("[TronBlockMonitor] No block state found, initializing", "height", currentHeight)

		err = tm.cacheManager.Set(ctx, constants.TronBlockIdentifier, currentHeight, 120)
		if err != nil {
			tm.logger.Error("[TronBlockMonitor] Error saving initial block height", "error", err)
			return err
		}

		return nil
	}

	tm.logger.Info("[TronBlockMonitor] Current height vs last processed",
		"current", currentHeight,
		"lastProcessed", lastProcessedBlockHeight)

	// Check if there are new blocks
	if currentHeight > lastProcessedBlockHeight {
		tm.logger.Info("[TronBlockMonitor] New blocks detected, starting processing",
			"newBlocks", currentHeight-lastProcessedBlockHeight)

		// Process blocks between last processed and current
		for height := lastProcessedBlockHeight + 1; height <= currentHeight; height++ {
			err := tm.processBlockAtHeight(ctx, height)
			if err != nil {
				tm.logger.Error("[TronBlockMonitor] Error processing block", "height", height, "error", err)
				// Continue processing other blocks even if one fails
				continue
			}
		}

		// Update last processed block height
		err = tm.cacheManager.Set(ctx, constants.TronBlockIdentifier, currentHeight, 3600)
		if err != nil {
			tm.logger.Error("[TronBlockMonitor] Error updating last processed height", "error", err)
			return err
		}

		tm.logger.Info("[TronBlockMonitor] Updated last processed block height", "height", currentHeight)
	} else {
		tm.logger.Info("[TronBlockMonitor] No new blocks to process")
	}

	return nil
}

// getCurrentBlock fetches the current Tron block
func (tm *TronBlockMonitor) getCurrentBlock(ctx context.Context) (*TronBlock, error) {
	url := fmt.Sprintf("%s/wallet/getnowblock", tm.apiURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	if tm.apiKey != "" {
		req.Header.Set("TRON-PRO-API-KEY", tm.apiKey)
	}

	resp, err := tm.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var block TronBlock
	err = json.NewDecoder(resp.Body).Decode(&block)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &block, nil
}

// processBlockAtHeight processes a block at the given height
func (tm *TronBlockMonitor) processBlockAtHeight(ctx context.Context, height uint64) error {
	tm.logger.Info("[TronBlockMonitor] Processing block at height", "height", height)

	// Fetch block by number
	block, err := tm.getBlockByNumber(ctx, height)
	if err != nil {
		return fmt.Errorf("error fetching block: %w", err)
	}

	if block == nil {
		return fmt.Errorf("no block returned for height %d", height)
	}

	// Parse transaction details for better accessibility
	for i, tx := range block.Transactions {
		for j, contract := range tx.RawData.Contract {
			// Parse contract parameter value
			if err := json.Unmarshal(contract.Parameter, &struct {
				Value *json.RawMessage `json:"value"`
			}{
				Value: (*json.RawMessage)(&contract.Parameter),
			}); err == nil {
				json.Unmarshal(contract.Parameter, &block.Transactions[i].RawData.Contract[j].ParameterValue)
			}
		}
	}

	// Transform transactions to a format suitable for the event
	txs := make([]map[string]interface{}, len(block.Transactions))
	for i, tx := range block.Transactions {
		txData := map[string]interface{}{
			"txID":      tx.TxID,
			"timestamp": tx.RawData.Timestamp,
		}

		// Add contract data if available
		if len(tx.RawData.Contract) > 0 {
			contract := tx.RawData.Contract[0]
			txData["contractType"] = contract.Type

			if contract.ParameterValue.OwnerAddress != "" {
				txData["from"] = hexToTronAddress(contract.ParameterValue.OwnerAddress)
			}

			if contract.ParameterValue.ToAddress != "" {
				txData["to"] = hexToTronAddress(contract.ParameterValue.ToAddress)
			}

			if contract.ParameterValue.Amount > 0 {
				txData["amount"] = contract.ParameterValue.Amount
			}
		}

		txs[i] = txData
	}

	// Create block data for event
	blockData := types.BlockData{
		ChainSymbol:  "TRX",
		BlockNumber:  height,
		BlockHash:    block.BlockID,
		Timestamp:    block.BlockHeader.RawData.Timestamp,
		ParentHash:   block.BlockHeader.RawData.ParentHash,
		Transactions: txs,
	}

	// Emit block found event
	tm.logger.Info("[TronBlockMonitor] Emitting block found event",
		"height", height,
		"txCount", len(txs))
	tm.eventManager.Publish("handleBlockFound", blockData)

	return nil
}

// getBlockByNumber fetches a block by its height
func (tm *TronBlockMonitor) getBlockByNumber(ctx context.Context, height uint64) (*TronBlock, error) {
	url := fmt.Sprintf("%s/wallet/getblockbynum", tm.apiURL)

	reqBody := map[string]interface{}{
		"num": height,
	}

	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(reqJSON))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	if tm.apiKey != "" {
		req.Header.Set("TRON-PRO-API-KEY", tm.apiKey)
	}

	resp, err := tm.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var block TronBlock
	err = json.NewDecoder(resp.Body).Decode(&block)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &block, nil
}

// hexToTronAddress converts a hexadecimal address to Tron base58 format
// Note: In a real implementation, this would use proper conversion logic
func hexToTronAddress(hexAddress string) string {
	// This is a placeholder - in a real implementation, you would convert
	// from hex to Tron's base58 format with checksum
	return fmt.Sprintf("T%s", hexAddress[2:])
}

// StartBlockMonitor starts monitoring Tron blocks
func (tm *TronBlockMonitor) StartBlockMonitor(ctx context.Context, queueService interface{}) error {
	type QueueServiceWithSchedule interface {
		ScheduleJob(ctx context.Context, queueName string, data map[string]interface{}, options map[string]interface{}) (string, error)
	}

	qs, ok := queueService.(QueueServiceWithSchedule)
	if !ok {
		return fmt.Errorf("queue service does not implement required methods")
	}

	// Get current block to start monitoring from
	currentBlock, err := tm.getCurrentBlock(ctx)
	if err != nil {
		tm.logger.Error("[TronBlockMonitor] Error fetching current block", "error", err)
		return err
	}

	currentHeight := currentBlock.BlockHeader.RawData.Number

	// Set initial block height in cache
	err = tm.cacheManager.Set(ctx, constants.TronBlockIdentifier, currentHeight, 3600)
	if err != nil {
		tm.logger.Error("[TronBlockMonitor] Error saving initial block height", "error", err)
		return err
	}

	// Create job data
	jobData := map[string]interface{}{
		"lastProcessedBlockHeight": currentHeight,
	}

	// Create job options with repeat
	options := map[string]interface{}{
		"jobId": "tron-block-monitor",
		"repeat": map[string]interface{}{
			"every": 10000, // 10 seconds
		},
		"removeOnComplete": false,
		"removeOnFail":     false,
	}

	// Schedule the job
	jobID, err := qs.ScheduleJob(ctx, constants.QueueTronBlockMonitor, jobData, options)
	if err != nil {
		tm.logger.Error("[TronBlockMonitor] Failed to schedule block monitor job", "error", err)
		return err
	}

	tm.logger.Info("[TronBlockMonitor] Block monitor job scheduled",
		"jobId", jobID,
		"startingHeight", currentHeight)
	return nil
}

// RegisterTronBlockMonitor registers the Tron block monitor with the queue service
func RegisterTronBlockMonitor(ctx context.Context, queueService interface{}, cacheManager *cache.CacheManager, eventManager *events.EventManager, logger logger.Logger, apiURL, apiKey string) error {
	// Cast the queue service to the correct type that has RegisterHandler method
	type QueueServiceWithRegister interface {
		RegisterHandler(queueName string, handler func(ctx context.Context, data map[string]interface{}) error)
		ProcessJobs(ctx context.Context, queueName string, concurrency int) error
	}

	qs, ok := queueService.(QueueServiceWithRegister)
	if !ok {
		return fmt.Errorf("queue service does not implement required methods")
	}

	monitor := NewTronBlockMonitor(cacheManager, eventManager, logger, apiURL, apiKey)

	// Register the handler
	qs.RegisterHandler(constants.QueueTronBlockMonitor, monitor.ProcessBlockMonitorJob)

	// Start processing with concurrency of 1 (one worker)
	err := qs.ProcessJobs(ctx, constants.QueueTronBlockMonitor, 1)
	if err != nil {
		return fmt.Errorf("failed to start Tron block monitor: %w", err)
	}

	logger.Info("Tron block monitor registered and started", "queue", constants.QueueTronBlockMonitor)
	return nil
}
