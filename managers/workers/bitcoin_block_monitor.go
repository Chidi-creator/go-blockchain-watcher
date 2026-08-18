package workers

import (
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

// BitcoinExplorerAPI is the API URL for Bitcoin blockchain data
const BitcoinExplorerAPI = "https://blockchain.info"

// BitcoinBlockResponse represents a response from the Bitcoin block API
type BitcoinBlockResponse struct {
	Hash      string `json:"hash"`
	Height    uint64 `json:"height"`
	Time      int64  `json:"time"`
	MainChain bool   `json:"main_chain"`
}

// BitcoinLatestBlockResponse represents a response from the Bitcoin latest block API
type BitcoinLatestBlockResponse struct {
	Hash   string `json:"hash"`
	Height uint64 `json:"height"`
	Time   int64  `json:"time"`
}

// BitcoinBlocksResponse represents a response containing multiple blocks
type BitcoinBlocksResponse struct {
	Blocks []BitcoinBlockResponse `json:"blocks"`
}

// BitcoinRawBlockResponse represents a response with full block details
type BitcoinRawBlockResponse struct {
	Hash         string              `json:"hash"`
	Height       uint64              `json:"height"`
	Time         int64               `json:"time"`
	Transactions []BitcoinTxResponse `json:"tx"`
}

// BitcoinTxResponse represents a Bitcoin transaction
type BitcoinTxResponse struct {
	Hash string `json:"hash"`
	// Add other fields as needed
}

// BitcoinBlockMonitor handles Bitcoin block monitoring
type BitcoinBlockMonitor struct {
	cacheManager *cache.CacheManager
	eventManager *events.EventManager
	httpClient   *http.Client
	logger       logger.Logger
}

// NewBitcoinBlockMonitor creates a new Bitcoin block monitor
func NewBitcoinBlockMonitor(
	cacheManager *cache.CacheManager,
	eventManager *events.EventManager,
	logger logger.Logger,
) *BitcoinBlockMonitor {
	return &BitcoinBlockMonitor{
		cacheManager: cacheManager,
		eventManager: eventManager,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: logger,
	}
}

// ProcessBlockMonitorJob processes a Bitcoin block monitor job
func (bm *BitcoinBlockMonitor) ProcessBlockMonitorJob(ctx context.Context, jobData map[string]interface{}) error {
	bm.logger.Info("[BitcoinBlockMonitor] Processing job data", "data", jobData)

	// Get block state from cache
	cacheResp, err := bm.cacheManager.Get(ctx, constants.BitcoinBlockIdentifier)
	if err != nil {
		bm.logger.Error("[BitcoinBlockMonitor] Error fetching block state from cache", "error", err)
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
			bm.logger.Info("[BitcoinBlockMonitor] Unknown type for cached block height", "type", fmt.Sprintf("%T", cacheResp.Data))
		}
	}

	if lastProcessedBlockHeight == 0 {
		// No block state found, initialize with current block height
		bm.logger.Info("[BitcoinBlockMonitor] No block state found, fetching initial height")
		latestBlock, err := bm.fetchLatestBlock(ctx)
		if err != nil {
			bm.logger.Error("[BitcoinBlockMonitor] Error fetching latest block", "error", err)
			return err
		}

		currentHeight := latestBlock.Height
		err = bm.cacheManager.Set(ctx, constants.BitcoinBlockIdentifier, currentHeight, 120)
		if err != nil {
			bm.logger.Error("[BitcoinBlockMonitor] Error saving initial block height", "error", err)
			return err
		}

		bm.logger.Info("[BitcoinBlockMonitor] Block state initialized", "height", currentHeight)
		return nil
	}

	// Get current block height
	latestBlock, err := bm.fetchLatestBlock(ctx)
	if err != nil {
		bm.logger.Error("[BitcoinBlockMonitor] Error fetching latest block", "error", err)
		return err
	}

	currentHeight := latestBlock.Height
	bm.logger.Info("[BitcoinBlockMonitor] Current height vs last processed",
		"current", currentHeight,
		"lastProcessed", lastProcessedBlockHeight)

	// Check if there are new blocks
	if currentHeight > lastProcessedBlockHeight {
		bm.logger.Info("[BitcoinBlockMonitor] New blocks detected, starting processing")

		// Process blocks between last processed and current
		for height := lastProcessedBlockHeight + 1; height <= currentHeight; height++ {
			err := bm.processBlockAtHeight(ctx, height)
			if err != nil {
				bm.logger.Error("[BitcoinBlockMonitor] Error processing block", "height", height, "error", err)
				// Continue processing other blocks even if one fails
				continue
			}
		}

		// Update last processed block height
		err = bm.cacheManager.Set(ctx, constants.BitcoinBlockIdentifier, currentHeight, 120)
		if err != nil {
			bm.logger.Error("[BitcoinBlockMonitor] Error updating last processed height", "error", err)
			return err
		}

		bm.logger.Info("[BitcoinBlockMonitor] Updated last processed block height", "height", currentHeight)
	} else {
		bm.logger.Info("[BitcoinBlockMonitor] No new blocks to process")
	}

	return nil
}

// fetchLatestBlock fetches the latest Bitcoin block
func (bm *BitcoinBlockMonitor) fetchLatestBlock(ctx context.Context) (*BitcoinLatestBlockResponse, error) {
	url := fmt.Sprintf("%s/latestblock", BitcoinExplorerAPI)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	resp, err := bm.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var latestBlock BitcoinLatestBlockResponse
	err = json.NewDecoder(resp.Body).Decode(&latestBlock)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &latestBlock, nil
}

// processBlockAtHeight processes a block at the given height
func (bm *BitcoinBlockMonitor) processBlockAtHeight(ctx context.Context, height uint64) error {
	bm.logger.Info("[BitcoinBlockMonitor] Processing block at height", "height", height)

	// Fetch block info
	url := fmt.Sprintf("%s/block-height/%d?format=json", BitcoinExplorerAPI, height)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	resp, err := bm.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var blocksResp BitcoinBlocksResponse
	err = json.NewDecoder(resp.Body).Decode(&blocksResp)
	if err != nil {
		return fmt.Errorf("error decoding response: %w", err)
	}

	// Find the main chain block
	var mainBlock *BitcoinBlockResponse
	for i, block := range blocksResp.Blocks {
		if block.MainChain {
			mainBlock = &blocksResp.Blocks[i]
			break
		}
	}

	if mainBlock == nil {
		return fmt.Errorf("no main chain block found for height %d", height)
	}

	// Fetch full block details with transactions
	rawBlock, err := bm.fetchBlockDetails(ctx, mainBlock.Hash)
	if err != nil {
		return fmt.Errorf("error fetching block details: %w", err)
	}

	// Extract transaction IDs
	txIds := make([]string, len(rawBlock.Transactions))
	for i, tx := range rawBlock.Transactions {
		txIds[i] = tx.Hash
	}

	// Create block data for event
	blockData := types.BlockData{
		ChainSymbol:  "BTC",
		BlockNumber:  rawBlock.Height,
		BlockHash:    rawBlock.Hash,
		Timestamp:    rawBlock.Time,
		Transactions: txIds,
	}

	// Emit block found event
	bm.logger.Info("[BitcoinBlockMonitor] Emitting block found event", "height", rawBlock.Height, "txCount", len(txIds))
	bm.eventManager.Publish("handleBlockFound", blockData)

	return nil
}

// fetchBlockDetails fetches detailed information about a block
func (bm *BitcoinBlockMonitor) fetchBlockDetails(ctx context.Context, blockHash string) (*BitcoinRawBlockResponse, error) {
	url := fmt.Sprintf("%s/rawblock/%s", BitcoinExplorerAPI, blockHash)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	resp, err := bm.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var rawBlock BitcoinRawBlockResponse
	err = json.NewDecoder(resp.Body).Decode(&rawBlock)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &rawBlock, nil
}

// RegisterBitcoinBlockMonitor registers the Bitcoin block monitor with the queue service
func RegisterBitcoinBlockMonitor(ctx context.Context, queueService interface{}, cacheManager *cache.CacheManager, eventManager *events.EventManager, logger logger.Logger) error {
	// Cast the queue service to the correct type that has RegisterHandler method
	type QueueServiceWithRegister interface {
		RegisterHandler(queueName string, handler func(ctx context.Context, data map[string]interface{}) error)
		ProcessJobs(ctx context.Context, queueName string, concurrency int) error
	}

	qs, ok := queueService.(QueueServiceWithRegister)
	if !ok {
		return fmt.Errorf("queue service does not implement required methods")
	}

	monitor := NewBitcoinBlockMonitor(cacheManager, eventManager, logger)

	// Register the handler
	qs.RegisterHandler(constants.QueueBitcoinBlockMonitor, monitor.ProcessBlockMonitorJob)

	// Start processing with concurrency of 1 (one worker)
	err := qs.ProcessJobs(ctx, constants.QueueBitcoinBlockMonitor, 1)
	if err != nil {
		return fmt.Errorf("failed to start Bitcoin block monitor: %w", err)
	}

	logger.Info("Bitcoin block monitor registered and started", "queue", constants.QueueBitcoinBlockMonitor)
	return nil
}
