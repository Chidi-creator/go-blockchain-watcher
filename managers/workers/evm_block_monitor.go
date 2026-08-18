package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/events"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/src/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Chain configuration
type ChainConfig struct {
	RpcURL       string
	ChainID      int
	Name         string
	Symbol       string
	ExplorerURL  string
	HexID        string
	NativeSymbol string
}

// Map of chain configurations
var ChainConfigs = map[string]ChainConfig{
	"matic": {
		RpcURL:  "https://polygon-rpc.com",
		ChainID: 137,
		Name:    "Polygon",
		Symbol:  "MATIC",
	},
	"op": {
		RpcURL:  "https://mainnet.optimism.io",
		ChainID: 10,
		Name:    "Optimism",
		Symbol:  "OP",
	},
	"dai": {
		RpcURL:       "https://rpc.xdaichain.com",
		ChainID:      100,
		Name:         "Gnosis Chain",
		Symbol:       "DAI",
		ExplorerURL:  "",
		HexID:        "",
		NativeSymbol: "",
	},
	"bnb": {
		RpcURL:  "https://bsc-dataseed.binance.org",
		ChainID: 56,
		Name:    "BNB Chain",
		Symbol:  "BNB",
	},
	"arb": {
		RpcURL:  "https://arb1.arbitrum.io/rpc",
		ChainID: 42161,
		Name:    "Arbitrum One",
		Symbol:  "ARB",
	},
	"eth": {
		RpcURL:  "https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161",
		ChainID: 1,
		Name:    "Ethereum",
		Symbol:  "ETH",
	},
}

// EVMBlockMonitor handles EVM-compatible blockchain monitoring
type EVMBlockMonitor struct {
	cacheManager *cache.CacheManager
	eventManager *events.EventManager
	logger       logger.Logger
	clients      map[int]*ethclient.Client
	httpClient   *http.Client
}

// NewEVMBlockMonitor creates a new EVM block monitor
func NewEVMBlockMonitor(
	cacheManager *cache.CacheManager,
	eventManager *events.EventManager,
	logger logger.Logger,
) *EVMBlockMonitor {
	return &EVMBlockMonitor{
		cacheManager: cacheManager,
		eventManager: eventManager,
		logger:       logger,
		clients:      make(map[int]*ethclient.Client),
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// Initialize initializes EVM clients for all chains
func (em *EVMBlockMonitor) Initialize(ctx context.Context) error {
	for _, chain := range ChainConfigs {
		client, err := ethclient.DialContext(ctx, chain.RpcURL)
		if err != nil {
			em.logger.Error("[EVMBlockMonitor] Failed to connect to chain",
				"name", chain.Name,
				"chainId", chain.ChainID,
				"rpcUrl", chain.RpcURL,
				"error", err)
			continue
		}
		em.clients[chain.ChainID] = client
		em.logger.Info("[EVMBlockMonitor] Connected to chain",
			"name", chain.Name,
			"chainId", chain.ChainID)
	}
	return nil
}

// ProcessBlockMonitorJob processes an EVM block monitor job
func (em *EVMBlockMonitor) ProcessBlockMonitorJob(ctx context.Context, jobData map[string]interface{}) error {
	em.logger.Info("[EVMBlockMonitor] Processing job data", "data", jobData)

	// Parse job data
	var chainID int
	var chainSymbol string

	// Extract chainId from job data
	if chainIDRaw, ok := jobData["chainId"]; ok {
		switch v := chainIDRaw.(type) {
		case float64:
			chainID = int(v)
		case int:
			chainID = v
		default:
			em.logger.Error("[EVMBlockMonitor] Invalid chainId type", "type", fmt.Sprintf("%T", chainIDRaw))
			return fmt.Errorf("invalid chainId type: %T", chainIDRaw)
		}
	}

	// Extract chainSymbol from job data
	if chainSymbolRaw, ok := jobData["chainSymbol"]; ok {
		if cs, ok := chainSymbolRaw.(string); ok {
			chainSymbol = cs
		}
	}

	if chainID == 0 {
		return fmt.Errorf("no chainId found in job data")
	}

	// Find the chain config
	var targetChain ChainConfig
	var found bool
	for _, chain := range ChainConfigs {
		if chain.ChainID == chainID {
			targetChain = chain
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("unsupported chain ID: %d", chainID)
	}

	if chainSymbol == "" {
		chainSymbol = targetChain.Symbol
	}

	// Get the client for this chain
	client, exists := em.clients[chainID]
	if !exists {
		// Try to initialize the client
		var err error
		client, err = ethclient.DialContext(ctx, targetChain.RpcURL)
		if err != nil {
			return fmt.Errorf("failed to connect to chain %s: %w", targetChain.Name, err)
		}
		em.clients[chainID] = client
	}

	// Get block state from cache
	cacheKey := fmt.Sprintf("%s_%d", constants.EVMBlockIdentifier, chainID)
	cacheResp, err := em.cacheManager.Get(ctx, cacheKey)
	if err != nil {
		em.logger.Error("[EVMBlockMonitor] Error fetching block state from cache",
			"chain", targetChain.Name,
			"error", err)
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
			em.logger.Info("[EVMBlockMonitor] Unknown type for cached block height",
				"chain", targetChain.Name,
				"type", fmt.Sprintf("%T", cacheResp.Data))
		}
	}

	// Get current block height
	currentHeight, err := client.BlockNumber(ctx)
	if err != nil {
		em.logger.Error("[EVMBlockMonitor] Error fetching current block height",
			"chain", targetChain.Name,
			"error", err)
		return err
	}

	if lastProcessedBlockHeight == 0 {
		// No block state found, initialize with current block height
		em.logger.Info("[EVMBlockMonitor] No block state found, initializing",
			"chain", targetChain.Name,
			"height", currentHeight)

		err = em.cacheManager.Set(ctx, cacheKey, currentHeight, 120)
		if err != nil {
			em.logger.Error("[EVMBlockMonitor] Error saving initial block height",
				"chain", targetChain.Name,
				"error", err)
			return err
		}

		return nil
	}

	em.logger.Info("[EVMBlockMonitor] Current height vs last processed",
		"chain", targetChain.Name,
		"current", currentHeight,
		"lastProcessed", lastProcessedBlockHeight)

	// Check if there are new blocks
	if currentHeight > lastProcessedBlockHeight {
		em.logger.Info("[EVMBlockMonitor] New blocks detected, starting processing",
			"chain", targetChain.Name,
			"newBlocks", currentHeight-lastProcessedBlockHeight)

		// Process blocks between last processed and current
		for height := lastProcessedBlockHeight + 1; height <= currentHeight; height++ {
			err := em.processBlockAtHeight(ctx, client, chainID, chainSymbol, height)
			if err != nil {
				em.logger.Error("[EVMBlockMonitor] Error processing block",
					"chain", targetChain.Name,
					"height", height,
					"error", err)
				// Continue processing other blocks even if one fails
				continue
			}
		}

		// Update last processed block height
		err = em.cacheManager.Set(ctx, cacheKey, currentHeight, 120)
		if err != nil {
			em.logger.Error("[EVMBlockMonitor] Error updating last processed height",
				"chain", targetChain.Name,
				"error", err)
			return err
		}

		em.logger.Info("[EVMBlockMonitor] Updated last processed block height",
			"chain", targetChain.Name,
			"height", currentHeight)
	} else {
		em.logger.Info("[EVMBlockMonitor] No new blocks to process",
			"chain", targetChain.Name)
	}

	return nil
}

// processBlockAtHeight processes a block at the given height
func (em *EVMBlockMonitor) processBlockAtHeight(ctx context.Context, client *ethclient.Client, chainID int, chainSymbol string, height uint64) error {
	var chainName string
	for _, cfg := range ChainConfigs {
		if cfg.ChainID == chainID {
			chainName = cfg.Name
			break
		}
	}

	em.logger.Info("[EVMBlockMonitor] Processing block at height",
		"chain", chainName,
		"height", height)

	// Fetch the block with full transaction details
	block, err := client.BlockByNumber(ctx, big.NewInt(int64(height)))
	if err != nil {
		return fmt.Errorf("error fetching block: %w", err)
	}

	if block == nil {
		return fmt.Errorf("no block returned for height %d", height)
	}

	// Extract transactions
	txs := block.Transactions()
	txHashes := make([]string, len(txs))
	for i, tx := range txs {
		txHashes[i] = tx.Hash().Hex()
	}

	// Create block data for event
	blockData := types.BlockData{
		ChainID:      chainID,
		ChainSymbol:  chainSymbol,
		BlockNumber:  block.NumberU64(),
		BlockHash:    block.Hash().Hex(),
		Timestamp:    block.Time(),
		ParentHash:   block.ParentHash().Hex(),
		Transactions: txHashes,
	}

	// Emit block found event
	em.logger.Info("[EVMBlockMonitor] Emitting block found event",
		"chain", chainName,
		"height", block.NumberU64(),
		"txCount", len(txHashes))
	em.eventManager.Publish("handleBlockFound", blockData)

	return nil
}

// StartBlockMonitor starts monitoring blocks for all supported chains
func (em *EVMBlockMonitor) StartBlockMonitor(ctx context.Context, queueService interface{}) error {
	type QueueServiceWithSchedule interface {
		ScheduleJob(ctx context.Context, queueName string, data map[string]interface{}, options map[string]interface{}) (string, error)
	}

	qs, ok := queueService.(QueueServiceWithSchedule)
	if !ok {
		return fmt.Errorf("queue service does not implement required methods")
	}

	for _, chain := range ChainConfigs {
		// Create job data
		jobData := map[string]interface{}{
			"chainId":     chain.ChainID,
			"chainSymbol": chain.Symbol,
		}

		// Create job options with repeat
		options := map[string]interface{}{
			"jobId": fmt.Sprintf("evm-block-monitor-%s", strings.ToLower(chain.Symbol)),
			"repeat": map[string]interface{}{
				"every": 15000, // 15 seconds
			},
			"removeOnComplete": false,
			"removeOnFail":     false,
		}

		// Schedule the job
		jobID, err := qs.ScheduleJob(ctx, constants.QueueEVMBlockMonitor, jobData, options)
		if err != nil {
			em.logger.Error("[EVMBlockMonitor] Failed to schedule block monitor job",
				"chain", chain.Name,
				"error", err)
			continue
		}

		em.logger.Info("[EVMBlockMonitor] Block monitor job scheduled",
			"chain", chain.Name,
			"jobId", jobID)
	}

	return nil
}

// RegisterEVMBlockMonitor registers the EVM block monitor with the queue service
func RegisterEVMBlockMonitor(ctx context.Context, queueService interface{}, cacheManager *cache.CacheManager, eventManager *events.EventManager, logger logger.Logger) error {
	// Cast the queue service to the correct type that has RegisterHandler method
	type QueueServiceWithRegister interface {
		RegisterHandler(queueName string, handler func(ctx context.Context, data map[string]interface{}) error)
		ProcessJobs(ctx context.Context, queueName string, concurrency int) error
	}

	qs, ok := queueService.(QueueServiceWithRegister)
	if !ok {
		return fmt.Errorf("queue service does not implement required methods")
	}

	monitor := NewEVMBlockMonitor(cacheManager, eventManager, logger)

	// Initialize EVM clients
	if err := monitor.Initialize(ctx); err != nil {
		logger.Warn("[EVMBlockMonitor] Partial initialization of EVM clients", "error", err)
	}

	// Register the handler
	qs.RegisterHandler(constants.QueueEVMBlockMonitor, monitor.ProcessBlockMonitorJob)

	// Start processing with concurrency of 3 (three workers)
	err := qs.ProcessJobs(ctx, constants.QueueEVMBlockMonitor, 3)
	if err != nil {
		return fmt.Errorf("failed to start EVM block monitor: %w", err)
	}

	logger.Info("EVM block monitor registered and started", "queue", constants.QueueEVMBlockMonitor)
	return nil
}
