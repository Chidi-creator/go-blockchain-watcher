package evm

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// 🚨 CRITICAL REMINDER: THIS WORKER MUST BE RUN FROM cmd/workers/evmChangeNow/
// 🚨 NEVER TRY TO RUN FROM ROOT main.go - USE: cd cmd/workers/evmChangeNow && go run main.go
// 🚨 THE USER HAS TOLD ME THIS MULTIPLE TIMES - DO NOT FORGET AGAIN!
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	requestManager "bitbucket.org/zapspace/zap-go-server/managers/http"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/managers/queue"
	"bitbucket.org/zapspace/zap-go-server/src/usecases"
)

// Define local constants for event types
var EventTypes = struct {
	BalanceUpdate string
	TxStatus      string
}{
	BalanceUpdate: "balance_update",
	TxStatus:      "tx_status",
}

type ChainConfig struct {
	RpcURL       string
	ChainID      int
	Name         string
	Symbol       string
	ExplorerURL  string
	NativeSymbol string
	Decimals     int
}

// ChainProcessor handles processing for a specific chain
type ChainProcessor struct {
	ctx            context.Context
	chainSymbol    string
	queueManager   *queue.QueueManager
	mongoClient    *mongo.Client
	redisClient    *redis.Client
	httpClient     *http.Client
	ankrAPIKey     string
	concurrency    int
	cfg            *config.Config
	logger         logger.Logger
	accountUseCase *usecases.AccountUseCase
	orderUseCase   *usecases.OrderUseCase
	client         *AnkrClient
	cacheManager   *cache.CacheManager
	chainConfig    ChainConfig
	requestManager *requestManager.RequestManager
}

// NewChainProcessor creates a new chain processor
func NewChainProcessor(
	ctx context.Context,
	chainSymbol string,
	queueManager *queue.QueueManager,
	mongoClient *mongo.Client,
	redisClient *redis.Client,
	httpClient *http.Client,
	ankrAPIKey string,
	concurrency int,
	cfg *config.Config,
	logger logger.Logger,
	accountUseCase *usecases.AccountUseCase,
	orderUseCase *usecases.OrderUseCase,
	ankrClient *AnkrClient,
	cacheManager *cache.CacheManager,
	chainConfig ChainConfig,
) *ChainProcessor {
	return &ChainProcessor{
		ctx:            ctx,
		chainSymbol:    chainSymbol,
		queueManager:   queueManager,
		mongoClient:    mongoClient,
		redisClient:    redisClient,
		httpClient:     httpClient,
		ankrAPIKey:     ankrAPIKey,
		concurrency:    concurrency,
		cfg:            cfg,
		logger:         logger,
		accountUseCase: accountUseCase,
		orderUseCase:   orderUseCase,
		client:         ankrClient,
		cacheManager:   cacheManager,
		chainConfig:    chainConfig,
		requestManager: requestManager.NewRequestManager(logger),
	}
}

// Start begins processing for this chain
func (p *ChainProcessor) Start() {
	p.logger.Info("Starting processor for chain", "chain", p.chainSymbol)

	// Get queue name for this chain
	queueName := fmt.Sprintf("queue_%s_watcher", p.chainSymbol)
	p.logger.Info("Using queue for chain watcher", "queueName", queueName)

	// Register handler for processing chain events
	p.queueManager.RegisterHandler(queueName, func(ctx context.Context, data map[string]interface{}) error {
		p.logger.Debug("Processing chain event", "chain", p.chainSymbol, "data", data)
		return p.ProcessChain(ctx, p.accountUseCase, p.orderUseCase, p.requestManager)
	})

	// Start processing jobs
	p.logger.Info("Starting to process chain watcher jobs", "chain", p.chainSymbol, "concurrency", p.concurrency)
	err := p.queueManager.ProcessJobs(p.ctx, queueName, p.concurrency)
	if err != nil {
		p.logger.Error("Failed to start processing jobs", "chain", p.chainSymbol, "error", err)
	}

	// Wait for context to be done
	<-p.ctx.Done()
	p.logger.Info("Processor for chain shutting down", "chain", p.chainSymbol)
}

func (p *ChainProcessor) ProcessChain(ctx context.Context, accountUsecase *usecases.AccountUseCase, orderUsecase *usecases.OrderUseCase, requestManager *requestManager.RequestManager) error {
	// 0. Ensure critical performance indexes exist
	p.logger.Info("Ensuring critical database indexes exist for optimal performance", "chain", p.chainSymbol)
	if err := p.EnsureOptimalIndexes(ctx); err != nil {
		p.logger.Warn("Failed to ensure optimal indexes", "error", err, "chain", p.chainSymbol)
	}

	// 1. Get the latest block number
	latestBlockNumber, err := p.client.GetLatestBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("error getting latest block number: %w", err)
	}

	p.logger.Info("Got latest block number",
		"chain", p.chainSymbol,
		"blockNumber", latestBlockNumber.String())

	// 2. Get the last processed block number from cache
	cacheKey := fmt.Sprintf("evm:%s:last-block-height", strings.ToLower(p.chainSymbol))
	cacheStart := time.Now()
	cacheResp, err := p.cacheManager.Get(ctx, cacheKey)
	p.logger.Debug("Cache lookup completed", "chain", p.chainSymbol, "cacheTime", time.Since(cacheStart).String(), "cacheKey", cacheKey)
	if err != nil {
		return fmt.Errorf("error getting last processed block from cache: %w", err)
	}

	var lastProcessedBlockNumber *big.Int
	if cacheResp.Data != nil {
		p.logger.Debug("Found cached data", "chain", p.chainSymbol, "cacheData", cacheResp.Data, "type", fmt.Sprintf("%T", cacheResp.Data))
		// Try to parse the cached height
		switch v := cacheResp.Data.(type) {
		case float64:
			lastProcessedBlockNumber = big.NewInt(int64(v))
		case int:
			lastProcessedBlockNumber = big.NewInt(int64(v))
		case int64:
			lastProcessedBlockNumber = big.NewInt(v)
		case string:
			var ok bool
			lastProcessedBlockNumber, ok = new(big.Int).SetString(v, 10)
			if !ok {
				p.logger.Warn("Failed to parse cached block string", "chain", p.chainSymbol, "value", v)
				lastProcessedBlockNumber = big.NewInt(0)
			}
		default:
			p.logger.Warn("Unknown type for cached block height",
				"type", fmt.Sprintf("%T", cacheResp.Data),
				"value", cacheResp.Data)
			lastProcessedBlockNumber = big.NewInt(0)
		}
	} else {
		p.logger.Debug("No cached data found", "chain", p.chainSymbol, "cacheKey", cacheKey)
		lastProcessedBlockNumber = big.NewInt(0)
	}

	p.logger.Info("Last processed block",
		"chain", p.chainSymbol,
		"blockNumber", lastProcessedBlockNumber.String())

	// 3. If no last processed block, start with latest block (fresh start behavior)
	if lastProcessedBlockNumber.Cmp(big.NewInt(0)) == 0 {
		p.logger.Info("No last processed block found, starting fresh with current block",
			"chain", p.chainSymbol,
			"blockNumber", latestBlockNumber.String(),
			"note", "Fresh start - processing current block only")

		// Process the latest block
		if err := p.processBlock(ctx, latestBlockNumber, accountUsecase, orderUsecase, requestManager); err != nil {
			return fmt.Errorf("error processing block: %w", err)
		}

		// Update the last processed block
		err = p.cacheManager.Set(ctx, cacheKey, latestBlockNumber.String(), 3600) // 1 hour TTL
		if err != nil {
			return fmt.Errorf("error setting last processed block in cache: %w", err)
		}
		p.logger.Debug("Updated cache with block number", "chain", p.chainSymbol, "blockNumber", latestBlockNumber.String(), "cacheKey", cacheKey)

		p.logger.Info("Fresh chain start completed - will process new blocks going forward",
			"chain", p.chainSymbol,
			"startingFromBlock", latestBlockNumber.String())
		return nil
	}

	// 4. If there are new blocks, process them
	if latestBlockNumber.Cmp(lastProcessedBlockNumber) > 0 {
		p.logger.Info("New blocks detected, processing",
			"chain", p.chainSymbol,
			"fromBlock", lastProcessedBlockNumber.Int64()+1,
			"toBlock", latestBlockNumber.Int64())

		// Process each block
		for blockNumber := new(big.Int).Add(lastProcessedBlockNumber, big.NewInt(1)); blockNumber.Cmp(latestBlockNumber) <= 0; blockNumber = new(big.Int).Add(blockNumber, big.NewInt(1)) {

			if err := p.processBlock(ctx, blockNumber, accountUsecase, orderUsecase, requestManager); err != nil {
				p.logger.Error("Error processing block",
					"chain", p.chainSymbol,
					"blockNumber", blockNumber.String(),
					"error", err)

				// Continue to next block even if one fails
				continue
			}
		}

		// Update the last processed block
		err = p.cacheManager.Set(ctx, cacheKey, latestBlockNumber.String(), 60) // 1 hour TTL
		if err != nil {
			return fmt.Errorf("error setting last processed block in cache: %w", err)
		}
		p.logger.Debug("Updated cache after processing blocks", "chain", p.chainSymbol, "blockNumber", latestBlockNumber.String(), "cacheKey", cacheKey)
	} else {
		p.logger.Info("No new blocks to process", "chain", p.chainSymbol)
	}

	return nil
}

// processBlock processes a single block
func (p *ChainProcessor) processBlock(ctx context.Context, blockNumber *big.Int, accountUsecase *usecases.AccountUseCase, orderUsecase *usecases.OrderUseCase, requestManager *requestManager.RequestManager) error {
	blockStart := time.Now()
	p.logger.Info("Processing block for transactions",
		"chain", p.chainSymbol,
		"blockNumber", blockNumber.String())

	// 1. Get block with full transaction details
	p.logger.Debug("STEP 1: Starting GetBlockByNumber call", "chain", p.chainSymbol, "blockNumber", blockNumber.String())
	apiStart := time.Now()
	block, err := p.client.GetBlockByNumber(ctx, blockNumber, true)
	apiTime := time.Since(apiStart)
	if err != nil {
		p.logger.Error("STEP 1: GetBlockByNumber failed", "chain", p.chainSymbol, "error", err, "apiTime", apiTime.String())
		return fmt.Errorf("error getting block by number: %w", err)
	}

	if block == nil {
		p.logger.Warn("STEP 1: GetBlockByNumber returned nil block", "chain", p.chainSymbol, "blockNumber", blockNumber.String(), "apiTime", apiTime.String())
		return nil
	}

	p.logger.Info("STEP 1: GetBlockByNumber completed",
		"chain", p.chainSymbol,
		"blockNumber", blockNumber.String(),
		"apiTime", apiTime.String(),
		"txCount", len(block.Transactions))

	if len(block.Transactions) == 0 {
		p.logger.Debug("STEP 1: No transactions in block",
			"chain", p.chainSymbol,
			"blockNumber", blockNumber.String(),
			"processingTime", time.Since(blockStart).String())
		return nil
	}

	// 2. Get token transfer logs for the same block
	p.logger.Debug("STEP 2: Starting GetPastLogs call", "chain", p.chainSymbol, "blockNumber", blockNumber.String())
	logsStart := time.Now()
	tokenLogs, err := p.getTokenTransferLogs(ctx, blockNumber, blockNumber)
	logsTime := time.Since(logsStart)
	if err != nil {
		p.logger.Warn("STEP 2: Failed to get token transfer logs", "error", err, "blockNumber", blockNumber.String(), "logsTime", logsTime.String())
		// Continue processing native transactions even if token logs fail
	} else {
		p.logger.Info("STEP 2: GetPastLogs completed",
			"chain", p.chainSymbol,
			"blockNumber", blockNumber.String(),
			"logsTime", logsTime.String(),
			"logCount", len(tokenLogs))
	}

	// 3. BATCH OPTIMIZATION: Extract ALL addresses first
	p.logger.Debug("STEP 3: Extracting all addresses for batch lookup", "chain", p.chainSymbol, "blockNumber", blockNumber.String())
	extractStart := time.Now()

	// Collect all unique addresses to check
	addressSet := make(map[string]bool)
	nativeTransactions := make([]*AnkrTransaction, 0, len(block.Transactions))
	tokenTransfers := make([]*TokenTransfer, 0, len(tokenLogs))

	// Extract native transaction "to" addresses
	for _, txInterface := range block.Transactions {
		txData, ok := txInterface.(map[string]interface{})
		if !ok {
			continue
		}

		tx, err := p.convertToAnkrTransaction(txData)
		if err != nil {
			p.logger.Warn("Failed to convert transaction", "error", err)
			continue
		}

		if tx.To != "" {
			addressSet[strings.ToLower(tx.To)] = true
			nativeTransactions = append(nativeTransactions, tx)
		}
	}

	// Extract token transfer "to" addresses
	for _, logData := range tokenLogs {
		tokenTransfer, err := p.parseTokenTransferLog(logData)
		if err != nil {
			p.logger.Warn("Failed to parse token transfer log", "error", err)
			continue
		}

		if tokenTransfer.To != "" {
			addressSet[strings.ToLower(tokenTransfer.To)] = true
			tokenTransfers = append(tokenTransfers, tokenTransfer)
		}
	}

	// Convert to slice for batch query
	addresses := make([]string, 0, len(addressSet))
	for addr := range addressSet {
		addresses = append(addresses, addr)
	}

	extractTime := time.Since(extractStart)
	p.logger.Info("STEP 3: Address extraction completed",
		"chain", p.chainSymbol,
		"blockNumber", blockNumber.String(),
		"extractTime", extractTime.String(),
		"nativeTransactions", len(nativeTransactions),
		"tokenTransfers", len(tokenTransfers),
		"uniqueAddresses", len(addresses))

	// 4. BATCH QUERY: Get all deposit accounts in ONE query
	p.logger.Debug("STEP 4: Starting batch deposit account lookup", "chain", p.chainSymbol, "addressCount", len(addresses))
	batchStart := time.Now()

	depositAccounts, err := p.findDepositAccountsByAddresses(ctx, addresses)
	batchTime := time.Since(batchStart)

	if err != nil {
		p.logger.Error("STEP 4: Batch deposit account lookup failed", "error", err, "batchTime", batchTime.String())
		return fmt.Errorf("failed to batch lookup deposit accounts: %w", err)
	}

	p.logger.Info("STEP 4: Batch deposit account lookup completed",
		"chain", p.chainSymbol,
		"blockNumber", blockNumber.String(),
		"batchTime", batchTime.String(),
		"queriedAddresses", len(addresses),
		"foundAccounts", len(depositAccounts))

	// Create address to account mapping for O(1) lookups
	accountMap := make(map[string]bson.M)
	for _, account := range depositAccounts {
		if walletAddr, ok := account["walletAddress"].(string); ok {
			accountMap[strings.ToLower(walletAddr)] = account
		}
	}

	// 5. Process native transactions (only check matches)
	p.logger.Debug("STEP 5: Processing native transactions with batch results", "chain", p.chainSymbol)
	nativeTxStart := time.Now()
	nativeMatches := 0

	for _, tx := range nativeTransactions {
		if account, exists := accountMap[strings.ToLower(tx.To)]; exists {
			if p.processMatchedTransaction(ctx, tx, account, "native", orderUsecase, requestManager) {
				nativeMatches++
			}
		}
	}

	nativeTxTime := time.Since(nativeTxStart)
	p.logger.Info("STEP 5: Native transaction processing completed",
		"chain", p.chainSymbol,
		"blockNumber", blockNumber.String(),
		"nativeTxTime", nativeTxTime.String(),
		"processedTx", len(nativeTransactions),
		"nativeMatches", nativeMatches)

	// 6. Process token transfers (only check matches)
	p.logger.Debug("STEP 6: Processing token transfers with batch results", "chain", p.chainSymbol)
	tokenTxStart := time.Now()
	tokenMatches := 0

	for _, transfer := range tokenTransfers {
		if account, exists := accountMap[strings.ToLower(transfer.To)]; exists {
			if p.processMatchedTokenTransfer(ctx, transfer, account, orderUsecase, requestManager) {
				tokenMatches++
			}
		}
	}

	tokenTxTime := time.Since(tokenTxStart)
	p.logger.Info("STEP 6: Token transfer processing completed",
		"chain", p.chainSymbol,
		"blockNumber", blockNumber.String(),
		"tokenTxTime", tokenTxTime.String(),
		"processedTransfers", len(tokenTransfers),
		"tokenMatches", tokenMatches)

	totalMatches := nativeMatches + tokenMatches
	processingTime := time.Since(blockStart)
	if totalMatches > 0 {
		p.logger.Info("Processed block with matches",
			"chain", p.chainSymbol,
			"blockNumber", blockNumber.String(),
			"nativeMatches", nativeMatches,
			"tokenMatches", tokenMatches,
			"totalMatches", totalMatches,
			"processingTime", processingTime.String())
	} else {
		p.logger.Debug("No matching transactions found in block",
			"chain", p.chainSymbol,
			"blockNumber", blockNumber.String(),
			"processingTime", processingTime.String())
	}

	return nil
}

// getTokenTransferLogs gets ERC-20 transfer logs for a block
func (p *ChainProcessor) getTokenTransferLogs(ctx context.Context, fromBlock, toBlock *big.Int) ([]interface{}, error) {
	// ERC-20 Transfer event signature: Transfer(address,address,uint256)
	transferTopicHash := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

	topics := [][]string{
		{transferTopicHash}, // First topic is the event signature
	}

	logs, err := p.client.GetPastLogs(ctx, fromBlock, toBlock, nil, topics)
	if err != nil {
		return nil, fmt.Errorf("failed to get token transfer logs: %w", err)
	}

	return logs, nil
}

// convertToAnkrTransaction converts map to AnkrTransaction struct
func (p *ChainProcessor) convertToAnkrTransaction(txData map[string]interface{}) (*AnkrTransaction, error) {
	tx := &AnkrTransaction{}

	if hash, ok := txData["hash"].(string); ok {
		tx.Hash = hash
	}
	if to, ok := txData["to"].(string); ok {
		tx.To = to
	}
	if from, ok := txData["from"].(string); ok {
		tx.From = from
	}
	if value, ok := txData["value"].(string); ok {
		tx.Value = value
	}
	if blockNumber, ok := txData["blockNumber"].(string); ok {
		tx.BlockNumber = blockNumber
	}
	if blockHash, ok := txData["blockHash"].(string); ok {
		tx.BlockHash = blockHash
	}

	return tx, nil
}

// TokenTransfer represents a parsed ERC-20 transfer
type TokenTransfer struct {
	TransactionHash string
	BlockNumber     string
	TokenAddress    string
	From            string
	To              string
	Amount          string
	LogIndex        string
}

// parseTokenTransferLog parses an ERC-20 transfer log
func (p *ChainProcessor) parseTokenTransferLog(logData interface{}) (*TokenTransfer, error) {
	logMap, ok := logData.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid log data format")
	}

	transfer := &TokenTransfer{}

	if txHash, ok := logMap["transactionHash"].(string); ok {
		transfer.TransactionHash = txHash
	}
	if blockNumber, ok := logMap["blockNumber"].(string); ok {
		transfer.BlockNumber = blockNumber
	}
	if address, ok := logMap["address"].(string); ok {
		transfer.TokenAddress = address
	}

	// Parse topics for from/to addresses
	if topics, ok := logMap["topics"].([]interface{}); ok && len(topics) >= 3 {
		// Topic 1: from address (padded to 32 bytes)
		if fromTopic, ok := topics[1].(string); ok && len(fromTopic) >= 66 {
			transfer.From = "0x" + fromTopic[26:] // Remove padding
		}
		// Topic 2: to address (padded to 32 bytes)
		if toTopic, ok := topics[2].(string); ok && len(toTopic) >= 66 {
			transfer.To = "0x" + toTopic[26:] // Remove padding
		}
	}

	// Parse data for amount
	if data, ok := logMap["data"].(string); ok && len(data) > 2 {
		transfer.Amount = data // Raw hex amount
	}

	if logIndex, ok := logMap["logIndex"].(string); ok {
		transfer.LogIndex = logIndex
	}

	return transfer, nil
}

// processMatchedTransaction processes a transaction that we know has a matching deposit account
func (p *ChainProcessor) processMatchedTransaction(ctx context.Context, tx *AnkrTransaction, account bson.M, transferType string, orderUsecase *usecases.OrderUseCase, requestManager *requestManager.RequestManager) bool {
	start := time.Now()

	// Find order that uses this deposit account
	accountIdInterface, ok := account["_id"]
	if !ok {
		p.logger.Warn("Account missing _id field", "walletAddress", tx.To)
		return false
	}

	accountId, ok := accountIdInterface.(primitive.ObjectID)
	if !ok {
		p.logger.Warn("Account _id is not ObjectID", "walletAddress", tx.To, "idType", fmt.Sprintf("%T", accountIdInterface))
		return false
	}

	order, err := p.findOrderByDepositAccount(ctx, accountId)
	if err != nil {
		p.logger.Debug("No order found for deposit account", "accountId", accountId.Hex(), "walletAddress", tx.To, "queryTime", time.Since(start).String())
		return false
	}

	p.logger.Info("🎯 DEPOSIT DETECTED", "walletAddress", tx.To, "txHash", tx.Hash, "transferType", transferType, "processingTime", time.Since(start).String())

	// Process the order transaction
	return p.processOrderTransaction(ctx, order, tx, transferType, requestManager)
}

// processMatchedTokenTransfer processes a token transfer that we know has a matching deposit account
func (p *ChainProcessor) processMatchedTokenTransfer(ctx context.Context, transfer *TokenTransfer, account bson.M, orderUsecase *usecases.OrderUseCase, requestManager *requestManager.RequestManager) bool {
	start := time.Now()

	// Find order that uses this deposit account
	accountIdInterface, ok := account["_id"]
	if !ok {
		p.logger.Warn("Account missing _id field", "walletAddress", transfer.To)
		return false
	}

	accountId, ok := accountIdInterface.(primitive.ObjectID)
	if !ok {
		p.logger.Warn("Account _id is not ObjectID", "walletAddress", transfer.To, "idType", fmt.Sprintf("%T", accountIdInterface))
		return false
	}

	order, err := p.findOrderByDepositAccount(ctx, accountId)
	if err != nil {
		p.logger.Debug("No order found for deposit account", "accountId", accountId.Hex(), "walletAddress", transfer.To, "queryTime", time.Since(start).String())
		return false
	}

	p.logger.Info("🎯 TOKEN TRANSFER DETECTED", "walletAddress", transfer.To, "txHash", transfer.TransactionHash, "tokenAddress", transfer.TokenAddress, "processingTime", time.Since(start).String())

	// Convert TokenTransfer to AnkrTransaction for processing
	tx := &AnkrTransaction{
		Hash:        transfer.TransactionHash,
		To:          transfer.To,
		From:        transfer.From,
		Value:       transfer.Amount,
		BlockNumber: transfer.BlockNumber,
	}

	// Process the order transaction
	return p.processOrderTransaction(ctx, order, tx, "token", requestManager)
}

// findDepositAccountsByAddresses finds ALL deposit accounts for given addresses in ONE query
func (p *ChainProcessor) findDepositAccountsByAddresses(ctx context.Context, addresses []string) ([]bson.M, error) {
	if len(addresses) == 0 {
		return []bson.M{}, nil
	}

	collection := p.mongoClient.Database(p.cfg.MongoDB.Database).Collection("accounts")

	// Convert addresses to lowercase for case-insensitive matching
	lowerAddresses := make([]string, len(addresses))
	for i, addr := range addresses {
		lowerAddresses[i] = strings.ToLower(addr)
	}

	var accounts []bson.M

	// First try exact lowercase matching (fastest)
	cursor, err := collection.Find(ctx, bson.M{
		"walletAddress": bson.M{
			"$in": lowerAddresses,
		},
		"deletedAt": bson.M{"$exists": false},
	})

	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &accounts); err != nil {
		return nil, err
	}

	// If no results found and we have addresses to search, try case-insensitive regex as fallback
	// This handles cases where stored addresses might have mixed case
	if len(accounts) == 0 && len(addresses) > 0 {
		p.logger.Debug("No exact lowercase matches found, trying case-insensitive regex matching", "addressCount", len(addresses))

		// Build regex patterns for case-insensitive matching
		regexPatterns := make([]bson.M, len(addresses))
		for i, addr := range addresses {
			regexPatterns[i] = bson.M{
				"walletAddress": bson.M{
					"$regex":   "^" + addr + "$",
					"$options": "i", // case-insensitive
				},
			}
		}

		cursor2, err := collection.Find(ctx, bson.M{
			"$or":       regexPatterns,
			"deletedAt": bson.M{"$exists": false},
		})

		if err != nil {
			return nil, err
		}
		defer cursor2.Close(ctx)

		if err = cursor2.All(ctx, &accounts); err != nil {
			return nil, err
		}

		if len(accounts) > 0 {
			p.logger.Info("Found accounts using case-insensitive regex matching", "foundCount", len(accounts))
		}
	}

	return accounts, nil
}

// processOrderTransaction processes a matched order transaction and updates order status to DEPOSIT_CONFIRMING
func (p *ChainProcessor) processOrderTransaction(ctx context.Context, order bson.M, tx *AnkrTransaction, transferType string, requestManager *requestManager.RequestManager) bool {
	orderId, ok := order["_id"].(primitive.ObjectID)
	if !ok {
		p.logger.Error("Invalid order ID format")
		return false
	}

	// Parse amount from transaction value
	amount, err := p.parseTransactionAmount(tx.Value, transferType)
	if err != nil {
		p.logger.Error("Failed to parse transaction amount", "value", tx.Value, "error", err)
		return false
	}

	// Update order status to DEPOSIT_CONFIRMING in database
	ordersCollection := p.mongoClient.Database(p.cfg.MongoDB.Database).Collection("orders")

	update := bson.M{
		"$set": bson.M{
			"status":    constants.ORDER_STATUS_DEPOSIT_CONFIRMING,
			"amount":    amount,
			"updatedAt": time.Now(),
			// Store transaction details for reference
			"latestTxHash": tx.Hash,
		},
	}

	result, err := ordersCollection.UpdateByID(ctx, orderId, update)
	if err != nil {
		p.logger.Error("Failed to update order status to DEPOSIT_CONFIRMING",
			"orderId", orderId,
			"txHash", tx.Hash,
			"error", err)
		return false
	}

	if result.MatchedCount == 0 {
		p.logger.Warn("No order found to update",
			"orderId", orderId,
			"txHash", tx.Hash)
		return false
	}

	p.logger.Info("Successfully updated order status to DEPOSIT_CONFIRMING",
		"chain", p.chainSymbol,
		"orderId", orderId,
		"txHash", tx.Hash,
		"amount", amount,
		"transferType", transferType,
		"depositAddress", tx.To)

	return true
}

// parseTransactionAmount parses transaction amount from hex value
func (p *ChainProcessor) parseTransactionAmount(hexValue, transferType string) (float64, error) {
	if hexValue == "" || hexValue == "0x0" {
		return 0, nil
	}

	// Remove 0x prefix if present
	if len(hexValue) > 2 && hexValue[:2] == "0x" {
		hexValue = hexValue[2:]
	}

	// Parse as big integer
	amount := new(big.Int)
	amount, ok := amount.SetString(hexValue, 16)
	if !ok {
		return 0, fmt.Errorf("failed to parse hex amount: %s", hexValue)
	}

	// Convert to float64 with appropriate decimals
	decimals := 18 // Default for most EVM chains (ETH and most tokens use 18 decimals)
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	amountFloat := new(big.Float).SetInt(amount)
	result := new(big.Float).Quo(amountFloat, divisor)

	floatAmount, _ := result.Float64()
	return floatAmount, nil
}

// EnsureOptimalIndexes creates the critical performance indexes if they don't exist
func (p *ChainProcessor) EnsureOptimalIndexes(ctx context.Context) error {
	database := p.mongoClient.Database(p.cfg.MongoDB.Database)

	// Critical index for account lookups - THIS IS THE MOST IMPORTANT ONE
	accountsCollection := database.Collection("accounts")
	accountsIndexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "walletAddress", Value: 1},
			{Key: "deletedAt", Value: 1},
		},
		Options: options.Index().SetName("walletAddress_deletedAt_performance"),
	}

	_, err := accountsCollection.Indexes().CreateOne(ctx, accountsIndexModel)
	if err != nil {
		// Index might already exist, which is fine
		p.logger.Info("Index creation result for accounts collection", "error", err)
	} else {
		p.logger.Info("Created critical performance index for accounts.walletAddress + deletedAt")
	}

	// Index for order lookups
	ordersCollection := database.Collection("orders")
	ordersIndexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "depositAccountIds", Value: 1},
			{Key: "status", Value: 1},
		},
		Options: options.Index().SetName("depositAccountIds_status_performance"),
	}

	_, err = ordersCollection.Indexes().CreateOne(ctx, ordersIndexModel)
	if err != nil {
		p.logger.Info("Index creation result for orders collection", "error", err)
	} else {
		p.logger.Info("Created performance index for orders.depositAccountIds + status")
	}

	return nil
}

// findOrderByDepositAccount finds an order that has the given account in its depositAccountIds
func (p *ChainProcessor) findOrderByDepositAccount(ctx context.Context, accountId primitive.ObjectID) (bson.M, error) {
	collection := p.mongoClient.Database(p.cfg.MongoDB.Database).Collection("orders")

	var order bson.M
	err := collection.FindOne(ctx, bson.M{
		"depositAccountIds": bson.M{"$in": []primitive.ObjectID{accountId}},
		"status":            "PENDING",
		// "deletedAt":         bson.M{"$exists": false},
	}).Decode(&order)

	if err != nil {
		return nil, err
	}

	return order, nil
}

func startScheduler(ctx context.Context, queueName string, redisClient *redis.Client, logger logger.Logger, scheduledTime int) {
	logger.Info("Starting scheduler for EVM processing", "intervalMinutes", scheduledTime)

	ticker := time.NewTicker(time.Duration(scheduledTime) * time.Minute)
	defer ticker.Stop()

	// Run initial job immediately
	go func() {
		if err := enqueueEVMJob(ctx, queueName, redisClient); err != nil {
			logger.Error("Failed to enqueue initial EVM job", "error", err)
		}
	}()

	for {
		select {
		case <-ticker.C:
			if err := enqueueEVMJob(ctx, queueName, redisClient); err != nil {
				logger.Error("Failed to enqueue EVM job", "error", err)
			}
		case <-ctx.Done():
			logger.Info("Scheduler shutting down")
			return
		}
	}
}

// enqueueEVMJob adds a new EVM processing job to the queue (no locking)
func enqueueEVMJob(ctx context.Context, queueName string, redisClient *redis.Client) error {
	jobData := map[string]interface{}{
		"timestamp": time.Now().Unix(),
		"type":      "block_processing",
	}

	jsonData, err := json.Marshal(jobData)
	if err != nil {
		return err
	}

	_, err = redisClient.LPush(ctx, queueName, jsonData).Result()
	return err
}
