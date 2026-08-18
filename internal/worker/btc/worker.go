package btc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/internal/worker"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/http"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/models"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/usecases"
)

type BTCUnspentOutput struct {
	TxHash          string `json:"tx_hash"`
	TxHashBigEndian string `json:"tx_hash_big_endian"`
	TxIndex         int    `json:"tx_index"`
	TxOutputN       int    `json:"tx_output_n"`
	Script          string `json:"script"`
	Value           int64  `json:"value"` // Amount in satoshis
	ValueHex        string `json:"value_hex"`
	Confirmations   int    `json:"confirmations"`
}

// BTCUnspentResponse represents a response with unspent outputs
type BTCUnspentResponse struct {
	UnspentOutputs []BTCUnspentOutput `json:"unspent_outputs"`
}

// BTCTransactionDetails represents Bitcoin transaction details
type BTCTransactionDetails struct {
	Hash      string
	BlockTime int64
	Status    string
	FromAddr  string
	ToAddr    string
	Amount    float64
}

// Environment variables constants
const (
	EnvBTCMainnetBaseURL = "BTC_MAINNET_BASE_URL"
)

// Worker represents the ChangeNow worker service
type Worker struct {
	cfg              *config.Config
	btcConcurrency   int
	logger           logger.Logger
	redisClient      *redis.Client
	queueSvc         *services.QueueService
	mongoClient      *mongo.Client
	cacheManager     *cache.CacheManager
	accountUsecase   *usecases.AccountUseCase
	client           *http.RequestManager
	btcScheduledTime int
}

// New creates a new ChangeNow worker
func New(cfg *config.Config, concurrency int, logger logger.Logger, redisClient *redis.Client, queueSvc *services.QueueService, mongoClient *mongo.Client, cacheManager *cache.CacheManager, accountUsecase *usecases.AccountUseCase, requestManager *http.RequestManager, btcScheduledTime int) worker.Worker {
	return &Worker{
		cfg:              cfg,
		btcConcurrency:   concurrency,
		logger:           logger,
		redisClient:      redisClient,
		queueSvc:         queueSvc,
		mongoClient:      mongoClient,
		cacheManager:     cacheManager,
		accountUsecase:   accountUsecase,
		client:           requestManager,
		btcScheduledTime: btcScheduledTime,
	}
}

// Start initiates the ChangeNow worker processing
func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info("Initializing Bitcoin block monitor worker...")

	// Parse config values
	mongoURI := w.cfg.MongoDB.URI

	w.logger.Info("Connected to MongoDB", "uri", mongoURI, "database", w.cfg.MongoDB.Database)

	// Initialize queue manager
	queueManager := w.queueSvc.GetQueueManager()

	requestManager := http.Initialize(w.logger)

	// Initialize ChangeNow provider
	bitcoinConfig := config.BitcoinConfig{
		Network:     "mainnet",
		RPCEndpoint: "https://blockchain.info",
	}

	// provider, err := bitcoin.NewProvider(w.logger, bitcoinConfig)
	// if err != nil {
	// 	w.logger.Error("Failed to initialize Bitcoin provider", "error", err)
	// 	return err
	// }
	w.logger.Info("Bitcoin provider initialized")

	// Get database
	db := w.mongoClient.Database(w.cfg.MongoDB.Database)

	queueName := constants.QueueNames["QueueBitcoinBlockMonitor"]

	w.logger.Info("Using queue for Bitcoin watcher", "queueName", queueName)

	// Register Bitcoin block monitor handler
	queueManager.RegisterHandler(queueName, func(ctx context.Context, data map[string]interface{}) error {
		w.logger.Debug("Handler invoked", "data", data)
		return processBitcoinAccounts(ctx, db, w.logger, w.cacheManager, w.accountUsecase, bitcoinConfig.RPCEndpoint, requestManager)
	})

	// Start processing jobs
	w.logger.Info("Starting to process Bitcoin block monitor jobs", "concurrency", w.btcConcurrency)
	err := queueManager.ProcessJobs(ctx, queueName, w.btcConcurrency)
	if err != nil {
		w.logger.Error("Failed to start processing jobs", "error", err)
		return err
	}

	// Start scheduler for Bitcoin processing
	go startScheduler(ctx, queueName, w.redisClient, w.logger, w.btcScheduledTime)

	// Wait for context to be done
	<-ctx.Done()
	w.logger.Info("Bitcoin block monitor worker shutting down")
	return nil
}

func startScheduler(ctx context.Context, queueName string, redisClient *redis.Client, logger logger.Logger, scheduledTime int) {
	ticker := time.NewTicker(time.Duration(scheduledTime) * time.Minute)
	defer ticker.Stop()

	logger.Info("Starting scheduler for Bitcoin processing")

	// Run immediately on startup
	if err := enqueueBitcoinJob(ctx, queueName, redisClient); err != nil {
		logger.Error("Failed to enqueue initial Bitcoin job", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping scheduler")
			return
		case <-ticker.C:
			if err := enqueueBitcoinJob(ctx, queueName, redisClient); err != nil {
				logger.Error("Failed to enqueue Bitcoin job", "error", err)
			}
		}
	}
}

func enqueueBitcoinJob(ctx context.Context, queueName string, redisClient *redis.Client) error {
	jobData := map[string]interface{}{
		"timestamp": time.Now().Unix(),
	}
	jsonData, err := json.Marshal(jobData)
	if err != nil {
		return fmt.Errorf("failed to marshal job data: %w", err)
	}

	if err := redisClient.RPush(ctx, queueName, jsonData).Err(); err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}
	return nil
}

func processBitcoinAccounts(
	ctx context.Context,
	db *mongo.Database,
	logger logger.Logger,
	cacheManager *cache.CacheManager,
	accountUsecase *usecases.AccountUseCase,
	apiURL string,
	client *http.RequestManager,
) error {
	logger.Info("Starting Bitcoin accounts processing")

	// Get all Bitcoin accounts
	accounts, err := getActiveAccountsByChainId(ctx, db, "BTC", logger)
	if err != nil {
		logger.Error("Failed to get Bitcoin accounts", "error", err)
		return err
	}

	logger.Info("Retrieved Bitcoin accounts", "count", len(accounts))

	// Process account balances and transactions in parallel
	processAccountBalancesAndTransactions(ctx, accounts, logger, db, accountUsecase, cacheManager, client, apiURL)

	logger.Info("Bitcoin accounts processing completed")
	return nil
}

// getActiveAccountsByChainId retrieves accounts for a specific blockchain by chainId
// but only for users who have been active in the last 3 days.
// This optimization helps focus processing resources on accounts of active users,
// improving performance and reducing unnecessary API calls.
func getActiveAccountsByChainId(ctx context.Context, db *mongo.Database, chainSymbol string, logger logger.Logger) ([]map[string]interface{}, error) {
	logger.Info("Getting active accounts by chain symbol", "chainSymbol", chainSymbol)

	// Step 1: Get the chain document by symbol to obtain the chainId
	chainsCollection := db.Collection("chains")
	var chain map[string]interface{}

	err := chainsCollection.FindOne(ctx, bson.M{"symbol": chainSymbol}).Decode(&chain)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			logger.Error("Chain not found", "chainSymbol", chainSymbol)
			return nil, fmt.Errorf("chain with symbol %s not found", chainSymbol)
		}
		logger.Error("Error finding chain", "error", err)
		return nil, err
	}

	chainId, ok := chain["_id"].(primitive.ObjectID)
	if !ok {
		logger.Error("Invalid chain ID format")
		return nil, fmt.Errorf("invalid chain ID format")
	}

	logger.Info("Found chain", "chainId", chainId, "chainName", chain["name"])

	// Step 2: Calculate timestamp for 3 days ago to filter active users
	threeDaysAgo := time.Now().AddDate(0, 0, -3)
	logger.Debug("Using activity threshold", "date", threeDaysAgo.Format(time.RFC3339))

	// Step 3: Set up MongoDB aggregation pipeline to join accounts with users and filter
	accountsCollection := db.Collection("accounts")

	// MongoDB aggregation pipeline to filter active accounts
	pipeline := mongo.Pipeline{
		// Stage 1: Match accounts for this chain that haven't been deleted
		{{Key: "$match", Value: bson.D{
			{Key: "chainId", Value: chainId},
			{Key: "deletedAt", Value: bson.M{"$exists": false}},
		}}},
		// Stage 2: Lookup to join with Wallet-Users collection based on userId
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "Wallet-Users"},
			{Key: "localField", Value: "userId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "user"},
		}}},
		// Stage 3: Unwind the user array (converts it from array to object)
		// This effectively filters out accounts where no matching user was found
		{{Key: "$unwind", Value: bson.D{{Key: "path", Value: "$user"}, {Key: "preserveNullAndEmptyArrays", Value: false}}}},
		// Stage 4: Match only users who have been active in the last 3 days and are not deleted
		{{Key: "$match", Value: bson.D{
			{Key: "user.lastActiveTime", Value: bson.M{"$gte": threeDaysAgo}},
			{Key: "user.isDeleted", Value: bson.M{"$ne": true}},
		}}},
		// Stage 5: Project stage to remove the user field from results to maintain same structure
		// as the original getAccountsByChainId function
		{{Key: "$project", Value: bson.D{
			{Key: "user", Value: 0}, // Remove the user field from results
		}}},
	}

	// Step 4: Execute the aggregation pipeline
	cursor, err := accountsCollection.Aggregate(ctx, pipeline)
	if err != nil {
		logger.Error("Error querying active accounts", "error", err)
		return nil, err
	}
	defer cursor.Close(ctx)

	// Step 5: Decode the results
	var accounts []map[string]interface{}
	if err = cursor.All(ctx, &accounts); err != nil {
		logger.Error("Error decoding active accounts", "error", err)
		return nil, err
	}

	logger.Info("Found active accounts", "count", len(accounts), "activeThreshold", "3 days")
	return accounts, nil
}

// processAccountBalancesAndTransactions processes balances and transaction histories for all accounts
func processAccountBalancesAndTransactions(
	ctx context.Context,
	accounts []map[string]interface{},
	logger logger.Logger,
	db *mongo.Database,
	accountUsecase *usecases.AccountUseCase,
	cacheManager *cache.CacheManager,
	client *http.RequestManager,
	apiURL string,
) {
	accountsCount := len(accounts)
	logger.Info("Processing accounts balances and transactions", "count", accountsCount)

	if accountsCount == 0 {
		logger.Info("No accounts to process")
		return
	}

	// Use a worker pool to process accounts in parallel
	maxWorkers := 5 // Bitcoin API may have rate limits, so keep this lower
	if accountsCount < maxWorkers {
		maxWorkers = accountsCount
	}

	// Create channels for work distribution
	jobs := make(chan map[string]interface{}, accountsCount)
	results := make(chan error, accountsCount)

	// Start worker pool
	for w := 1; w <= maxWorkers; w++ {
		go func(workerID int) {
			for account := range jobs {
				logger.Debug("Worker processing account",
					"workerID", workerID,
					"accountId", account["_id"],
					"walletAddress", account["walletAddress"])

				// Process the account
				err := processAccountBalanceAndTransactions(ctx, account, apiURL, client, logger, db, accountUsecase, cacheManager)
				results <- err
			}
		}(w)
	}

	// Send jobs to workers
	for _, account := range accounts {
		jobs <- account
	}
	close(jobs)

	// Collect results
	var successCount, errorCount int
	for i := 0; i < accountsCount; i++ {
		err := <-results
		if err != nil {
			errorCount++
			logger.Error("Failed to process account", "error", err, "position", i+1)
		} else {
			successCount++
		}
	}

	logger.Info("Account processing completed",
		"total", accountsCount,
		"successful", successCount,
		"failed", errorCount)
}

// processAccountBalanceAndTransactions processes a single account's balance and transaction history
func processAccountBalanceAndTransactions(
	ctx context.Context,
	account map[string]interface{},
	apiURL string,
	client *http.RequestManager,
	logger logger.Logger,
	db *mongo.Database,
	accountUsecase *usecases.AccountUseCase,
	cacheManager *cache.CacheManager,
) error {

	walletAddress, ok := account["walletAddress"].(string)
	if !ok || walletAddress == "" {
		return fmt.Errorf("invalid wallet address for account %v", account["_id"])
	}

	// Validate Bitcoin address format
	if !isValidBitcoinAddress(walletAddress) {
		return fmt.Errorf("invalid Bitcoin address format: %s", walletAddress)
	}

	userId, ok := account["userId"].(primitive.ObjectID)
	if !ok {
		return fmt.Errorf("invalid userId for account %v", account["_id"])
	}

	// Get supported currency info for this account
	supportedCurrency, err := getSupportedCurrency(ctx, db, account, logger)
	if err != nil {
		logger.Error("Failed to get supported currency", "error", err)
		return err
	}

	// Log the currency information
	logger.Debug("Using supported currency",
		"walletAddress", walletAddress,
		"currencyId", supportedCurrency["_id"])

	// 1. Get account balance
	balance, err := getBitcoinAccountBalance(ctx, apiURL, walletAddress, client, logger)
	if err != nil {
		logger.Error("Failed to get account balance",
			"walletAddress", walletAddress,
			"error", err)
		return err
	}

	// Update account balance in database
	accountsCollection := db.Collection("accounts")
	updateResult, err := accountsCollection.UpdateOne(
		ctx,
		bson.M{"_id": account["_id"]},
		bson.M{
			"$set": bson.M{
				"balance":   balance,
				"updatedAt": time.Now(),
			},
		},
	)

	if err != nil {
		logger.Error("Failed to update account balance",
			"accountId", account["_id"],
			"error", err)
		return err
	}

	// Expire wallet portfolio cache
	if userId, ok := account["userId"].(primitive.ObjectID); ok {
		// Extract accountId
		accountId, ok := account["_id"].(primitive.ObjectID)
		if !ok {
			logger.Warn("Failed to get account ID", "account", account["_id"])
			return err
		}

		// Match the format used in Node.js: `${USER_PORTFOLIO_CACHE_KEY}:${userId.toString()}`
		walletCacheKey := fmt.Sprintf("%s:%s", constants.WALLET_PORTFOLIO_CACHE_KEY, userId.Hex())
		accountCacheKey := fmt.Sprintf("%s:%s", constants.ACCOUNT_PORTFOLIO_CACHE_KEY, accountId.Hex())

		// Handle each error separately
		if err := cacheManager.Delete(ctx, walletCacheKey); err != nil {
			logger.Warn("Failed to expire wallet portfolio cache", "error", err, "userId", userId.Hex())
		} else {
			logger.Debug("Expired wallet portfolio cache", "userId", userId.Hex())
		}

		if err := cacheManager.Delete(ctx, accountCacheKey); err != nil {
			logger.Warn("Failed to expire account portfolio cache", "error", err, "accountId", accountId.Hex())
		} else {
			logger.Debug("Expired account portfolio cache", "accountId", accountId.Hex())
		}
	}

	logger.Info("Updated account balance",
		"accountId", account["_id"],
		"walletAddress", walletAddress,
		"balance", balance,
		"modified", updateResult.ModifiedCount)

	// 2. Get transaction history and process it
	err = processTransactionHistory(ctx, apiURL, walletAddress, userId, account, client, logger, db, accountUsecase, supportedCurrency)
	if err != nil {
		logger.Error("Failed to process transaction history",
			"walletAddress", walletAddress,
			"error", err)
		return err
	}

	return nil
}

// isValidBitcoinAddress checks if the given address follows Bitcoin address format
func isValidBitcoinAddress(address string) bool {
	// Simple check for Bitcoin address format
	// Mainnet addresses start with 1, 3, or bc1
	if strings.HasPrefix(address, "1") ||
		strings.HasPrefix(address, "3") ||
		strings.HasPrefix(address, "bc1") {
		return true
	}
	// Skip testnet addresses (starting with m, n, or 2)
	return false
}

// getSupportedCurrency retrieves supported currency information for an account
func getSupportedCurrency(ctx context.Context, db *mongo.Database, account map[string]interface{}, logger logger.Logger) (map[string]interface{}, error) {
	// Extract chainId and currencyId from account
	chainId := account["chainId"]
	currencyId, hasCurrencyId := account["currencyId"]

	// Early validation
	if chainId == nil {
		return nil, fmt.Errorf("chainId is missing from account")
	}

	if hasCurrencyId && currencyId == nil {
		logger.Warn("currencyId is nil despite hasCurrencyId being true")
		hasCurrencyId = false
	}

	supportedCurrenciesCollection := db.Collection("supportedcurrencies")

	// Convert chainId to ObjectID regardless of its original type
	var chainIdObj primitive.ObjectID
	var err error

	// Try different approaches to convert to ObjectID
	switch v := chainId.(type) {
	case primitive.ObjectID:
		// Already an ObjectID
		chainIdObj = v
	case string:
		// Try to convert string to ObjectID
		chainIdObj, err = primitive.ObjectIDFromHex(v)
		if err != nil {
			logger.Error("Invalid chainId format", "chainId", v, "error", err)
			return nil, fmt.Errorf("invalid chainId format: %w", err)
		}
	default:
		// Try to convert to string first, then to ObjectID
		chainIdStr := fmt.Sprintf("%v", chainId)
		chainIdObj, err = primitive.ObjectIDFromHex(chainIdStr)
		if err != nil {
			logger.Error("Failed to convert chainId to ObjectID", "chainId", chainIdStr, "error", err)
			return nil, fmt.Errorf("failed to convert chainId to ObjectID: %w", err)
		}
	}

	// Same process for currencyId if it exists
	var currencyIdObj primitive.ObjectID
	var currencyIdValid bool = false
	if hasCurrencyId {
		switch v := currencyId.(type) {
		case primitive.ObjectID:
			currencyIdObj = v
			currencyIdValid = true
		case string:
			currencyIdObj, err = primitive.ObjectIDFromHex(v)
			if err != nil {
				logger.Warn("Invalid currencyId format, will use only chainId for query",
					"currencyId", v, "error", err)
				hasCurrencyId = false
			} else {
				currencyIdValid = true
			}
		default:
			currencyIdStr := fmt.Sprintf("%v", currencyId)
			currencyIdObj, err = primitive.ObjectIDFromHex(currencyIdStr)
			if err != nil {
				logger.Warn("Failed to convert currencyId to ObjectID, will use only chainId for query",
					"currencyId", currencyIdStr, "error", err)
				hasCurrencyId = false
			} else {
				currencyIdValid = true
			}
		}
	}

	// Build filter using the ObjectIDs
	filter := bson.M{"chainId": chainIdObj}
	if hasCurrencyId && currencyIdValid {
		filter["currencyId"] = currencyIdObj
	}

	logger.Info("Querying supportedCurrencies with ObjectIDs",
		"chainIdHex", chainIdObj.Hex(),
		"hasCurrencyId", hasCurrencyId,
		"currencyIdHex", func() string {
			if hasCurrencyId {
				return currencyIdObj.Hex()
			}
			return "not used"
		}())

	// Query the database
	cursor, err := supportedCurrenciesCollection.Find(ctx, filter)
	if err != nil {
		logger.Error("Failed to query supportedCurrencies", "error", err)
		return nil, fmt.Errorf("failed to query database: %w", err)
	}
	defer cursor.Close(ctx)

	// Decode the results
	var supportedCurrencies []map[string]interface{}
	if err := cursor.All(ctx, &supportedCurrencies); err != nil {
		logger.Error("Failed to decode query results", "error", err)
		return nil, fmt.Errorf("failed to decode query results: %w", err)
	}

	// Check if we found anything
	if len(supportedCurrencies) == 0 {
		// Return default empty document
		logger.Warn("No supported currency found for the provided IDs, using default")
		return map[string]interface{}{
			"tokenAddress": "",
			"chainId":      chainIdObj,
		}, nil
	}

	// Return the first matching currency
	supportedCurrency := supportedCurrencies[0]
	logger.Info("Found matching supported currency",
		"id", supportedCurrency["_id"])

	return supportedCurrency, nil
}

// getBitcoinAccountBalance gets the BTC balance for an account
func getBitcoinAccountBalance(ctx context.Context, apiURL string, walletAddress string, client *http.RequestManager, logger logger.Logger) (float64, error) {
	// For Bitcoin, we'll use the unspent outputs endpoint to calculate balance
	url := fmt.Sprintf("%s/unspent?active=%s", apiURL, walletAddress)

	logger.Debug("Requesting Bitcoin balance", "url", url)

	responseBody, err := client.MakeBitcoinRequest(ctx, url, &http.RequestOptions{
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Method: "GET",
		URL:    url,
	})
	if err != nil {
		return 0, fmt.Errorf("error making request: %w", err)
	}

	logger.Debug("Bitcoin API response", "response", string(responseBody))

	// Check if the response contains an error message
	var errorResp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(responseBody, &errorResp); err == nil && errorResp.Error != "" {
		if errorResp.Error == "No free outputs" {
			// This is normal for addresses with zero balance
			logger.Debug("Address has no unspent outputs (zero balance)",
				"walletAddress", walletAddress)
			return 0, nil
		}
		return 0, fmt.Errorf("API error: %s", errorResp.Error)
	}

	// Parse the actual response
	var unspentResp BTCUnspentResponse
	if err := json.Unmarshal(responseBody, &unspentResp); err != nil {
		logger.Error("Failed to parse unspent outputs response",
			"error", err,
			"body", string(responseBody[:min(len(responseBody), 200)]))
		return 0, fmt.Errorf("error parsing response: %w", err)
	}

	// Calculate total balance in satoshis
	var totalSatoshis int64
	for _, output := range unspentResp.UnspentOutputs {
		totalSatoshis += output.Value
	}

	// Convert from satoshis to BTC (1 BTC = 100,000,000 satoshis)
	balanceBTC := float64(totalSatoshis) / 100000000.0

	logger.Debug("Got Bitcoin balance",
		"address", walletAddress,
		"outputs", len(unspentResp.UnspentOutputs),
		"balance", balanceBTC)

	return balanceBTC, nil
}

// processTransactionHistory gets transaction history for a Bitcoin account and stores it
func processTransactionHistory(
	ctx context.Context,
	apiURL string,
	walletAddress string,
	userId primitive.ObjectID,
	account map[string]interface{},
	client *http.RequestManager,
	logger logger.Logger,
	db *mongo.Database,
	accountUsecase *usecases.AccountUseCase,
	supportedCurrency map[string]interface{},
) error {
	// Use the same unspent endpoint as for balance
	url := fmt.Sprintf("%s/unspent?active=%s&limit=50", apiURL, walletAddress)

	logger.Debug("Requesting Bitcoin transaction data", "url", url)

	responseBody, err := client.MakeBitcoinRequest(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}

	logger.Debug("Bitcoin API response", "response", string(responseBody))

	// Check if the response contains an error message
	var errorResp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(responseBody, &errorResp); err == nil && errorResp.Error != "" {
		if errorResp.Error == "No free outputs" {
			// This is normal for addresses with zero balance
			logger.Debug("Address has no unspent outputs (zero balance)",
				"walletAddress", walletAddress)
			return nil
		}
		return fmt.Errorf("API error: %s", errorResp.Error)
	}

	// Parse the actual response
	var unspentResp BTCUnspentResponse
	if err := json.Unmarshal(responseBody, &unspentResp); err != nil {
		logger.Error("Failed to parse unspent outputs response",
			"error", err,
			"body", string(responseBody[:min(len(responseBody), 200)]))
		return fmt.Errorf("error parsing response: %w", err)
	}

	logger.Info("Got unspent outputs",
		"address", walletAddress,
		"count", len(unspentResp.UnspentOutputs))

	// Process each unspent output
	for _, utxo := range unspentResp.UnspentOutputs {
		// Check if this transaction is already in the database
		transactionsCollection := db.Collection("transactions")
		count, err := transactionsCollection.CountDocuments(ctx, bson.M{"signature": utxo.TxHashBigEndian})
		if err != nil {
			logger.Error("Failed to check if transaction exists", "error", err)
			continue
		}

		if count > 0 {
			logger.Debug("Transaction already processed", "txid", utxo.TxHashBigEndian)
			continue
		}

		// Get transaction details from hash
		txDetails, err := getTransactionDetails(ctx, apiURL, utxo.TxHashBigEndian, walletAddress, client)
		if err != nil {
			logger.Error("Failed to get transaction details",
				"txid", utxo.TxHashBigEndian,
				"error", err)
			continue
		}

		// Get accountId and chainId as primitive.ObjectID
		var accountId, chainId primitive.ObjectID

		if id, ok := account["_id"].(primitive.ObjectID); ok {
			accountId = id
		} else if idStr, ok := account["_id"].(string); ok {
			if oid, err := primitive.ObjectIDFromHex(idStr); err == nil {
				accountId = oid
			}
		}

		if id, ok := account["chainId"].(primitive.ObjectID); ok {
			chainId = id
		} else if idStr, ok := account["chainId"].(string); ok {
			if oid, err := primitive.ObjectIDFromHex(idStr); err == nil {
				chainId = oid
			}
		}

		//create account for receiverId using accountUsecase
		receiverAccount, err := accountUsecase.CreateOrFindAccount(ctx, userId, chainId, txDetails.ToAddr)
		if err != nil {
			logger.Error("Failed to create receiver account", "error", err)
			continue
		}

		// Convert satoshis to BTC
		amount := float64(utxo.Value) / 100000000.0

		// Create transaction model
		transaction := models.Transaction{
			ID:          primitive.NewObjectID(),
			InitiatorId: userId,
			Hash:        utxo.TxHashBigEndian,
			ReceiverId:  receiverAccount.ID,
			CurrencyID:  supportedCurrency["currencyId"].(primitive.ObjectID),
			Amount:      amount,
			SenderId:    accountId,
			ChainID:     chainId,
			Success:     true,     // Unspent outputs are by definition successful
			Type:        "WALLET", // All UTXOs represent received funds
			Direction:   "RECEIVE",
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}

		// Insert into database
		_, err = transactionsCollection.InsertOne(ctx, transaction)
		if err != nil {
			logger.Error("Failed to store transaction", "error", err)
			continue
		}

		logger.Info("Stored new transaction",
			"txid", utxo.TxHashBigEndian,
			"amount", amount)
	}

	return nil
}

// getTransactionDetails gets basic details for a transaction
func getTransactionDetails(
	ctx context.Context,
	apiURL string,
	txHash string,
	walletAddress string,
	client *http.RequestManager,
) (*BTCTransactionDetails, error) {
	url := fmt.Sprintf("%s/rawtx/%s", apiURL, txHash)

	responseBody, err := client.MakeBitcoinRequest(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}

	// Log the raw response for debugging
	fmt.Println("Transaction API response:", string(responseBody))

	// Check if the response contains an error message
	var errorResp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(responseBody, &errorResp); err == nil && errorResp.Error != "" {
		return nil, fmt.Errorf("API error: %s", errorResp.Error)
	}

	// Parse JSON directly from response body
	var rawTx map[string]interface{}
	if err := json.Unmarshal(responseBody, &rawTx); err != nil {
		return nil, fmt.Errorf("error parsing transaction: %w", err)
	}

	// Extract basic transaction details
	txDetails := &BTCTransactionDetails{
		Hash:     txHash,
		Status:   "success",     // Unspent outputs are successful by definition
		FromAddr: "unknown",     // Will try to determine below
		ToAddr:   walletAddress, // The wallet address is the receiver for UTXOs
	}

	// Get block time if available
	if timeVal, ok := rawTx["time"].(float64); ok {
		txDetails.BlockTime = int64(timeVal)
	}

	// Try to determine sender address
	if inputs, ok := rawTx["inputs"].([]interface{}); ok && len(inputs) > 0 {
		if input, ok := inputs[0].(map[string]interface{}); ok {
			if prevOut, ok := input["prev_out"].(map[string]interface{}); ok {
				if addr, ok := prevOut["addr"].(string); ok {
					txDetails.FromAddr = addr
				}
			}
		}
	}

	return txDetails, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
