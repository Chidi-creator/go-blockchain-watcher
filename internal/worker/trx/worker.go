package btc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	http2 "net/http"
	"os"
	"strconv"
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

type TronTransaction struct {
	TxID            string       `json:"txID"`
	BlockNumber     int64        `json:"blockNumber"`
	BlockTimeStamp  int64        `json:"block_timestamp"`
	ContractAddress string       `json:"contract_address,omitempty"`
	To              string       `json:"to,omitempty"`
	From            string       `json:"from,omitempty"`
	Value           string       `json:"value,omitempty"`
	Type            string       `json:"type,omitempty"`
	RawData         *TronRawData `json:"raw_data,omitempty"`
	Confirmed       bool         `json:"confirmed"`
}

// TronRawData contains raw data for a transaction
type TronRawData struct {
	Contract  []TronContract `json:"contract"`
	Timestamp int64          `json:"timestamp"`
}

// TronContract contains the contract data
type TronContract struct {
	Type           string          `json:"type"`
	Parameter      json.RawMessage `json:"parameter"`
	ParameterValue struct {
		OwnerAddress string `json:"owner_address"`
		ToAddress    string `json:"to_address,omitempty"`
		Amount       int64  `json:"amount,omitempty"`
	} `json:"-"`
}

// TronTransactionResponse represents the API response for transaction queries
type TronTransactionResponse struct {
	Data    []TronTransaction `json:"data"`
	Success bool              `json:"success"`
	Meta    struct {
		Links struct {
			Next string `json:"next"`
		} `json:"links"`
		Page        int    `json:"page"`
		Fingerprint string `json:"fingerprint"`
	} `json:"meta"`
}

// TronTransactionDetails represents TRON transaction details for processing
type TronTransactionDetails struct {
	Hash      string
	BlockTime int64
	Status    string
	FromAddr  string
	ToAddr    string
	Amount    float64
	TokenAddr string
}

// Environment variables constants

// Default API endpoints
const (
	DefaultTronAPIURL = "https://api.trongrid.io"
)

// Worker represents the ChangeNow worker service
type Worker struct {
	cfg              *config.Config
	concurrency      int
	logger           logger.Logger
	redisClient      *redis.Client
	queueSvc         *services.QueueService
	mongoClient      *mongo.Client
	cacheManager     *cache.CacheManager
	accountUsecase   *usecases.AccountUseCase
	client           *http.RequestManager
	trxScheduledTime int
}

// New creates a new ChangeNow worker
func New(cfg *config.Config, concurrency int, logger logger.Logger, redisClient *redis.Client, queueSvc *services.QueueService, mongoClient *mongo.Client, cacheManager *cache.CacheManager, accountUsecase *usecases.AccountUseCase, requestManager *http.RequestManager, trxScheduledTime int) worker.Worker {
	return &Worker{
		cfg:              cfg,
		concurrency:      concurrency,
		logger:           logger,
		redisClient:      redisClient,
		queueSvc:         queueSvc,
		mongoClient:      mongoClient,
		cacheManager:     cacheManager,
		accountUsecase:   accountUsecase,
		client:           requestManager,
		trxScheduledTime: trxScheduledTime,
	}
}

// Start initiates the ChangeNow worker processing
func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info("Initializing Solana Worker...")

	// Parse config values
	mongoURI := w.cfg.MongoDB.URI

	w.logger.Info("Connected to MongoDB", "uri", mongoURI, "database", w.cfg.MongoDB.Database)

	// Initialize queue manager
	queueManager := w.queueSvc.GetQueueManager()

	EnvTronAPIKey := os.Getenv("TRON_API_KEY")
	EnvTronAPIURL := os.Getenv("TRON_API_URL")

	// Get database
	db := w.mongoClient.Database(w.cfg.MongoDB.Database)

	queueName := constants.QueueNames["QueueTronBlockMonitor"]

	w.logger.Info("Using queue for TRON watcher", "queueName", queueName)

	accounts, err := getActiveAccountsByChainId(ctx, db, "TRX", w.logger)
	if err != nil {
		w.logger.Error("Failed to get active TRON accounts", "error", err)
		return err
	}

	// Register Solana watcher handler
	queueManager.RegisterHandler(queueName, func(ctx context.Context, data map[string]interface{}) error {
		w.logger.Debug("Handler invoked", "data", data)
		return processAccountBalancesAndTransactions(ctx, accounts, EnvTronAPIURL, EnvTronAPIKey, http2.DefaultClient, w.client, w.logger, db, w.accountUsecase, w.cacheManager)
	})

	// Start processing jobs
	w.logger.Info("Starting to process TRON watcher jobs", "concurrency", w.concurrency)
	err = queueManager.ProcessJobs(ctx, queueName, w.concurrency)
	if err != nil {
		w.logger.Error("Failed to start processing jobs", "error", err)
		return err
	}

	// Start health checker for Redis
	go startScheduler(ctx, queueName, w.redisClient, w.logger, w.trxScheduledTime)

	// Wait for context to be done
	<-ctx.Done()
	w.logger.Info("TRON worker shutting down")
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

func startScheduler(ctx context.Context, queueName string, redisClient *redis.Client, logger logger.Logger, scheduledTime int) {
	ticker := time.NewTicker(time.Duration(scheduledTime) * time.Minute)
	defer ticker.Stop()

	logger.Info("Starting scheduler for TRON processing")

	// Run immediately on startup
	if err := enqueueTronJob(ctx, queueName, redisClient); err != nil {
		logger.Error("Failed to enqueue initial TRON job", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping scheduler")
			return
		case <-ticker.C:
			if err := enqueueTronJob(ctx, queueName, redisClient); err != nil {
				logger.Error("Failed to enqueue TRON job", "error", err)
			}
		}
	}
}

func enqueueTronJob(ctx context.Context, queueName string, redisClient *redis.Client) error {
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

// processAccountBalancesAndTransactions processes balances and transactions for TRON accounts
func processAccountBalancesAndTransactions(
	ctx context.Context,
	accounts []map[string]interface{},
	apiURL string,
	apiKey string,
	client *http2.Client,
	requestManager *http.RequestManager,
	logger logger.Logger,
	db *mongo.Database,
	accountUsecase *usecases.AccountUseCase,
	cacheManager *cache.CacheManager,
) error {
	accountsCount := len(accounts)
	logger.Info("Processing TRON accounts balances and transactions", "count", accountsCount)

	if accountsCount == 0 {
		logger.Info("No accounts to process")
		return nil
	}

	// Use a worker pool to process accounts in parallel
	maxWorkers := 3 // TronGrid API may have rate limits, keep this conservative
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
				err := processAccountBalanceAndTransactions(ctx, account, apiURL, apiKey, client, requestManager, logger, db, accountUsecase, cacheManager)
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

	return nil
}

// processAccountBalanceAndTransactions processes a single TRON account's balance and transactions
func processAccountBalanceAndTransactions(
	ctx context.Context,
	account map[string]interface{},
	apiURL string,
	apiKey string,
	client *http2.Client,
	requestManager *http.RequestManager,
	logger logger.Logger,
	db *mongo.Database,
	accountUsecase *usecases.AccountUseCase,
	cacheManager *cache.CacheManager,
) error {
	walletAddress, ok := account["walletAddress"].(string)
	if !ok || walletAddress == "" {
		return fmt.Errorf("invalid wallet address for account %v", account["_id"])
	}

	// Check if this is a valid TRON address (should be 34 characters starting with 'T')
	if !isValidTronAddress(walletAddress) {
		return fmt.Errorf("invalid TRON address format: %s", walletAddress)
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

	watchedOrders, err := FindOrdersWithMatchingDepositAddress(ctx, walletAddress, db)

	if err != nil {
		logger.Error("Failed to find orders with watched account of address",
			"walletAddress", walletAddress,
			"error", err)
		return err
	}

	if len(watchedOrders) == 0 {
		logger.Info("You are currently watching address", walletAddress)
	} else {
		_, err := ProcessWatchedAddress(ctx, walletAddress, watchedOrders, requestManager, logger)

		if err != nil {
			logger.Error("Failed to process watched address",
				"walletAddress", walletAddress,
				"error", err)
		}
	}

	// 1. Get account balance (TRX native token)
	balance, err := getTronAccountBalance(ctx, apiURL, apiKey, walletAddress, client, logger)
	if err != nil {
		logger.Error("Failed to get account TRX balance",
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

	// Expire wallet and account portfolio caches
	userId, ok := account["userId"].(primitive.ObjectID)
	if ok {
		// Extract accountId safely
		accountId, ok := account["_id"].(primitive.ObjectID)
		if !ok {
			logger.Warn("Failed to get account ID", "account", account["_id"])
			// We can continue since this is just for cache
		} else {
			// Create cache keys
			walletCacheKey := fmt.Sprintf("%s:%s", constants.WALLET_PORTFOLIO_CACHE_KEY, userId.Hex())
			accountCacheKey := fmt.Sprintf("%s:%s", constants.ACCOUNT_PORTFOLIO_CACHE_KEY, accountId.Hex())

			// Delete wallet cache
			if err := cacheManager.Delete(ctx, walletCacheKey); err != nil {
				logger.Warn("Failed to expire wallet portfolio cache", "error", err, "userId", userId.Hex())
			} else {
				logger.Debug("Expired wallet portfolio cache", "userId", userId.Hex())
			}

			// Delete account cache
			if err := cacheManager.Delete(ctx, accountCacheKey); err != nil {
				logger.Warn("Failed to expire account portfolio cache", "error", err, "accountId", accountId.Hex())
			} else {
				logger.Debug("Expired account portfolio cache", "accountId", accountId.Hex())
			}
		}
	}

	logger.Info("Updated account balance",
		"accountId", account["_id"],
		"walletAddress", walletAddress,
		"balance", balance,
		"modified", updateResult.ModifiedCount)

	// 2. Process native TRX transactions first
	err = processNativeTransactions(ctx, apiURL, apiKey, walletAddress, userId, account, client, logger, db, accountUsecase)
	if err != nil {
		logger.Error("Failed to process native TRX transactions",
			"walletAddress", walletAddress,
			"error", err)
		// Continue processing tokens despite error
	}

	// 3. Process TRC20 token transactions
	// Check if we have a token contract address in supported currency
	if tokenAddress, ok := supportedCurrency["tokenAddress"].(string); ok && tokenAddress != "" {
		err = processTokenTransactions(ctx, apiURL, apiKey, walletAddress, tokenAddress, userId, account, client, logger, db, accountUsecase)
		if err != nil {
			logger.Error("Failed to process token transactions",
				"walletAddress", walletAddress,
				"tokenAddress", tokenAddress,
				"error", err)
			// Continue despite error
		}
	}

	return nil
}

// isValidTronAddress checks if the given address is a valid TRON address
func isValidTronAddress(address string) bool {
	return strings.HasPrefix(address, "T") && len(address) == 34
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

func ProcessWatchedAddress(ctx context.Context, address string, orders []bson.M, requestManager *http.RequestManager, logger logger.Logger) ([]bson.M, error) {

	//loop through orders
	for _, order := range orders {
		//get order id
		orderID := order["_id"].(primitive.ObjectID)

		//get order
		order := order["order"].(bson.M)

		//create object to send to url
		orderObj := map[string]interface{}{
			"order":        order,
			"goWorkerType": "sol",
		}

		request, err := requestManager.Post(ctx, "NODE_SERVER", "/webhooks/watchAddress", orderObj, nil)
		if err != nil {
			logger.Error("Failed to update order status", "error", err, "orderId", orderID)
		} else {
			logger.Info("Updated order status to completed", "orderId", orderID)
		}

		fmt.Println("request", string(request))
	}

	return nil, nil
}

func FindOrdersWithMatchingDepositAddress(ctx context.Context, address string, db *mongo.Database) ([]bson.M, error) {
	pipeline := mongo.Pipeline{
		// Stage 1: Match initial criteria for orders
		{{Key: "$match", Value: bson.M{
			"status":            "PENDING",
			"flow":              "BUY",
			"depositAccountIds": bson.M{"$exists": true, "$ne": []primitive.ObjectID{}},
		}}},

		// Stage 2: Lookup accounts based on depositAccountIds
		{{Key: "$lookup", Value: bson.M{
			"from":         "accounts",
			"localField":   "depositAccountIds",
			"foreignField": "_id",
			"as":           "depositAccounts",
		}}},

		// Stage 3: Filter orders where any depositAccount's walletAddress equals our single address
		{{Key: "$match", Value: bson.M{
			"depositAccounts.walletAddress": address, // Changed from $in operator to direct equality
		}}},
	}

	cursor, err := db.Collection("orders").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}

// getTronAccountBalance gets the TRX balance for a TRON account
func getTronAccountBalance(ctx context.Context, apiURL string, apiKey string, walletAddress string, client *http2.Client, logger logger.Logger) (float64, error) {
	// For TRX native balance, we need to get the account details
	url := fmt.Sprintf("%s/v1/accounts/%s", apiURL, walletAddress)

	logger.Info("Requesting TRON account details", "url", url)

	req, err := http2.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("error creating request: %w", err)
	}

	// Add API key header if available
	if apiKey != "" {
		req.Header.Set("TRON-PRO-API-KEY", apiKey)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http2.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("unexpected status code: %d, response: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read and parse response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("error reading response body: %w", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return 0, fmt.Errorf("error parsing response: %w", err)
	}

	// Extract balance - expecting TRX value in SUN units
	balanceData, ok := data["data"].([]interface{})
	if !ok || len(balanceData) == 0 {
		logger.Debug("No account data found, assuming zero balance", "address", walletAddress)
		return 0, nil
	}

	accountInfo, ok := balanceData[0].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid account data format")
	}

	balanceValue, ok := accountInfo["balance"].(float64)
	if !ok {
		// Try to parse as string
		if balanceStr, ok := accountInfo["balance"].(string); ok {
			if parsed, err := strconv.ParseFloat(balanceStr, 64); err == nil {
				balanceValue = parsed
			} else {
				logger.Warn("Could not parse balance value", "balance", balanceStr)
				balanceValue = 0
			}
		} else {
			logger.Warn("Balance not found in account data", "account", accountInfo)
			balanceValue = 0
		}
	}

	// Convert from SUN to TRX (1 TRX = 1,000,000 SUN)
	balanceTRX := balanceValue / 1000000.0

	logger.Debug("Got TRON balance",
		"address", walletAddress,
		"balance", balanceTRX)

	return balanceTRX, nil
}

// processNativeTransactions gets and processes native TRX transactions for an account
func processNativeTransactions(
	ctx context.Context,
	apiURL string,
	apiKey string,
	walletAddress string,
	userId primitive.ObjectID,
	account map[string]interface{},
	client *http2.Client,
	logger logger.Logger,
	db *mongo.Database,
	accountUsecase *usecases.AccountUseCase,
) error {
	// Get native TRX transactions from the API
	url := fmt.Sprintf("%s/v1/accounts/%s/transactions?only_confirmed=true&limit=200&order_by=block_timestamp,desc", apiURL, walletAddress)

	logger.Debug("Requesting TRON native transactions", "url", url)

	req, err := http2.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	// Add API key header if available
	if apiKey != "" {
		req.Header.Set("TRON-PRO-API-KEY", apiKey)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http2.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, response: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read and parse response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response body: %w", err)
	}

	var response TronTransactionResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("error parsing response: %w", err)
	}

	logger.Info("Got TRON native transactions",
		"address", walletAddress,
		"count", len(response.Data),
		"success", response.Success)

	// Filter for TransferContract transactions (native TRX transfers)
	var trxTransactions []TronTransaction
	for _, tx := range response.Data {
		if tx.RawData != nil && len(tx.RawData.Contract) > 0 {
			// We're only interested in TransferContract transactions
			if tx.RawData.Contract[0].Type == "TransferContract" {
				// Parse the contract parameter value
				var paramValue struct {
					Value struct {
						OwnerAddress string `json:"owner_address"`
						ToAddress    string `json:"to_address"`
						Amount       int64  `json:"amount"`
					} `json:"value"`
				}

				if err := json.Unmarshal(tx.RawData.Contract[0].Parameter, &paramValue); err == nil {
					// Set the values in our struct
					tx.RawData.Contract[0].ParameterValue.OwnerAddress = paramValue.Value.OwnerAddress
					tx.RawData.Contract[0].ParameterValue.ToAddress = paramValue.Value.ToAddress
					tx.RawData.Contract[0].ParameterValue.Amount = paramValue.Value.Amount

					// Only include if amount is non-zero
					if paramValue.Value.Amount != 0 {
						trxTransactions = append(trxTransactions, tx)
					}
				}
			}
		}
	}

	if len(trxTransactions) == 0 {
		logger.Info("No relevant TRX transfer transactions found")
		return nil
	}

	logger.Info("Processing TRX transfer transactions", "count", len(trxTransactions))

	// Process each transaction
	return processTransactionBatch(ctx, trxTransactions, walletAddress, userId, account, logger, db, accountUsecase)
}

// processTokenTransactions gets and processes TRC20 token transactions for an account
func processTokenTransactions(
	ctx context.Context,
	apiURL string,
	apiKey string,
	walletAddress string,
	tokenAddress string,
	userId primitive.ObjectID,
	account map[string]interface{},
	client *http2.Client,
	logger logger.Logger,
	db *mongo.Database,
	accountUsecase *usecases.AccountUseCase,
) error {
	// Get TRC20 token transactions from the API
	url := fmt.Sprintf("%s/v1/accounts/%s/transactions/trc20?only_confirmed=true&limit=30&order_by=block_timestamp,desc", apiURL, walletAddress)

	logger.Debug("Requesting TRON token transactions", "url", url)

	req, err := http2.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	// Add API key header if available
	if apiKey != "" {
		req.Header.Set("TRON-PRO-API-KEY", apiKey)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http2.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, response: %s", resp.StatusCode, string(bodyBytes))
	}

	// Read and parse response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response body: %w", err)
	}

	var response TronTransactionResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("error parsing response: %w", err)
	}

	logger.Info("Got TRON token transactions",
		"address", walletAddress,
		"count", len(response.Data),
		"success", response.Success)

	if len(response.Data) == 0 {
		logger.Info("No token transactions found")
		return nil
	}

	// Filter for transactions related to our specific token address
	var tokenTransactions []TronTransaction
	for _, tx := range response.Data {
		if strings.EqualFold(tx.ContractAddress, tokenAddress) {
			tokenTransactions = append(tokenTransactions, tx)
		}
	}

	if len(tokenTransactions) == 0 {
		logger.Info("No matching token transactions found for token address", "tokenAddress", tokenAddress)
		return nil
	}

	logger.Info("Processing token transactions", "count", len(tokenTransactions), "tokenAddress", tokenAddress)

	// Process each transaction
	return processTransactionBatch(ctx, tokenTransactions, walletAddress, userId, account, logger, db, accountUsecase)
}

// processTransactionBatch processes a batch of transactions and stores them in the database
func processTransactionBatch(
	ctx context.Context,
	transactions []TronTransaction,
	walletAddress string,
	userId primitive.ObjectID,
	account map[string]interface{},
	logger logger.Logger,
	db *mongo.Database,
	accountUsecase *usecases.AccountUseCase,
) error {
	transactionsCollection := db.Collection("transactions")

	for _, tx := range transactions {
		// Check if this transaction is already in the database
		count, err := transactionsCollection.CountDocuments(ctx, bson.M{"signature": tx.TxID})
		if err != nil {
			logger.Error("Failed to check if transaction exists", "error", err)
			continue
		}

		if count > 0 {
			logger.Debug("Transaction already processed", "txid", tx.TxID)
			continue
		}

		// Get transaction details (from/to addresses and amount)
		txDetails := extractTransactionDetails(tx)
		if txDetails.Amount <= 0 {
			logger.Debug("Skipping transaction with zero or negative amount", "txid", tx.TxID)
			continue
		}

		// Get accountId and chainId as primitive.ObjectID
		var accountId, chainId, currencyId primitive.ObjectID

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

		if id, ok := account["currencyId"].(primitive.ObjectID); ok {
			currencyId = id
		} else if idStr, ok := account["currencyId"].(string); ok {
			if oid, err := primitive.ObjectIDFromHex(idStr); err == nil {
				currencyId = oid
			}
		}

		//create account for receiverId using accountUsecase
		receiverAccount, err := accountUsecase.CreateOrFindAccount(ctx, userId, chainId, txDetails.ToAddr)
		if err != nil {
			logger.Error("Failed to create receiver account", "error", err)
			continue
		}

		// Create transaction model
		transaction := models.Transaction{
			ID:          primitive.NewObjectID(),
			InitiatorId: userId,
			SenderId:    accountId,
			CurrencyID:  currencyId,
			Hash:        tx.TxID,
			ReceiverId:  receiverAccount.ID,
			// FromAddress: txDetails.FromAddr,
			// ToAddress:   txDetails.ToAddr,
			Type:      "WALLET",
			Amount:    txDetails.Amount,
			ChainID:   chainId,
			Success:   true, // Only confirmed transactions are included
			Direction: determineTransactionType(txDetails.FromAddr, walletAddress),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		// Insert into database
		_, err = transactionsCollection.InsertOne(ctx, transaction)
		if err != nil {
			logger.Error("Failed to store transaction", "error", err, "txid", tx.TxID)
			continue
		}

		logger.Info("Stored new transaction",
			"txid", tx.TxID,
			"type", transaction.Type,
			"amount", txDetails.Amount)
	}

	return nil
}

// extractTransactionDetails extracts the key details from a TRON transaction
func extractTransactionDetails(tx TronTransaction) *TronTransactionDetails {
	details := &TronTransactionDetails{
		Hash:      tx.TxID,
		BlockTime: tx.BlockTimeStamp,
		Status:    "success", // Only confirmed transactions are processed
	}

	// Handle different transaction types
	if tx.ContractAddress != "" {
		// This is a TRC20 token transaction
		details.TokenAddr = tx.ContractAddress
		details.FromAddr = tx.From
		details.ToAddr = tx.To

		// Parse value (string) to float64
		if tx.Value != "" {
			if amount, err := strconv.ParseFloat(tx.Value, 64); err == nil {
				// TRC20 tokens might have different decimals, we'll assume 6 if not provided
				decimals := 6
				// Convert to proper decimal
				details.Amount = amount / math.Pow10(decimals)
			}
		}
	} else if tx.RawData != nil && len(tx.RawData.Contract) > 0 {
		// This is a native TRX transfer
		contract := tx.RawData.Contract[0]

		if contract.Type == "TransferContract" {
			// These are already parsed during filtering
			details.FromAddr = contract.ParameterValue.OwnerAddress
			details.ToAddr = contract.ParameterValue.ToAddress

			// Convert from SUN to TRX (1 TRX = 1,000,000 SUN)
			details.Amount = float64(contract.ParameterValue.Amount) / 1000000.0
		}
	}

	return details
}

// determineTransactionType determines if a transaction is SEND or RECEIVE based on addresses
func determineTransactionType(from, walletAddress string) string {
	if strings.EqualFold(from, walletAddress) {
		return "SEND"
	}
	return "RECEIVE"
}
