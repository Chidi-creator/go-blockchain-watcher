package sol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	http2 "net/http"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

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

type SolanaAccountInfo struct {
	Jsonrpc string `json:"jsonrpc"`
	Result  struct {
		Context struct {
			Slot int64 `json:"slot"`
		} `json:"context"`
		Value struct {
			Data       []string `json:"data"`
			Executable bool     `json:"executable"`
			Lamports   int64    `json:"lamports"`
			Owner      string   `json:"owner"`
			RentEpoch  int64    `json:"rentEpoch"`
		} `json:"value"`
	} `json:"result"`
	ID int `json:"id"`
}

// SolanaRpcRequest represents a Solana JSON-RPC request
type SolanaRpcRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params,omitempty"`
}

// SolanaTransactionResponse represents a Solana transaction response
type SolanaTransactionResponse struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  struct {
		Meta        interface{} `json:"meta,omitempty"` // Transaction meta information
		Transaction struct {
			Message struct {
				AccountKeys  []string    `json:"accountKeys"` // All accounts involved in the transaction
				Header       interface{} `json:"header,omitempty"`
				Instructions []struct {
					ProgramIdIndex int    `json:"programIdIndex"`
					Accounts       []int  `json:"accounts"`
					Data           string `json:"data,omitempty"`
				} `json:"instructions,omitempty"`
				RecentBlockhash string `json:"recentBlockhash,omitempty"`
			} `json:"message"`
			Signatures []string `json:"signatures,omitempty"`
		} `json:"transaction"`
		BlockTime int64 `json:"blockTime,omitempty"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// TransactionDetails represents Solana transaction details
type TransactionDetails struct {
	Signature   string
	BlockTime   int64
	AccountKeys []string
	Status      string
	FromAddress string
	ToAddress   string
	Amount      float64
}

// Worker represents the ChangeNow worker service
type Worker struct {
	cfg              *config.Config
	solConcurrency   int
	logger           logger.Logger
	redisClient      *redis.Client
	queueSvc         *services.QueueService
	mongoClient      *mongo.Client
	cacheManager     *cache.CacheManager
	accountUsecase   *usecases.AccountUseCase
	client           *http.RequestManager
	solScheduledTime int
}

// New creates a new ChangeNow worker
func New(cfg *config.Config, concurrency int, logger logger.Logger, redisClient *redis.Client, queueSvc *services.QueueService, mongoClient *mongo.Client, cacheManager *cache.CacheManager, accountUsecase *usecases.AccountUseCase, requestManager *http.RequestManager, solScheduledTime int) worker.Worker {
	return &Worker{
		cfg:              cfg,
		solConcurrency:   concurrency,
		logger:           logger,
		redisClient:      redisClient,
		queueSvc:         queueSvc,
		mongoClient:      mongoClient,
		cacheManager:     cacheManager,
		accountUsecase:   accountUsecase,
		client:           requestManager,
		solScheduledTime: solScheduledTime,
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

	// Get database
	db := w.mongoClient.Database(w.cfg.MongoDB.Database)

	queueName := constants.QueueNames["QueueSolanaBlockMonitor"]

	w.logger.Info("Using queue for Solana watcher", "queueName", queueName)

	// Register Solana watcher handler
	queueManager.RegisterHandler(queueName, func(ctx context.Context, data map[string]interface{}) error {
		w.logger.Debug("Handler invoked", "data", data)
		return processSolanaAccounts(ctx, db, w.cfg.Chains.Solana.RPCEndpoint, http2.DefaultClient, w.logger, w.client, w.accountUsecase)
	})

	// Start processing jobs
	w.logger.Info("Starting to process Solana block monitor jobs", "concurrency", w.solConcurrency)
	err := queueManager.ProcessJobs(ctx, queueName, w.solConcurrency)
	if err != nil {
		w.logger.Error("Failed to start processing jobs", "error", err)
		return err
	}

	// Start health checker for Redis
	go startScheduler(ctx, queueName, w.redisClient, w.logger, w.solScheduledTime)

	// Wait for context to be done
	<-ctx.Done()
	w.logger.Info("Solana worker shutting down")
	return nil
}

func startScheduler(ctx context.Context, queueName string, redisClient *redis.Client, logger logger.Logger, scheduledTime int) {
	ticker := time.NewTicker(time.Duration(scheduledTime) * time.Minute)
	defer ticker.Stop()

	logger.Info("Starting scheduler for Solana processing")

	// Run immediately on startup
	if err := enqueueSolJob(ctx, queueName, redisClient); err != nil {
		logger.Error("Failed to enqueue initial Solana job", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping scheduler")
			return
		case <-ticker.C:
			if err := enqueueSolJob(ctx, queueName, redisClient); err != nil {
				logger.Error("Failed to enqueue Solana job", "error", err)
			}
		}
	}
}

func enqueueSolJob(ctx context.Context, queueName string, redisClient *redis.Client) error {
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

func processSolanaAccounts(
	ctx context.Context,
	db *mongo.Database,
	rpcURL string,
	client *http2.Client,
	logger logger.Logger,
	http *http.RequestManager,
	accountUsecase *usecases.AccountUseCase,
) error {
	logger.Info("Starting Solana accounts processing")

	// Get all Solana accounts
	accounts, err := getActiveAccountsByChainId(ctx, db, "SOL", logger)
	if err != nil {
		logger.Error("Failed to get Solana accounts", "error", err)
		return err
	}

	logger.Info("Retrieved Solana accounts", "count", len(accounts))

	// Process account balances and transactions in parallel
	err = processAccountBalancesAndTransactions(ctx, accounts, rpcURL, client, logger, db, http, accountUsecase)
	if err != nil {
		logger.Error("Failed to process accounts", "error", err)
		return err
	}

	logger.Info("Solana accounts processing completed")
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
// of a specific chain in parallel
func processAccountBalancesAndTransactions(
	ctx context.Context,
	accounts []map[string]interface{},
	rpcURL string,
	client *http2.Client,
	logger logger.Logger,
	db *mongo.Database,
	http *http.RequestManager,
	accountUsecase *usecases.AccountUseCase,
) error {
	accountsCount := len(accounts)
	logger.Info("Processing accounts balances and transactions", "count", accountsCount)

	if accountsCount == 0 {
		logger.Info("No accounts to process")
		return nil
	}

	// Use a worker pool to process accounts in parallel
	maxWorkers := 5 // Adjust based on rate limits and performance
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
				err := processAccountBalanceAndTransactions(ctx, account, rpcURL, client, logger, db, http, accountUsecase)
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

	// Return error if any account failed
	if errorCount > 0 {
		return fmt.Errorf("failed to process %d of %d accounts", errorCount, accountsCount)
	}
	return nil
}

// processAccountBalanceAndTransactions processes a single account's balance and transaction history
func processAccountBalanceAndTransactions(
	ctx context.Context,
	account map[string]interface{},
	rpcURL string,
	client *http2.Client,
	logger logger.Logger,
	db *mongo.Database,
	http *http.RequestManager,
	accountUsecase *usecases.AccountUseCase,
) error {
	walletAddress, ok := account["walletAddress"].(string)
	if !ok || walletAddress == "" {
		return fmt.Errorf("invalid wallet address for account %v", account["_id"])
	}

	userId, ok := account["userId"].(primitive.ObjectID)
	if !ok {
		return fmt.Errorf("invalid userId for account %v", account["_id"])
	}

	//get SupportedCurrency by chainId
	supportedCurrency, err := getSupportedCurrency(ctx, db, account, logger)
	if err != nil {
		logger.Error("Failed to get supported currency",
			"error", err)
		return err
	}

	fmt.Println("supportedCurrency", supportedCurrency["_id"], supportedCurrency["chainId"])

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
		_, err := ProcessWatchedAddress(ctx, walletAddress, watchedOrders, http, logger)

		if err != nil {
			logger.Error("Failed to process watched address",
				"walletAddress", walletAddress,
				"error", err)
		}
	}

	// 1. Get account balance
	balance, err := getSolanaAccountBalance(ctx, rpcURL, walletAddress, client, logger, supportedCurrency)
	if err != nil {
		logger.Error("Failed to get account balance",
			"walletAddress", walletAddress,
			"error", err)
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

	// Extract accountId safely but we won't use it for cache in this function
	// as we don't have access to the cacheManager here
	if _, idOk := account["_id"].(primitive.ObjectID); !idOk {
		logger.Warn("Failed to get account ID", "account", account["_id"])
		// We can continue since this only affects cache clearing
	}

	// Note: Cache expiration should be handled at a higher level where cacheManager is available

	logger.Info("Updated account balance",
		"accountId", account["_id"],
		"walletAddress", walletAddress,
		"balance", balance,
		"modified", updateResult.ModifiedCount)

	// 2. Get transaction history and process it
	// We'll implement this with a separate function
	err = processTransactionHistory(ctx, rpcURL, walletAddress, userId, account, client, logger, db, accountUsecase, supportedCurrency)
	if err != nil {
		logger.Error("Failed to process transaction history",
			"walletAddress", walletAddress,
			"error", err)
		return err
	}

	return nil
}

// getSolanaAccountBalance gets the SOL balance for an account
func getSolanaAccountBalance(
	ctx context.Context,
	rpcURL string,
	walletAddress string,
	client *http2.Client,
	logger logger.Logger,
	supportedCurrency map[string]interface{},
) (float64, error) {
	// Check if tokenAddress exists and is not empty
	tokenAddress, _ := supportedCurrency["tokenAddress"].(string)

	// If it's not a string, try to convert it to a string
	if tokenAddress == "" && supportedCurrency["tokenAddress"] != nil {
		tokenAddress = fmt.Sprintf("%v", supportedCurrency["tokenAddress"])
	}

	// Check if it's a valid non-empty token address
	if tokenAddress != "" && tokenAddress != "<nil>" && tokenAddress != "null" {
		logger.Debug("Token address found, getting token balance", "tokenAddress", tokenAddress)
		return getSolanaTokenBalance(ctx, rpcURL, walletAddress, client, logger, supportedCurrency)
	}

	// If no valid token address, get native SOL balance
	logger.Debug("No token address provided or empty token address, getting native SOL balance")

	// Create request to get balance
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getBalance",
		"params": []interface{}{
			walletAddress,
			map[string]string{
				"commitment": "confirmed",
			},
		},
	}

	// Extract API key if needed
	apiKey := ""
	if strings.Contains(rpcURL, "/solana/") && strings.Count(rpcURL, "/") > 3 {
		parts := strings.Split(rpcURL, "/")
		if len(parts) > 4 && parts[len(parts)-1] != "" {
			apiKey = parts[len(parts)-1]
		}
	} else {
		apiKey = os.Getenv("SOLANA_ANKR_API_KEY")
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Send request
	req, err := http2.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return 0, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}

	// Implement retry logic
	maxRetries := 3
	var resp *http2.Response
	var responseBody []byte

	for retry := 0; retry <= maxRetries; retry++ {
		if retry > 0 {
			backoffTime := time.Duration(retry*retry) * 500 * time.Millisecond
			logger.Info("Retrying Solana RPC request", "retry", retry, "backoff", backoffTime)
			time.Sleep(backoffTime)
		}

		resp, err = client.Do(req)
		if err != nil {
			if retry == maxRetries {
				return 0, fmt.Errorf("failed to make RPC request (after %d retries): %w", retry, err)
			}
			continue
		}

		responseBody, err = io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			if retry == maxRetries {
				return 0, fmt.Errorf("failed to read response body (after %d retries): %w", retry, err)
			}
			continue
		}

		if resp.StatusCode != http2.StatusOK {
			if retry == maxRetries {
				return 0, fmt.Errorf("unexpected status code: %d (after %d retries)", resp.StatusCode, retry)
			}
			continue
		}

		break
	}

	// Parse response
	var rpcResp struct {
		Result struct {
			Value int64 `json:"value"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(responseBody, &rpcResp); err != nil {
		return 0, fmt.Errorf("failed to decode RPC response: %w", err)
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		return 0, fmt.Errorf("RPC error: %s (code: %d)", rpcResp.Error.Message, rpcResp.Error.Code)
	}

	// Convert from lamports to SOL (1 SOL = 1,000,000,000 lamports)
	// Solana has 9 decimal places
	solDecimals := 9
	balanceSOL := float64(rpcResp.Result.Value) / math.Pow10(solDecimals)

	logger.Debug("Got account balance", "address", walletAddress, "balance", balanceSOL)
	return balanceSOL, nil
}

func getSolanaTokenBalance(ctx context.Context, rpcURL string, walletAddress string, client *http2.Client, logger logger.Logger, supportedCurrency map[string]interface{}) (float64, error) {
	// Extract token address from supportedCurrency
	tokenAddress, ok := supportedCurrency["tokenAddress"].(string)
	if !ok {
		// Try to handle non-string tokenAddress values
		if tokenAddress, ok = fmt.Sprintf("%v", supportedCurrency["tokenAddress"]), true; ok {
			if tokenAddress == "<nil>" || tokenAddress == "" {
				return 0, fmt.Errorf("token address is empty in supported currency data")
			}
		} else {
			return 0, fmt.Errorf("invalid token address format in supported currency data")
		}
	}

	// Additional validation for empty token address
	if tokenAddress == "" {
		return 0, fmt.Errorf("token address is empty in supported currency data")
	}

	logger.Debug("Getting SPL token balance",
		"walletAddress", walletAddress,
		"tokenAddress", tokenAddress)

	// First, we need to get the associated token account for this wallet and token
	tokenAccountAddress, err := getAssociatedTokenAccount(ctx, rpcURL, walletAddress, tokenAddress, client, logger)
	if err != nil {
		return 0, fmt.Errorf("failed to get associated token account: %w", err)
	}

	// If no token account found, balance is 0
	if tokenAccountAddress == "" {
		logger.Info("No token account found for wallet, balance is 0",
			"walletAddress", walletAddress,
			"tokenAddress", tokenAddress)
		return 0, nil
	}

	// Now get the balance of the token account
	// Create request to get token balance
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTokenAccountBalance",
		"params": []interface{}{
			tokenAccountAddress,
		},
	}

	// Extract API key if needed
	apiKey := ""
	if strings.Contains(rpcURL, "/solana/") && strings.Count(rpcURL, "/") > 3 {
		parts := strings.Split(rpcURL, "/")
		if len(parts) > 4 && parts[len(parts)-1] != "" {
			apiKey = parts[len(parts)-1]
		}
	} else {
		apiKey = os.Getenv("SOLANA_ANKR_API_KEY")
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Send request
	req, err := http2.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return 0, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}

	// Implement retry logic
	maxRetries := 3
	var resp *http2.Response
	var responseBody []byte

	for retry := 0; retry <= maxRetries; retry++ {
		if retry > 0 {
			backoffTime := time.Duration(retry*retry) * 500 * time.Millisecond
			logger.Info("Retrying Solana RPC request", "retry", retry, "backoff", backoffTime)
			time.Sleep(backoffTime)
		}

		resp, err = client.Do(req)
		if err != nil {
			if retry == maxRetries {
				return 0, fmt.Errorf("failed to make RPC request (after %d retries): %w", retry, err)
			}
			continue
		}

		responseBody, err = io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			if retry == maxRetries {
				return 0, fmt.Errorf("failed to read response body (after %d retries): %w", retry, err)
			}
			continue
		}

		if resp.StatusCode != http2.StatusOK {
			if retry == maxRetries {
				return 0, fmt.Errorf("unexpected status code: %d (after %d retries)", resp.StatusCode, retry)
			}
			continue
		}

		break
	}

	// Parse response
	var rpcResp struct {
		Result struct {
			Value struct {
				Amount   string  `json:"amount"`
				Decimals int     `json:"decimals"`
				UIAmount float64 `json:"uiAmount"`
			} `json:"value"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(responseBody, &rpcResp); err != nil {
		return 0, fmt.Errorf("failed to decode RPC response: %w", err)
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		return 0, fmt.Errorf("RPC error: %s (code: %d)", rpcResp.Error.Message, rpcResp.Error.Code)
	}

	// Return the UI amount which is already formatted with correct decimals
	balance := rpcResp.Result.Value.UIAmount

	logger.Debug("Got token balance",
		"address", walletAddress,
		"token", tokenAddress,
		"balance", balance)

	return balance, nil
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

// getAssociatedTokenAccount finds the associated token account for a wallet and token
func getAssociatedTokenAccount(ctx context.Context, rpcURL, walletAddress, tokenAddress string, client *http2.Client, logger logger.Logger) (string, error) {
	// Create request to get token accounts by owner
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTokenAccountsByOwner",
		"params": []interface{}{
			walletAddress,
			map[string]string{
				"mint": tokenAddress,
			},
			map[string]string{
				"encoding": "jsonParsed",
			},
		},
	}

	// Extract API key if needed
	apiKey := ""
	if strings.Contains(rpcURL, "/solana/") && strings.Count(rpcURL, "/") > 3 {
		parts := strings.Split(rpcURL, "/")
		if len(parts) > 4 && parts[len(parts)-1] != "" {
			apiKey = parts[len(parts)-1]
		}
	} else {
		apiKey = os.Getenv("SOLANA_ANKR_API_KEY")
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	// Send request
	req, err := http2.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}

	// Implement retry logic
	maxRetries := 3
	var resp *http2.Response
	var responseBody []byte

	for retry := 0; retry <= maxRetries; retry++ {
		if retry > 0 {
			backoffTime := time.Duration(retry*retry) * 500 * time.Millisecond
			logger.Info("Retrying Solana RPC request", "retry", retry, "backoff", backoffTime)
			time.Sleep(backoffTime)
		}

		resp, err = client.Do(req)
		if err != nil {
			if retry == maxRetries {
				return "", fmt.Errorf("failed to make RPC request (after %d retries): %w", retry, err)
			}
			continue
		}

		responseBody, err = io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			if retry == maxRetries {
				return "", fmt.Errorf("failed to read response body (after %d retries): %w", retry, err)
			}
			continue
		}

		if resp.StatusCode != http2.StatusOK {
			if retry == maxRetries {
				return "", fmt.Errorf("unexpected status code: %d (after %d retries)", resp.StatusCode, retry)
			}
			continue
		}

		break
	}

	// Parse response
	var rpcResp struct {
		Result struct {
			Value []struct {
				Pubkey string `json:"pubkey"`
			} `json:"value"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(responseBody, &rpcResp); err != nil {
		return "", fmt.Errorf("failed to decode RPC response: %w", err)
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		return "", fmt.Errorf("RPC error: %s (code: %d)", rpcResp.Error.Message, rpcResp.Error.Code)
	}

	// Extract the token account address
	if len(rpcResp.Result.Value) == 0 {
		// No token account found, which is normal if user hasn't interacted with this token
		return "", nil
	}

	// Return the first token account address
	return rpcResp.Result.Value[0].Pubkey, nil
}

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
		// For diagnostic purposes only, try to list some documents in the collection
		diagCursor, _ := supportedCurrenciesCollection.Find(ctx, bson.M{}, options.Find().SetLimit(3))
		if diagCursor != nil {
			defer diagCursor.Close(ctx)
			var samples []map[string]interface{}
			if err := diagCursor.All(ctx, &samples); err == nil && len(samples) > 0 {
				logger.Info("No matching documents found. Sample documents in collection:", "count", len(samples))
				for i, doc := range samples {
					// Extract string rep of ObjectIDs for comparison
					var chainIdHex, currencyIdHex string
					if oid, ok := doc["chainId"].(primitive.ObjectID); ok {
						chainIdHex = oid.Hex()
					}
					if oid, ok := doc["currencyId"].(primitive.ObjectID); ok {
						currencyIdHex = oid.Hex()
					}

					logger.Info("Sample document",
						"index", i,
						"chainIdHex", chainIdHex,
						"currencyIdHex", currencyIdHex)

					// If this document has the same chainId as our query, log more details
					if chainIdHex == chainIdObj.Hex() {
						logger.Info("Found document with matching chainId but wasn't returned in query",
							"currencyIdHex", currencyIdHex,
							"queryCurrencyIdHex", func() string {
								if hasCurrencyId {
									return currencyIdObj.Hex()
								}
								return "not used in query"
							}())
					}
				}
			} else {
				logger.Warn("No documents found in supportedCurrencies collection")
			}
		}

		// Return default empty document
		logger.Warn("No supported currency found for the provided IDs, using default")
		return map[string]interface{}{
			"tokenAddress": "",
			"chainId":      chainIdObj,
		}, nil
	}

	// Return the first matching currency
	supportedCurrency := supportedCurrencies[0]

	// Ensure tokenAddress exists
	if _, exists := supportedCurrency["tokenAddress"]; !exists {
		supportedCurrency["tokenAddress"] = ""
	}

	logger.Info("Found matching supported currency",
		"id", supportedCurrency["_id"],
		"tokenAddress", supportedCurrency["tokenAddress"])

	return supportedCurrency, nil
}

// Helper function to create a pointer to int64
// func ptr(i int64) *int64 {
// 	return &i
// }

// processTransactionHistory gets recent transactions for an account and processes them
func processTransactionHistory(
	ctx context.Context,
	rpcURL string,
	walletAddress string,
	userId primitive.ObjectID,
	account map[string]interface{},
	client *http2.Client,
	logger logger.Logger,
	db *mongo.Database,
	accountUsecase *usecases.AccountUseCase,
	supportedCurrency map[string]interface{},
) error {
	// Get recent transactions for the address
	signatures, err := getRecentTransactionSignatures(ctx, rpcURL, walletAddress, client, logger)
	if err != nil {
		return fmt.Errorf("failed to get recent transactions: %w", err)
	}

	logger.Info("Got recent transaction signatures",
		"address", walletAddress,
		"count", len(signatures))

	// Process each transaction
	// We'll limit to 10 most recent for performance
	maxToProcess := 10
	if len(signatures) > maxToProcess {
		signatures = signatures[:maxToProcess]
	}

	for i, signature := range signatures {
		// Process transaction details and save to DB if needed
		err := processTransactionDetails(ctx, rpcURL, signature, walletAddress, userId, account, client, logger, db, accountUsecase, supportedCurrency)
		if err != nil {
			logger.Warn("Failed to process transaction",
				"signature", signature,
				"index", i,
				"error", err)
			// Continue with next transaction
			continue
		}
	}

	return nil
}

// getRecentTransactionSignatures gets recent transaction signatures for an account
func getRecentTransactionSignatures(
	ctx context.Context,
	rpcURL string,
	walletAddress string,
	client *http2.Client,
	logger logger.Logger,
) ([]string, error) {
	// Extract API key from URL or environment
	var apiKey string
	var baseURL string

	// Check if API key is in the URL
	if strings.Contains(rpcURL, "/solana/") && strings.Count(rpcURL, "/") > 3 {
		parts := strings.Split(rpcURL, "/")
		if len(parts) > 4 && parts[len(parts)-1] != "" {
			// Last part is potentially the API key
			apiKey = parts[len(parts)-1]
			// Remove API key from URL to use in header instead
			baseURL = strings.Join(parts[:len(parts)-1], "/")
		} else {
			baseURL = rpcURL
		}
	} else {
		baseURL = rpcURL
	}

	// If still no API key, try environment variable
	if apiKey == "" {
		apiKey = os.Getenv("SOLANA_ANKR_API_KEY")
	}

	// Create request to get recent transactions
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getSignaturesForAddress",
		"params": []interface{}{
			walletAddress,
			map[string]interface{}{
				"limit": 20, // Limit to 20 most recent
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Send request to the baseURL
	req, err := http2.NewRequestWithContext(ctx, "POST", baseURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}

	// Implement retry logic
	maxRetries := 3
	var resp *http2.Response
	var responseBody []byte

	for retry := 0; retry <= maxRetries; retry++ {
		if retry > 0 {
			backoffTime := time.Duration(retry*retry) * 500 * time.Millisecond
			logger.Info("Retrying Solana RPC request", "retry", retry, "backoff", backoffTime)
			time.Sleep(backoffTime)
		}

		resp, err = client.Do(req)
		if err != nil {
			if retry == maxRetries {
				return nil, fmt.Errorf("failed to make RPC request (after %d retries): %w", retry, err)
			}
			continue
		}

		responseBody, err = io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			if retry == maxRetries {
				return nil, fmt.Errorf("failed to read response body (after %d retries): %w", retry, err)
			}
			continue
		}

		// Special handling for 401/403 authentication errors
		if resp.StatusCode == 401 || resp.StatusCode == 403 {
			// Log the error details
			errorMsg := string(responseBody)
			if len(errorMsg) > 200 {
				errorMsg = errorMsg[:200] + "..."
			}
			logger.Error("Authentication failed with Ankr API",
				"statusCode", resp.StatusCode,
				"response", errorMsg,
				"wallet", walletAddress)

			// If this is our last retry, return error
			if retry == maxRetries {
				return nil, fmt.Errorf("authentication failed with Ankr API (status %d)", resp.StatusCode)
			}

			// On first auth error, try appending the API key to the URL instead of using header
			if retry == 0 && apiKey != "" {
				urlWithKey := fmt.Sprintf("%s/%s", baseURL, apiKey)
				logger.Info("Retrying with API key in URL instead of header", "retry", retry+1)

				// Create a new request with the API key in URL
				req, err = http2.NewRequestWithContext(ctx, "POST", urlWithKey, bytes.NewBuffer(reqBytes))
				if err != nil {
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				continue
			}

			continue
		}

		if resp.StatusCode != http2.StatusOK {
			if retry == maxRetries {
				return nil, fmt.Errorf("unexpected status code: %d (after %d retries)", resp.StatusCode, retry)
			}
			continue
		}

		break
	}

	// Parse response
	var rpcResp struct {
		Result []struct {
			Signature string `json:"signature"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(responseBody, &rpcResp); err != nil {
		return nil, fmt.Errorf("failed to decode RPC response: %w", err)
	}

	// Check for RPC error
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s (code: %d)", rpcResp.Error.Message, rpcResp.Error.Code)
	}

	// Extract signatures
	signatures := make([]string, 0, len(rpcResp.Result))
	for _, result := range rpcResp.Result {
		signatures = append(signatures, result.Signature)
	}

	return signatures, nil
}

// processTransactionDetails processes transaction details and stores them if needed
func processTransactionDetails(
	ctx context.Context,
	rpcURL string,
	signature string,
	walletAddress string,
	userId primitive.ObjectID,
	account map[string]interface{},
	client *http2.Client,
	logger logger.Logger,
	db *mongo.Database,
	accountUsecase *usecases.AccountUseCase,
	supportedCurrency map[string]interface{},
) error {
	// Check if transaction is already processed
	transactionsCollection := db.Collection("transactions")
	count, err := transactionsCollection.CountDocuments(ctx, bson.M{"signature": signature})
	if err != nil {
		return fmt.Errorf("failed to check if transaction exists: %w", err)
	}

	if count > 0 {
		logger.Debug("Transaction already processed", "signature", signature)
		return nil
	}

	// Get transaction details
	txDetails, err := getTransactionDetails(ctx, rpcURL, signature, client, logger)
	if err != nil {
		return fmt.Errorf("failed to get transaction details: %w", err)
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
	receiverAccount, err := accountUsecase.CreateOrFindAccount(ctx, userId, chainId, txDetails.ToAddress)
	if err != nil {
		logger.Error("Failed to create receiver account", "error", err)
		return fmt.Errorf("failed to create receiver account: %w", err)
	}

	// Create transaction model
	transaction := models.Transaction{
		ID:          primitive.NewObjectID(),
		InitiatorId: userId,
		SenderId:    accountId,
		Hash:        signature,
		CurrencyID:  supportedCurrency["currencyId"].(primitive.ObjectID),
		// FromAddress: txDetails.FromAddress,
		// ToAddress:   txDetails.ToAddress,
		ReceiverId: receiverAccount.ID,
		Amount:     txDetails.Amount,
		Type:       "WALLET",
		ChainID:    chainId,
		Success:    txDetails.Status == "success",
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	// Determine transaction type based on wallet's position
	if strings.EqualFold(walletAddress, txDetails.ToAddress) {
		transaction.Direction = "RECEIVE"
	} else {
		transaction.Direction = "SEND"
	}

	// Insert into database
	_, err = transactionsCollection.InsertOne(ctx, transaction)
	if err != nil {
		return fmt.Errorf("failed to store transaction: %w", err)
	}

	logger.Info("Stored new transaction",
		"signature", signature,
		"from", txDetails.FromAddress,
		"to", txDetails.ToAddress,
		"amount", txDetails.Amount,
		"wallet", walletAddress)
	return nil
}

// getTransactionDetails gets details for a specific transaction
func getTransactionDetails(
	ctx context.Context,
	rpcURL string,
	signature string,
	client *http2.Client,
	logger logger.Logger,
) (*TransactionDetails, error) {
	// Extract API key from URL or environment
	var apiKey string
	var baseURL string

	// Check if API key is in the URL
	if strings.Contains(rpcURL, "/solana/") && strings.Count(rpcURL, "/") > 3 {
		parts := strings.Split(rpcURL, "/")
		if len(parts) > 4 && parts[len(parts)-1] != "" {
			// Last part is potentially the API key
			apiKey = parts[len(parts)-1]
			// Remove API key from URL to use in header instead
			baseURL = strings.Join(parts[:len(parts)-1], "/")
		} else {
			baseURL = rpcURL
		}
	} else {
		baseURL = rpcURL
	}

	// If still no API key, try environment variable
	if apiKey == "" {
		apiKey = os.Getenv("SOLANA_ANKR_API_KEY")
	}

	// Create request to get transaction details
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTransaction",
		"params": []interface{}{
			signature,
			map[string]interface{}{
				"encoding":                       "json",
				"maxSupportedTransactionVersion": 0,
			},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Send request to the baseURL
	req, err := http2.NewRequestWithContext(ctx, "POST", baseURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}

	// Implement retry logic
	maxRetries := 3
	var resp *http2.Response
	var responseBody []byte

	for retry := 0; retry <= maxRetries; retry++ {
		if retry > 0 {
			backoffTime := time.Duration(retry*retry) * 500 * time.Millisecond
			logger.Info("Retrying Solana RPC request", "retry", retry, "backoff", backoffTime)
			time.Sleep(backoffTime)
		}

		resp, err = client.Do(req)
		if err != nil {
			if retry == maxRetries {
				return nil, fmt.Errorf("failed to make RPC request (after %d retries): %w", retry, err)
			}
			continue
		}

		responseBody, err = io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			if retry == maxRetries {
				return nil, fmt.Errorf("failed to read response body (after %d retries): %w", retry, err)
			}
			continue
		}

		// Special handling for 401/403 authentication errors
		if resp.StatusCode == 401 || resp.StatusCode == 403 {
			// Log the error details
			errorMsg := string(responseBody)
			if len(errorMsg) > 200 {
				errorMsg = errorMsg[:200] + "..."
			}
			logger.Error("Authentication failed with Ankr API",
				"statusCode", resp.StatusCode,
				"response", errorMsg)

			// If this is our last retry, return error
			if retry == maxRetries {
				return nil, fmt.Errorf("authentication failed with Ankr API (status %d)", resp.StatusCode)
			}

			// On first auth error, try appending the API key to the URL instead of using header
			if retry == 0 && apiKey != "" {
				urlWithKey := fmt.Sprintf("%s/%s", baseURL, apiKey)
				logger.Info("Retrying with API key in URL instead of header", "retry", retry+1)

				// Create a new request with the API key in URL
				req, err = http2.NewRequestWithContext(ctx, "POST", urlWithKey, bytes.NewBuffer(reqBytes))
				if err != nil {
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				continue
			}

			continue
		}

		if resp.StatusCode != http2.StatusOK {
			if retry == maxRetries {
				return nil, fmt.Errorf("unexpected status code: %d (after %d retries)", resp.StatusCode, retry)
			}
			continue
		}

		break
	}

	// Parse full transaction response to extract sender/receiver/amount
	var txResponse SolanaTransactionResponse
	if err := json.Unmarshal(responseBody, &txResponse); err != nil {
		return nil, fmt.Errorf("failed to decode transaction response: %w", err)
	}

	// Check for RPC error
	if txResponse.Error != nil {
		return nil, fmt.Errorf("RPC error: %s (code: %d)", txResponse.Error.Message, txResponse.Error.Code)
	}

	// Extract transaction details
	txDetails := &TransactionDetails{
		Signature: signature,
		BlockTime: txResponse.Result.BlockTime,
		Status:    "success",
	}

	// Check if transaction failed
	if txResponse.Result.Meta != nil {
		// In Solana, if Meta.Err is not null, the transaction failed
		if err, ok := txResponse.Result.Meta.(map[string]interface{}); ok && err["err"] != nil {
			txDetails.Status = "failed"
		}
	}

	// Extract account keys if available
	if txResponse.Result.Transaction.Message.AccountKeys != nil {
		txDetails.AccountKeys = txResponse.Result.Transaction.Message.AccountKeys
	}

	// Try to extract from/to addresses and amount
	fromAddress, toAddress, amount := extractTransferDetails(txResponse)
	txDetails.FromAddress = fromAddress
	txDetails.ToAddress = toAddress
	txDetails.Amount = amount

	logger.Debug("Extracted transaction details",
		"signature", signature,
		"from", fromAddress,
		"to", toAddress,
		"amount", amount,
		"status", txDetails.Status)

	return txDetails, nil
}

// extractTransferDetails attempts to extract the sender, receiver and amount from a transaction
func extractTransferDetails(txResponse SolanaTransactionResponse) (string, string, float64) {
	var fromAddress, toAddress string
	var amount float64

	// Default from address is usually the fee payer (first account)
	if len(txResponse.Result.Transaction.Message.AccountKeys) > 0 {
		fromAddress = txResponse.Result.Transaction.Message.AccountKeys[0]
	}

	// Try to extract more detailed info from the transaction
	// This is a simplified approach - full parsing would require much more complex logic
	if txResponse.Result.Transaction.Message.Instructions != nil {
		for _, instruction := range txResponse.Result.Transaction.Message.Instructions {
			// Check program ID index - System Program (for native SOL transfers)
			// is usually at index 2 for simple transfers
			programIndex := instruction.ProgramIdIndex
			if programIndex >= 0 && programIndex < len(txResponse.Result.Transaction.Message.AccountKeys) {
				programId := txResponse.Result.Transaction.Message.AccountKeys[programIndex]

				// System Program ID for Solana
				if programId == "11111111111111111111111111111111" {
					// For native SOL transfers via system program
					// The accounts array typically has [sender, receiver]
					if len(instruction.Accounts) >= 2 {
						fromIdx := instruction.Accounts[0]
						toIdx := instruction.Accounts[1]

						if fromIdx < len(txResponse.Result.Transaction.Message.AccountKeys) &&
							toIdx < len(txResponse.Result.Transaction.Message.AccountKeys) {
							// Update sender/receiver
							fromAddress = txResponse.Result.Transaction.Message.AccountKeys[fromIdx]
							toAddress = txResponse.Result.Transaction.Message.AccountKeys[toIdx]

							// Try to extract amount from pre/post balance changes
							// This is a very simplified approach
							if txResponse.Result.Meta != nil {
								if meta, ok := txResponse.Result.Meta.(map[string]interface{}); ok {
									if preBalances, ok := meta["preBalances"].([]interface{}); ok {
										if postBalances, ok := meta["postBalances"].([]interface{}); ok {
											if len(preBalances) > fromIdx && len(postBalances) > fromIdx {
												preBalance, _ := preBalances[fromIdx].(float64)
												postBalance, _ := postBalances[fromIdx].(float64)

												// Rough estimation of transfer amount (minus fees)
												if preBalance > postBalance {
													// Convert from lamports to SOL (1 SOL = 1,000,000,000 lamports)
													amount = (preBalance - postBalance) / 1_000_000_000
												}
											}
										}
									}
								}
							}
						}
					}
				} else if programId == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" {
					// SPL Token Program for token transfers
					// Simplistic detection of token transfers
					if len(instruction.Accounts) >= 3 {
						// Token transfers typically have [source, destination, owner] accounts
						fromIdx := instruction.Accounts[0]
						toIdx := instruction.Accounts[1]

						if fromIdx < len(txResponse.Result.Transaction.Message.AccountKeys) &&
							toIdx < len(txResponse.Result.Transaction.Message.AccountKeys) {
							// These are token accounts, not wallet addresses
							fromAddress = txResponse.Result.Transaction.Message.AccountKeys[fromIdx]
							toAddress = txResponse.Result.Transaction.Message.AccountKeys[toIdx]

							// For tokens, extracting the amount would require parsing the instruction data
							// which is beyond the scope of this simple implementation
						}
					}
				}
			}
		}
	}

	return fromAddress, toAddress, amount
}
