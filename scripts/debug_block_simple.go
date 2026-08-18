package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"bitbucket.org/zapspace/zap-go-server/internal/worker/evm"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

type BlockDebugger struct {
	ctx           context.Context
	chainSymbol   string
	mongoClient   *mongo.Client
	redisClient   *redis.Client
	ankrClient    *evm.AnkrClient
	logger        logger.Logger
	mongoDatabase string
}

type TransactionInfo struct {
	Hash        string  `json:"hash"`
	From        string  `json:"from"`
	To          string  `json:"to"`
	Value       string  `json:"value"`
	ValueETH    float64 `json:"valueETH"`
	BlockNumber string  `json:"blockNumber"`
	Type        string  `json:"type"` // "native" or "token"
	TokenAddr   string  `json:"tokenAddress,omitempty"`
}

type TokenTransfer struct {
	TransactionHash string
	BlockNumber     string
	TokenAddress    string
	From            string
	To              string
	Amount          string
}

type BlockAnalysis struct {
	BlockNumber     string            `json:"blockNumber"`
	TotalTxCount    int               `json:"totalTxCount"`
	NativeTxCount   int               `json:"nativeTxCount"`
	TokenTxCount    int               `json:"tokenTxCount"`
	Transactions    []TransactionInfo `json:"transactions"`
	TokenTransfers  []TransactionInfo `json:"tokenTransfers"`
	MatchedAccounts []string          `json:"matchedAccounts"`
	TargetAddresses []string          `json:"targetAddresses,omitempty"`
	FoundTargets    []TransactionInfo `json:"foundTargets,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run debug_block_simple.go <block_number> [target_address1] [target_address2] ...")
		fmt.Println("Example: go run debug_block_simple.go 52534867 0x24f150521C1B0DB290EE611B2E051FAA8E821024")
		fmt.Println("\nRequired environment variables:")
		fmt.Println("- MONGODB_URI")
		fmt.Println("- MONGODB_DATABASE")
		fmt.Println("- ANKR_API_KEY")
		fmt.Println("\nOptional environment variables:")
		fmt.Println("- REDIS_HOST (default: localhost)")
		fmt.Println("- REDIS_PORT (default: 6379)")
		fmt.Println("- REDIS_PASSWORD (default: empty)")
		fmt.Println("- REDIS_DB (default: 0)")
		os.Exit(1)
	}

	blockNumberStr := os.Args[1]
	blockNumber, err := strconv.ParseInt(blockNumberStr, 10, 64)
	if err != nil {
		log.Fatalf("Invalid block number: %v", err)
	}

	// Optional target addresses to specifically look for
	var targetAddresses []string
	if len(os.Args) > 2 {
		targetAddresses = os.Args[2:]
		fmt.Printf("🎯 Looking for specific addresses: %v\n", targetAddresses)
	}

	// Load environment variables from project root
	loadEnvFile()

	// Initialize components
	ctx := context.Background()

	// Initialize logger
	logger := logger.NewLogger("info")

	// Get required environment variables
	mongoURI := getRequiredEnv("MONGODB_URI")
	mongoDatabase := getRequiredEnv("MONGODB_DATABASE")
	ankrAPIKey := getRequiredEnv("ANKR_API_KEY")

	// Redis configuration (with defaults if not provided)
	redisHost := getEnvWithDefault("REDIS_HOST", "localhost")
	redisPort := getEnvWithDefault("REDIS_PORT", "6379")
	redisPassword := getEnvWithDefault("REDIS_PASSWORD", "")
	redisDB := getIntEnvWithDefault("REDIS_DB", 0)

	// Connect to MongoDB
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	// Test MongoDB connection
	err = mongoClient.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to ping MongoDB: %v", err)
	}
	fmt.Println("✅ Connected to MongoDB")

	// Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisHost + ":" + redisPort,
		Password: redisPassword,
		DB:       redisDB,
	})
	defer redisClient.Close()

	// Test Redis connection
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		fmt.Printf("⚠️  Redis connection failed: %v (continuing without Redis)\n", err)
		redisClient = nil
	} else {
		fmt.Println("✅ Connected to Redis")
	}

	// Initialize Ankr client for BSC
	chainSymbol := "bsc"
	httpClient := &http.Client{Timeout: 30 * time.Second}
	ankrClient := evm.NewAnkrClient("https://rpc.ankr.com/bsc", ankrAPIKey, httpClient, logger)

	debugger := &BlockDebugger{
		ctx:           ctx,
		chainSymbol:   chainSymbol,
		mongoClient:   mongoClient,
		redisClient:   redisClient,
		ankrClient:    ankrClient,
		logger:        logger,
		mongoDatabase: mongoDatabase,
	}

	// Debug the block
	fmt.Printf("🔍 Analyzing block %s on %s chain...\n\n", blockNumberStr, strings.ToUpper(chainSymbol))
	analysis, err := debugger.DebugBlock(big.NewInt(blockNumber), targetAddresses)
	if err != nil {
		log.Fatalf("Failed to debug block: %v", err)
	}

	// Output detailed results
	jsonData, err := json.MarshalIndent(analysis, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal results: %v", err)
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("🔍 DETAILED BLOCK ANALYSIS FOR BLOCK %s\n", blockNumberStr)
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println(string(jsonData))

	// Summary
	fmt.Println("\n" + strings.Repeat("-", 80))
	fmt.Println("📊 SUMMARY:")
	fmt.Printf("Total transactions: %d\n", analysis.TotalTxCount)
	fmt.Printf("Native transactions: %d\n", analysis.NativeTxCount)
	fmt.Printf("Token transfers: %d\n", analysis.TokenTxCount)
	fmt.Printf("Matched deposit accounts: %d\n", len(analysis.MatchedAccounts))

	if len(targetAddresses) > 0 {
		fmt.Printf("Target addresses searched: %d\n", len(targetAddresses))
		fmt.Printf("Target addresses found: %d\n", len(analysis.FoundTargets))

		if len(analysis.FoundTargets) > 0 {
			fmt.Println("\n🎯 FOUND TARGET TRANSACTIONS:")
			for _, tx := range analysis.FoundTargets {
				fmt.Printf("  ✅ Hash: %s\n", tx.Hash)
				fmt.Printf("     From: %s\n", tx.From)
				fmt.Printf("     To: %s\n", tx.To)
				fmt.Printf("     Value: %s (%f)\n", tx.Value, tx.ValueETH)
				fmt.Printf("     Type: %s\n", tx.Type)
				if tx.TokenAddr != "" {
					fmt.Printf("     Token Address: %s\n", tx.TokenAddr)
				}
				fmt.Println()
			}
		} else {
			fmt.Println("\n❌ NO TARGET TRANSACTIONS FOUND")
			fmt.Println("   This could mean:")
			fmt.Println("   - The transaction was not in this block")
			fmt.Println("   - The address case doesn't match (but our search is case-insensitive)")
			fmt.Println("   - The transaction was in a different block")
		}
	}

	if len(analysis.MatchedAccounts) > 0 {
		fmt.Println("\n💰 DEPOSIT ACCOUNTS FOUND IN THIS BLOCK:")
		for _, addr := range analysis.MatchedAccounts {
			fmt.Printf("  - %s\n", addr)
		}
	}

	fmt.Println(strings.Repeat("-", 80))
	fmt.Println("✅ Analysis complete!")
}

// loadEnvFile loads .env from the project root (one directory up from scripts)
func loadEnvFile() {
	// Try current directory first
	err := godotenv.Load()
	if err != nil {
		// Try parent directory (project root) - use ".." to go up one level
		envPath := "../.env"
		err = godotenv.Load(envPath)
		if err != nil {
			fmt.Printf("Warning: .env file not found in current directory or parent directory: %v\n", err)
			fmt.Println("Make sure you have the required environment variables set:")
			fmt.Println("- MONGODB_URI")
			fmt.Println("- MONGODB_DATABASE")
			fmt.Println("- ANKR_API_KEY")
		} else {
			fmt.Printf("✅ Loaded .env from: %s\n", envPath)
		}
	} else {
		fmt.Println("✅ Loaded .env from current directory")
	}
}

func getRequiredEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Required environment variable %s is not set", key)
	}
	return value
}

func getEnvWithDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getIntEnvWithDefault(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("Warning: Invalid integer value for %s: %s, using default %d", key, value, defaultValue)
		return defaultValue
	}
	return intValue
}

func (d *BlockDebugger) DebugBlock(blockNumber *big.Int, targetAddresses []string) (*BlockAnalysis, error) {
	d.logger.Info("Starting block debug", "blockNumber", blockNumber.String(), "targets", len(targetAddresses))

	analysis := &BlockAnalysis{
		BlockNumber:     blockNumber.String(),
		Transactions:    []TransactionInfo{},
		TokenTransfers:  []TransactionInfo{},
		MatchedAccounts: []string{},
		TargetAddresses: targetAddresses,
		FoundTargets:    []TransactionInfo{},
	}

	// Normalize target addresses to lowercase for case-insensitive matching
	normalizedTargets := make(map[string]string) // lowercase -> original
	for _, addr := range targetAddresses {
		normalizedTargets[strings.ToLower(addr)] = addr
		d.logger.Info("Normalized target address", "original", addr, "lowercase", strings.ToLower(addr))
	}

	// 1. Get block with transactions
	d.logger.Info("Fetching block data...")
	block, err := d.ankrClient.GetBlockByNumber(d.ctx, blockNumber, true)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	if block == nil {
		d.logger.Warn("Block not found or has no data")
		return analysis, nil
	}

	analysis.TotalTxCount = len(block.Transactions)
	d.logger.Info("Block fetched", "txCount", analysis.TotalTxCount)

	// 2. Get token transfer logs
	d.logger.Info("Fetching token transfer logs...")
	tokenLogs, err := d.getTokenTransferLogs(blockNumber)
	if err != nil {
		d.logger.Warn("Failed to get token logs", "error", err)
		tokenLogs = []interface{}{} // Continue with empty logs
	}

	// 3. Process native transactions
	d.logger.Info("Processing native transactions...")
	nativeTransactions := []TransactionInfo{}
	addressSet := make(map[string]bool)

	for _, txInterface := range block.Transactions {
		txData, ok := txInterface.(map[string]interface{})
		if !ok {
			continue
		}

		tx, err := d.convertToTransactionInfo(txData, "native")
		if err != nil {
			d.logger.Warn("Failed to convert transaction", "error", err)
			continue
		}

		nativeTransactions = append(nativeTransactions, tx)
		if tx.To != "" {
			addressSet[strings.ToLower(tx.To)] = true
		}
		if tx.From != "" {
			addressSet[strings.ToLower(tx.From)] = true
		}

		// Check if this transaction involves any target addresses (case-insensitive)
		if len(normalizedTargets) > 0 {
			if _, found := normalizedTargets[strings.ToLower(tx.To)]; found {
				d.logger.Info("Found target in native tx 'to' field", "txHash", tx.Hash, "to", tx.To)
				analysis.FoundTargets = append(analysis.FoundTargets, tx)
			}
			if _, found := normalizedTargets[strings.ToLower(tx.From)]; found {
				d.logger.Info("Found target in native tx 'from' field", "txHash", tx.Hash, "from", tx.From)
				analysis.FoundTargets = append(analysis.FoundTargets, tx)
			}
		}
	}

	analysis.Transactions = nativeTransactions
	analysis.NativeTxCount = len(nativeTransactions)

	// 4. Process token transfers
	d.logger.Info("Processing token transfers...")
	tokenTransfers := []TransactionInfo{}

	for _, logData := range tokenLogs {
		transfer, err := d.parseTokenTransferLog(logData)
		if err != nil {
			d.logger.Warn("Failed to parse token transfer", "error", err)
			continue
		}

		tx := TransactionInfo{
			Hash:        transfer.TransactionHash,
			From:        transfer.From,
			To:          transfer.To,
			Value:       transfer.Amount,
			ValueETH:    d.parseAmount(transfer.Amount),
			BlockNumber: transfer.BlockNumber,
			Type:        "token",
			TokenAddr:   transfer.TokenAddress,
		}

		tokenTransfers = append(tokenTransfers, tx)
		if tx.To != "" {
			addressSet[strings.ToLower(tx.To)] = true
		}
		if tx.From != "" {
			addressSet[strings.ToLower(tx.From)] = true
		}

		// Check if this token transfer involves any target addresses (case-insensitive)
		if len(normalizedTargets) > 0 {
			if _, found := normalizedTargets[strings.ToLower(tx.To)]; found {
				d.logger.Info("Found target in token transfer 'to' field", "txHash", tx.Hash, "to", tx.To, "tokenAddr", tx.TokenAddr)
				analysis.FoundTargets = append(analysis.FoundTargets, tx)
			}
			if _, found := normalizedTargets[strings.ToLower(tx.From)]; found {
				d.logger.Info("Found target in token transfer 'from' field", "txHash", tx.Hash, "from", tx.From, "tokenAddr", tx.TokenAddr)
				analysis.FoundTargets = append(analysis.FoundTargets, tx)
			}
		}
	}

	analysis.TokenTransfers = tokenTransfers
	analysis.TokenTxCount = len(tokenTransfers)

	// 5. Check which addresses have deposit accounts (with enhanced case-insensitive matching)
	d.logger.Info("Checking for deposit accounts...")
	addresses := make([]string, 0, len(addressSet))
	for addr := range addressSet {
		addresses = append(addresses, addr)
	}

	depositAccounts, err := d.findDepositAccountsByAddresses(addresses)
	if err != nil {
		d.logger.Warn("Failed to find deposit accounts", "error", err)
	} else {
		for _, account := range depositAccounts {
			if walletAddr, ok := account["walletAddress"].(string); ok {
				analysis.MatchedAccounts = append(analysis.MatchedAccounts, walletAddr)
			}
		}
	}

	d.logger.Info("Block debug completed",
		"nativeTx", analysis.NativeTxCount,
		"tokenTx", analysis.TokenTxCount,
		"matchedAccounts", len(analysis.MatchedAccounts),
		"foundTargets", len(analysis.FoundTargets))

	return analysis, nil
}

func (d *BlockDebugger) getTokenTransferLogs(blockNumber *big.Int) ([]interface{}, error) {
	// ERC-20 Transfer event signature
	transferTopicHash := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	topics := [][]string{{transferTopicHash}}

	return d.ankrClient.GetPastLogs(d.ctx, blockNumber, blockNumber, nil, topics)
}

func (d *BlockDebugger) convertToTransactionInfo(txData map[string]interface{}, txType string) (TransactionInfo, error) {
	tx := TransactionInfo{Type: txType}

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
		tx.ValueETH = d.parseAmount(value)
	}
	if blockNumber, ok := txData["blockNumber"].(string); ok {
		tx.BlockNumber = blockNumber
	}

	return tx, nil
}

func (d *BlockDebugger) parseTokenTransferLog(logData interface{}) (*TokenTransfer, error) {
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
		transfer.Amount = data
	}

	return transfer, nil
}

func (d *BlockDebugger) parseAmount(hexValue string) float64 {
	if hexValue == "" || hexValue == "0x0" {
		return 0
	}

	// Remove 0x prefix if present
	if len(hexValue) > 2 && hexValue[:2] == "0x" {
		hexValue = hexValue[2:]
	}

	// Parse as big integer
	amount := new(big.Int)
	amount, ok := amount.SetString(hexValue, 16)
	if !ok {
		return 0
	}

	// Convert to float64 with 18 decimals (standard for most EVM tokens)
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	amountFloat := new(big.Float).SetInt(amount)
	result := new(big.Float).Quo(amountFloat, divisor)

	floatAmount, _ := result.Float64()
	return floatAmount
}

// Enhanced findDepositAccountsByAddresses with better case-insensitive matching
func (d *BlockDebugger) findDepositAccountsByAddresses(addresses []string) ([]bson.M, error) {
	if len(addresses) == 0 {
		return []bson.M{}, nil
	}

	collection := d.mongoClient.Database(d.mongoDatabase).Collection("accounts")

	// Convert addresses to lowercase for case-insensitive matching
	lowerAddresses := make([]string, len(addresses))
	for i, addr := range addresses {
		lowerAddresses[i] = strings.ToLower(addr)
	}

	// Enhanced query with both lowercase matching and regex for case-insensitive fallback
	var accounts []bson.M

	// First try with lowercase matching (faster)
	cursor, err := collection.Find(d.ctx, bson.M{
		"walletAddress": bson.M{
			"$in": lowerAddresses,
		},
		"deletedAt": bson.M{"$exists": false},
	})

	if err != nil {
		return nil, err
	}
	defer cursor.Close(d.ctx)

	if err = cursor.All(d.ctx, &accounts); err != nil {
		return nil, err
	}

	// If no results, try case-insensitive regex matching as fallback
	if len(accounts) == 0 && len(addresses) > 0 {
		d.logger.Info("No lowercase matches found, trying case-insensitive regex matching")

		regexPatterns := make([]bson.M, len(addresses))
		for i, addr := range addresses {
			regexPatterns[i] = bson.M{
				"walletAddress": bson.M{
					"$regex":   "^" + addr + "$",
					"$options": "i", // case-insensitive
				},
			}
		}

		cursor2, err := collection.Find(d.ctx, bson.M{
			"$or":       regexPatterns,
			"deletedAt": bson.M{"$exists": false},
		})

		if err != nil {
			return nil, err
		}
		defer cursor2.Close(d.ctx)

		if err = cursor2.All(d.ctx, &accounts); err != nil {
			return nil, err
		}
	}

	return accounts, nil
}
