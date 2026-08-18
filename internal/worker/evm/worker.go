package evm

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	"bitbucket.org/zapspace/zap-go-server/config/mongodb"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/internal/worker"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	httpmanager "bitbucket.org/zapspace/zap-go-server/managers/http"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain"
	"bitbucket.org/zapspace/zap-go-server/src/adapters"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/usecases"
)

// Constants for environment variables and API
const (
	EnvAnkrAPIKey     = "ANKR_API_KEY"
	DefaultAnkrAPIURL = "https://rpc.ankr.com/eth" // Will be modified by chain
)

// ChainWorker represents a running chain worker
type ChainWorker struct {
	chainSymbol string
	cancel      context.CancelFunc
	wg          *sync.WaitGroup
	// Resource tracking for monitoring
	startTime    time.Time
	requestCount int64
	errorCount   int64
}

// Worker represents the EVM worker service
type Worker struct {
	cfg           *config.Config
	concurrency   int
	logger        logger.Logger
	redisClient   *redis.Client
	queueSvc      *services.QueueService
	mongoClient   *mongo.Client
	cacheManager  *cache.CacheManager
	scheduledTime int
	// Dynamic chain management
	activeChains map[string]*ChainWorker
	chainsMutex  sync.RWMutex
	stopChan     chan struct{}
}

// New creates a new EVM worker
func New(cfg *config.Config, concurrency int, logger logger.Logger, redisClient *redis.Client, queueSvc *services.QueueService, mongoClient *mongo.Client, cacheManager *cache.CacheManager, scheduledTime int) worker.Worker {
	return &Worker{
		cfg:           cfg,
		concurrency:   concurrency,
		logger:        logger,
		redisClient:   redisClient,
		queueSvc:      queueSvc,
		mongoClient:   mongoClient,
		cacheManager:  cacheManager,
		scheduledTime: scheduledTime,
		activeChains:  make(map[string]*ChainWorker),
		stopChan:      make(chan struct{}),
	}
}

// Start initiates the EVM worker processing
func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info("Initializing Dynamic EVM Worker...")

	// Initialize common dependencies
	httpClient := createHTTPClient()
	ankrAPIKey := os.Getenv(EnvAnkrAPIKey)
	if ankrAPIKey == "" {
		w.logger.Warn("No Ankr API key provided, API requests may be rate-limited")
	}

	// Initialize MongoDB repositories
	walletRepo := mongodb.NewWalletRepository(w.mongoClient, w.cfg.MongoDB.Database)
	accountRepo := mongodb.NewAccountRepository(w.mongoClient, w.cfg.MongoDB.Database)
	currencyRepo := mongodb.NewCurrencyRepository(w.mongoClient, w.cfg.MongoDB.Database)
	supportedCurrencyRepo := mongodb.NewSupportedCurrencyRepository(w.mongoClient, w.cfg.MongoDB.Database)
	chainRepo := mongodb.NewChainRepository(w.mongoClient, w.cfg.MongoDB.Database)
	orderRepo := mongodb.NewOrderRepository(w.mongoClient, w.cfg.MongoDB.Database)

	// Create adapters for domain repositories
	walletDomainRepo := adapters.NewWalletRepositoryAdapter(walletRepo)
	accountDomainRepo := adapters.NewAccountRepositoryAdapter(accountRepo)
	orderDomainRepo := adapters.NewOrderRepositoryAdapter(orderRepo)
	currencyDomainRepo := adapters.NewCurrencyRepositoryAdapter(currencyRepo)
	supportedCurrencyDomainRepo := adapters.NewSupportedCurrencyRepositoryAdapter(supportedCurrencyRepo)
	chainDomainRepo := adapters.NewChainRepositoryAdapter(chainRepo)

	// Initialize blockchain provider factory
	providerFactory := blockchain.NewProviderFactory(
		w.logger,
		w.cfg.Chains.Bitcoin,
		w.cfg.Chains.EVM,
		w.cfg.Chains.Solana,
		w.cfg.Chains.Tron,
		config.ChangeNowConfig{},     // Empty ChangeNow config
		w.queueSvc.GetQueueManager(), // Use the queue manager
		nil,                          // No cache manager
	)

	// Initialize services (infrastructure layer)
	blockchainSvc := services.NewBlockchainService(w.logger, providerFactory)

	// Initialize RequestManager
	requestManager := httpmanager.NewRequestManager(w.logger)
	w.logger.Info("RequestManager initialized")

	// Initialize use cases (application layer)
	accountUseCase := usecases.NewAccountUseCase(
		accountDomainRepo,
		walletDomainRepo,
		currencyDomainRepo,
		supportedCurrencyDomainRepo,
		chainDomainRepo,
		blockchainSvc,
		w.logger,
		requestManager,
	)
	orderUseCase := usecases.NewOrderUseCase(
		orderDomainRepo,
		walletDomainRepo,
		accountDomainRepo,
		currencyDomainRepo,
		supportedCurrencyDomainRepo,
		chainDomainRepo,
		blockchainSvc,
		w.logger,
		requestManager,
	)

	// Start the order monitor in a goroutine
	go w.startOrderMonitor(ctx, httpClient, ankrAPIKey, accountUseCase, orderUseCase, requestManager)

	w.logger.Info("Dynamic EVM Worker started - monitoring pending orders")

	// Wait for context cancellation or shutdown signal
	waitForShutdown(w.logger, w.stopChan)

	// Stop all active chains
	w.stopAllChains()

	return nil
}

// startOrderMonitor periodically checks for pending orders and manages chain workers
func (w *Worker) startOrderMonitor(ctx context.Context, httpClient *http.Client, ankrAPIKey string, accountUseCase *usecases.AccountUseCase, orderUseCase *usecases.OrderUseCase, requestManager *httpmanager.RequestManager) {
	ticker := time.NewTicker(1 * time.Minute) // Check every 1 minute
	defer ticker.Stop()

	w.logger.Info("Starting order monitor - checking for pending orders every 1 minute")

	// Run an initial check immediately
	w.checkPendingOrdersAndManageChains(ctx, httpClient, ankrAPIKey, accountUseCase, orderUseCase, requestManager)

	for {
		select {
		case <-ticker.C:
			w.checkPendingOrdersAndManageChains(ctx, httpClient, ankrAPIKey, accountUseCase, orderUseCase, requestManager)
		case <-w.stopChan:
			w.logger.Info("Stopping order monitor")
			return
		case <-ctx.Done():
			w.logger.Info("Context cancelled, stopping order monitor")
			return
		}
	}
}

// checkPendingOrdersAndManageChains checks for pending orders and starts/stops chains accordingly
func (w *Worker) checkPendingOrdersAndManageChains(ctx context.Context, httpClient *http.Client, ankrAPIKey string, accountUseCase *usecases.AccountUseCase, orderUseCase *usecases.OrderUseCase, requestManager *httpmanager.RequestManager) {
	w.logger.Info("Periodic check: Scanning for pending orders to manage chain workers")

	// Get chains that have pending orders
	chainsWithOrders, err := w.getChainsWithPendingOrders(ctx)
	if err != nil {
		w.logger.Error("Failed to get chains with pending orders", "error", err)
		return
	}

	// Get current state for comparison
	w.chainsMutex.RLock()
	currentActiveChains := w.getActiveChainsList()
	activeCount := len(w.activeChains)
	w.chainsMutex.RUnlock()

	w.logger.Info("Periodic check results",
		"chainsWithOrders", chainsWithOrders,
		"currentActiveChains", currentActiveChains,
		"activeCount", activeCount)

	// Manage chain workers based on pending orders
	w.manageChainsBasedOnOrders(ctx, chainsWithOrders, httpClient, ankrAPIKey, accountUseCase, orderUseCase, requestManager)

	// Log final state after management
	w.chainsMutex.RLock()
	finalActiveChains := w.getActiveChainsList()
	finalActiveCount := len(w.activeChains)
	w.chainsMutex.RUnlock()

	w.logger.Info("Periodic check completed",
		"finalActiveChains", finalActiveChains,
		"finalActiveCount", finalActiveCount)
}

// getChainsWithPendingOrders queries the database for chains that have pending orders
func (w *Worker) getChainsWithPendingOrders(ctx context.Context) ([]string, error) {
	ordersCollection := w.mongoClient.Database(w.cfg.MongoDB.Database).Collection("orders")

	// Aggregate to find chains with pending orders
	pipeline := []bson.M{
		{
			"$match": bson.M{
				"flow":   constants.ORDER_FLOW_BUY,
				"status": constants.ORDER_STATUS_PENDING,
			},
		},
		{
			"$lookup": bson.M{
				"from":         "chains",
				"localField":   "buyChain",
				"foreignField": "_id",
				"as":           "chain",
			},
		},
		{
			"$unwind": "$chain",
		},
		{
			"$group": bson.M{
				"_id":   "$chain.symbol",
				"count": bson.M{"$sum": 1},
			},
		},
	}

	cursor, err := ordersCollection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate pending orders by chain: %w", err)
	}
	defer cursor.Close(ctx)

	var results []struct {
		ChainSymbol string `bson:"_id"`
		Count       int    `bson:"count"`
	}

	if err = cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("failed to decode aggregation results: %w", err)
	}

	// Extract chain symbols, but only for EVM chains
	var chainSymbols []string
	for _, result := range results {
		chainSymbol := strings.ToLower(result.ChainSymbol)
		// Check if this is a supported EVM chain
		if _, exists := SupportedChains[chainSymbol]; exists {
			chainSymbols = append(chainSymbols, chainSymbol)
			w.logger.Info("Found pending orders for EVM chain", "chain", chainSymbol, "count", result.Count)
		}
	}

	return chainSymbols, nil
}

// manageChainsBasedOnOrders starts chains that have orders and stops chains that don't
func (w *Worker) manageChainsBasedOnOrders(ctx context.Context, chainsWithOrders []string, httpClient *http.Client, ankrAPIKey string, accountUseCase *usecases.AccountUseCase, orderUseCase *usecases.OrderUseCase, requestManager *httpmanager.RequestManager) {
	w.chainsMutex.Lock()
	defer w.chainsMutex.Unlock()

	// Early optimization: check if current active chains exactly match required chains
	if w.chainsMatchCurrentState(chainsWithOrders) {
		w.logger.Debug("Chain workers already match required state - no changes needed",
			"activeChains", w.getActiveChainsList(),
			"requiredChains", chainsWithOrders)
		return
	}

	// Convert slice to map for quick lookup
	orderChains := make(map[string]bool)
	for _, chain := range chainsWithOrders {
		orderChains[chain] = true
	}

	// Stop chains that no longer have orders
	for chainSymbol, chainWorker := range w.activeChains {
		if !orderChains[chainSymbol] {
			w.logger.Info("Stopping chain worker - no pending orders", "chain", chainSymbol)
			w.stopChainWorker(chainSymbol, chainWorker)
			delete(w.activeChains, chainSymbol)
		}
	}

	// Start chains that have orders but aren't running
	for _, chainSymbol := range chainsWithOrders {
		if _, exists := w.activeChains[chainSymbol]; !exists {
			w.logger.Info("Starting chain worker - found pending orders", "chain", chainSymbol)
			if chainWorker := w.startChainWorker(ctx, chainSymbol, httpClient, ankrAPIKey, accountUseCase, orderUseCase, requestManager); chainWorker != nil {
				w.activeChains[chainSymbol] = chainWorker
			}
		}
	}

	// Log current status
	w.logger.Info("Chain workers status updated", "totalActive", len(w.activeChains), "activeChains", w.getActiveChainsList())
}

// chainsMatchCurrentState checks if the required chains exactly match the currently active chains
func (w *Worker) chainsMatchCurrentState(requiredChains []string) bool {
	// Quick length check
	if len(requiredChains) != len(w.activeChains) {
		return false
	}

	// Convert required chains to map for quick lookup
	requiredMap := make(map[string]bool)
	for _, chain := range requiredChains {
		requiredMap[chain] = true
	}

	// Check if all active chains are in required chains
	for activeChain := range w.activeChains {
		if !requiredMap[activeChain] {
			return false
		}
	}

	// If we reach here, both sets have same length and all active chains are required
	return true
}

// startChainWorker starts a worker for a specific chain
func (w *Worker) startChainWorker(ctx context.Context, chainSymbol string, httpClient *http.Client, ankrAPIKey string, accountUseCase *usecases.AccountUseCase, orderUseCase *usecases.OrderUseCase, requestManager *httpmanager.RequestManager) *ChainWorker {
	chainSymbol = strings.ToLower(chainSymbol)
	chainConfig, ok := SupportedChains[chainSymbol]
	if !ok {
		w.logger.Error("Unsupported chain - cannot start worker", "chain", chainSymbol)
		return nil
	}

	// Add startup delay to prevent resource spikes when multiple chains start simultaneously
	// This staggers API calls, database operations, and resource initialization
	startupDelay := time.Duration(len(w.activeChains)) * 2 * time.Second
	if startupDelay > 0 {
		w.logger.Info("Adding startup delay to prevent resource conflicts",
			"chain", chainSymbol,
			"delay", startupDelay.String(),
			"activeChains", len(w.activeChains))
		time.Sleep(startupDelay)
	}

	w.logger.Info("Initializing EVM worker for chain",
		"chain", chainConfig.Name,
		"symbol", chainConfig.Symbol,
		"chainId", chainConfig.ChainID)

	// Create a cancellable context for this chain
	chainCtx, cancel := context.WithCancel(ctx)

	// Create WaitGroup for this chain
	var wg sync.WaitGroup

	// Set cache key to indicate this chain is active
	cacheKey := fmt.Sprintf("evm:chain:%s:active", chainSymbol)
	err := w.cacheManager.Set(chainCtx, cacheKey, true, 60) // 1 minute TTL
	if err != nil {
		w.logger.Warn("Failed to set chain active cache key", "chain", chainSymbol, "error", err)
	}

	// Initialize chain-specific services
	ankrClient := NewAnkrClient(chainConfig.RpcURL, ankrAPIKey, httpClient, w.logger)
	processor := NewChainProcessor(chainCtx, chainSymbol, w.queueSvc.GetQueueManager(), w.mongoClient, w.redisClient, httpClient, ankrAPIKey, w.concurrency, w.cfg, w.logger, accountUseCase, orderUseCase, ankrClient, w.cacheManager, chainConfig)

	queueName := fmt.Sprintf("%s:%s", constants.QueueNames["QueueEVMBlockMonitor"], chainSymbol)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			// Remove cache key when chain stops
			w.cacheManager.Delete(context.Background(), cacheKey)
		}()

		// Register and start processing
		w.queueSvc.GetQueueManager().RegisterHandler(queueName, func(ctx context.Context, data map[string]interface{}) error {
			w.logger.Info("Processing EVM chain", "chain", chainConfig.Name)
			return processor.ProcessChain(ctx, accountUseCase, orderUseCase, requestManager)
		})

		w.logger.Info("Starting EVM queue processor",
			"queue", queueName,
			"concurrency", w.concurrency,
			"chain", chainConfig.Name)

		if err := w.queueSvc.GetQueueManager().ProcessJobs(chainCtx, queueName, w.concurrency); err != nil {
			w.logger.Error("Failed to start processing jobs",
				"error", err,
				"chain", chainConfig.Name)
		}

		// Start scheduler for this chain
		startScheduler(chainCtx, queueName, w.redisClient, w.logger, w.scheduledTime)
	}()

	return &ChainWorker{
		chainSymbol: chainSymbol,
		cancel:      cancel,
		wg:          &wg,
		startTime:   time.Now(),
	}
}

// stopChainWorker stops a specific chain worker
func (w *Worker) stopChainWorker(chainSymbol string, chainWorker *ChainWorker) {
	w.logger.Info("Stopping chain worker", "chain", chainSymbol)

	// Cancel the context
	chainWorker.cancel()

	// Wait for the worker to finish (with timeout)
	done := make(chan struct{})
	go func() {
		chainWorker.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.logger.Info("Chain worker stopped successfully", "chain", chainSymbol)
	case <-time.After(30 * time.Second):
		w.logger.Warn("Chain worker stop timeout", "chain", chainSymbol)
	}
}

// stopAllChains stops all active chain workers
func (w *Worker) stopAllChains() {
	w.chainsMutex.Lock()
	defer w.chainsMutex.Unlock()

	w.logger.Info("Stopping all active chain workers", "count", len(w.activeChains))

	for chainSymbol, chainWorker := range w.activeChains {
		w.stopChainWorker(chainSymbol, chainWorker)
	}

	// Clear the map
	w.activeChains = make(map[string]*ChainWorker)

	w.logger.Info("All chain workers stopped")
}

// getActiveChainsList returns a list of currently active chains
func (w *Worker) getActiveChainsList() []string {
	var chains []string
	for chain := range w.activeChains {
		chains = append(chains, chain)
	}
	return chains
}

// GetStatus returns the current status of the EVM worker
func (w *Worker) GetStatus() map[string]interface{} {
	w.chainsMutex.RLock()
	defer w.chainsMutex.RUnlock()

	status := map[string]interface{}{
		"active":          true,
		"totalChains":     len(w.activeChains),
		"activeChains":    w.getActiveChainsList(),
		"supportedChains": getSupportedChainsList(),
		"lastCheck":       time.Now().Format(time.RFC3339),
	}

	// Add individual chain cache status
	chainStatus := make(map[string]bool)
	for chainSymbol := range w.activeChains {
		// Chain is active if it's in our activeChains map
		chainStatus[chainSymbol] = true
	}
	status["chainCacheStatus"] = chainStatus

	return status
}

// getSupportedChainsList returns a list of all supported EVM chains
func getSupportedChainsList() []string {
	var chains []string
	for chainSymbol := range SupportedChains {
		chains = append(chains, chainSymbol)
	}
	return chains
}

// IsChainActive checks if a specific chain is currently active
func (w *Worker) IsChainActive(chainSymbol string) bool {
	w.chainsMutex.RLock()
	defer w.chainsMutex.RUnlock()

	_, exists := w.activeChains[chainSymbol]
	return exists
}

// GetActiveChainCount returns the number of currently active chains
func (w *Worker) GetActiveChainCount() int {
	w.chainsMutex.RLock()
	defer w.chainsMutex.RUnlock()

	return len(w.activeChains)
}

// GetResourceUsage returns detailed resource usage information for all chains
func (w *Worker) GetResourceUsage() map[string]interface{} {
	w.chainsMutex.RLock()
	defer w.chainsMutex.RUnlock()

	resources := make(map[string]interface{})
	totalGoroutines := 0
	totalRequests := int64(0)
	totalErrors := int64(0)

	chainDetails := make(map[string]interface{})
	for chain, worker := range w.activeChains {
		// Calculate estimated goroutines per chain
		chainGoroutines := w.concurrency + 3 // workers + main + scheduler + initial job
		totalGoroutines += chainGoroutines

		requests := atomic.LoadInt64(&worker.requestCount)
		errors := atomic.LoadInt64(&worker.errorCount)
		totalRequests += requests
		totalErrors += errors

		chainDetails[chain] = map[string]interface{}{
			"uptime":              time.Since(worker.startTime).String(),
			"estimatedGoroutines": chainGoroutines,
			"requestCount":        requests,
			"errorCount":          errors,
			"errorRate": func() float64 {
				if requests > 0 {
					return float64(errors) / float64(requests) * 100
				}
				return 0
			}(),
		}
	}

	resources["summary"] = map[string]interface{}{
		"totalChains":         len(w.activeChains),
		"estimatedGoroutines": totalGoroutines,
		"totalRequests":       totalRequests,
		"totalErrors":         totalErrors,
		"overallErrorRate": func() float64 {
			if totalRequests > 0 {
				return float64(totalErrors) / float64(totalRequests) * 100
			}
			return 0
		}(),
	}
	resources["chains"] = chainDetails
	resources["lastCheck"] = time.Now().Format(time.RFC3339)

	return resources
}

// LogResourceUsage logs current resource usage (called by health checker)
func (w *Worker) LogResourceUsage() {
	resources := w.GetResourceUsage()
	summary := resources["summary"].(map[string]interface{})

	w.logger.Info("EVM Worker Resource Usage",
		"totalChains", summary["totalChains"],
		"estimatedGoroutines", summary["estimatedGoroutines"],
		"totalRequests", summary["totalRequests"],
		"totalErrors", summary["totalErrors"],
		"errorRate", fmt.Sprintf("%.2f%%", summary["overallErrorRate"]),
		"activeChains", w.getActiveChainsList(),
	)
}

// Helper functions from the original main.go

func getTargetChains(singleChainFlag string) []string {
	if singleChainFlag != "" {
		return []string{singleChainFlag}
	}

	// Check if chains are specified in the environment
	chains := ""
	if chains == "" {
		// Return empty slice if no chains specified - we now manage chains dynamically
		return []string{}
	}

	// Split chains and filter out empty strings
	chainList := strings.Split(chains, ",")
	var validChains []string
	for _, chain := range chainList {
		chain = strings.TrimSpace(chain)
		if chain != "" {
			validChains = append(validChains, chain)
		}
	}

	return validChains
}

func createHTTPClient() *http.Client {
	return &http.Client{
		Timeout: http.DefaultClient.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			MaxConnsPerHost:     100,
		},
	}
}

func waitForShutdown(logger logger.Logger, stopChan chan struct{}) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("Shutdown signal received")
	close(stopChan)
}
