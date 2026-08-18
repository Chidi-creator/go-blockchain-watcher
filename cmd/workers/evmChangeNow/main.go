package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	mongodbConfig "bitbucket.org/zapspace/zap-go-server/config/mongodb"
	redisConfig "bitbucket.org/zapspace/zap-go-server/config/redis"
	"bitbucket.org/zapspace/zap-go-server/config/secrets"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	combined "bitbucket.org/zapspace/zap-go-server/internal/worker/evmChangeNow"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	httpmanager "bitbucket.org/zapspace/zap-go-server/managers/http"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain"
	"bitbucket.org/zapspace/zap-go-server/src/adapters"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/usecases"
)

func main() {

	// Initialize secrets manager
	sm, err := secrets.NewAWSSecretsManager("us-east-1")
	if err != nil {
		log.Fatalf("Failed to initialize secrets manager: %v", err)
	}

	// Get current working directory
	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get working directory: %v", err)
	}

	// Path to .env file (in root directory of deployed app)
	envPath := filepath.Join(wd, ".env")

	// // Check if .env file already exists
	// if _, err := os.Stat(envPath); err == nil {
	// 	// File exists
	// 	log.Fatalf("Error: .env file already exists at %s. Won't override existing file.", envPath)
	// } else if !os.IsNotExist(err) {
	// 	// Some other error occurred when checking file
	// 	log.Fatalf("Error checking if .env file exists: %v", err)
	// }

	// Fetch and write secrets
	if err := sm.FetchAndWriteSecrets("new-backend-secrets", envPath); err != nil {
		log.Fatalf("Failed to load secrets: %v", err)
	}

	// Parse command line flags
	// evmConcurrency := flag.Int("evm-concurrency", 2, "Number of concurrent EVM worker processes")
	// changenowConcurrency := flag.Int("changenow-concurrency", 1, "Number of concurrent ChangeNow worker processes")
	// flag.Parse()

	// Initialize configuration
	cfg, err := config.Load()
	if err != nil {
		panic("Failed to load configuration: " + err.Error())
	}

	// Reduced concurrency to prevent goroutine explosion with multiple chains
	// With N chains active, total goroutines = N * concurrency + N * 3 (management)
	// Setting to 1 keeps resource usage linear instead of exponential
	evmConcurrency := 2 // Was 3 - reduced to prevent resource explosion
	changenowConcurrency := 1

	// Initialize logger
	log := logger.NewLogger(cfg.LogLevel)
	log.Info("EvmChangeNow Worker starting",
		"evm_concurrency", evmConcurrency,
		"changenow_concurrency", changenowConcurrency)

	// Create a context that will be canceled on SIGTERM or SIGINT
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-signalChan
		log.Info("Received signal, initiating shutdown", "signal", sig)
		cancel()
	}()

	// Initialize and start the evmChangeNow worker
	redisClient, err := redisConfig.GetInstance(cfg.Redis, log)
	if err != nil {
		log.Error("Failed to connect to Redis", "error", err)
		os.Exit(1)
	}

	queueSvc := services.NewQueueService(redisClient, log)
	defer redisClient.Close()
	log.Info("Connected to Redis", "addr", cfg.Redis.Host+":"+cfg.Redis.Port)

	mongoClient, err := mongodbConfig.NewClient(cfg.MongoDB.URI, cfg.MongoDB.Database)
	if err != nil {
		log.Error("Failed to connect to MongoDB", "error", err)
		os.Exit(1)
	}
	defer mongoClient.Disconnect(ctx)

	cacheManager := cache.NewCacheManager(redisClient.GetRedisClient(), log)

	// Initialize MongoDB repositories
	walletRepo := mongodbConfig.NewWalletRepository(mongoClient.Client, cfg.MongoDB.Database)
	accountRepo := mongodbConfig.NewAccountRepository(mongoClient.Client, cfg.MongoDB.Database)
	currencyRepo := mongodbConfig.NewCurrencyRepository(mongoClient.Client, cfg.MongoDB.Database)
	supportedCurrencyRepo := mongodbConfig.NewSupportedCurrencyRepository(mongoClient.Client, cfg.MongoDB.Database)
	chainRepo := mongodbConfig.NewChainRepository(mongoClient.Client, cfg.MongoDB.Database)
	orderRepo := mongodbConfig.NewOrderRepository(mongoClient.Client, cfg.MongoDB.Database)

	// Create adapters for domain repositories
	walletDomainRepo := adapters.NewWalletRepositoryAdapter(walletRepo)
	accountDomainRepo := adapters.NewAccountRepositoryAdapter(accountRepo)
	orderDomainRepo := adapters.NewOrderRepositoryAdapter(orderRepo)
	currencyDomainRepo := adapters.NewCurrencyRepositoryAdapter(currencyRepo)
	supportedCurrencyDomainRepo := adapters.NewSupportedCurrencyRepositoryAdapter(supportedCurrencyRepo)
	chainDomainRepo := adapters.NewChainRepositoryAdapter(chainRepo)

	// Initialize blockchain provider factory
	providerFactory := blockchain.NewProviderFactory(
		log,
		cfg.Chains.Bitcoin,
		cfg.Chains.EVM,
		cfg.Chains.Solana,
		cfg.Chains.Tron,
		config.ChangeNowConfig{},   // Empty ChangeNow config
		queueSvc.GetQueueManager(), // Use the queue manager
		nil,                        // No cache manager
	)

	// Initialize services (infrastructure layer)
	blockchainSvc := services.NewBlockchainService(log, providerFactory)

	// Initialize RequestManager
	requestManager := httpmanager.NewRequestManager(log)

	// Initialize order usecase
	orderUsecase := usecases.NewOrderUseCase(
		orderDomainRepo,
		walletDomainRepo,
		accountDomainRepo,
		currencyDomainRepo,
		supportedCurrencyDomainRepo,
		chainDomainRepo,
		blockchainSvc,
		log,
		requestManager,
	)

	worker := combined.New(cfg, evmConcurrency, changenowConcurrency, log, redisClient, queueSvc, mongoClient.Client, cacheManager, orderUsecase)
	if err := worker.Start(ctx); err != nil {
		log.Error("EvmChangeNow worker failed", "error", err)
		os.Exit(1)
	}

	log.Info("EvmChangeNow worker shut down successfully")
}
