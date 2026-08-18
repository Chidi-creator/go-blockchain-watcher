package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"bitbucket.org/zapspace/zap-go-server/config/mongodb"
	redisConfig "bitbucket.org/zapspace/zap-go-server/config/redis"
	"bitbucket.org/zapspace/zap-go-server/config/secrets"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/internal/worker/solBTCTrx"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/http"
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

	// Initialize logger
	log := logger.NewLogger(cfg.LogLevel)

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

	// Initialize and start the solBTCTrx worker
	redisClient, err := redisConfig.GetInstance(cfg.Redis, log)
	if err != nil {
		log.Error("Failed to connect to Redis", "error", err)
		os.Exit(1)
	}

	queueSvc := services.NewQueueService(redisClient, log)
	defer redisClient.Close()
	log.Info("Connected to Redis", "addr", cfg.Redis.Host+":"+cfg.Redis.Port)

	mongoClient, err := mongodb.NewClient(cfg.MongoDB.URI, cfg.MongoDB.Database)
	if err != nil {
		log.Error("Failed to connect to MongoDB", "error", err)
		os.Exit(1)
	}
	defer mongoClient.Disconnect(ctx)

	appLogger := logger.NewLogger(cfg.LogLevel)
	appLogger.Info("Starting wallet service...")

	cacheManager := cache.NewCacheManager(redisClient.GetRedisClient(), appLogger)

	requestManager := http.NewRequestManager(log)

	defer mongoClient.Disconnect(context.Background())
	log.Info("Connected to MongoDB")

	// Initialize blockchain provider factory
	providerFactory := blockchain.NewProviderFactory(
		appLogger,
		cfg.Chains.Bitcoin,
		cfg.Chains.EVM,
		cfg.Chains.Solana,
		cfg.Chains.Tron,
		config.ChangeNowConfig{},   // Empty ChangeNow config
		queueSvc.GetQueueManager(), // Use the queue manager
		nil,                        // No cache manager
	)

	// Initialize services (infrastructure layer)
	blockchainSvc := services.NewBlockchainService(appLogger, providerFactory)

	// Create adapters for domain repositories

	accountRepo := mongodb.NewAccountRepository(mongoClient.Client, cfg.MongoDB.Database)
	walletRepo := mongodb.NewWalletRepository(mongoClient.Client, cfg.MongoDB.Database)
	currencyRepo := mongodb.NewCurrencyRepository(mongoClient.Client, cfg.MongoDB.Database)
	supportedCurrencyRepo := mongodb.NewSupportedCurrencyRepository(mongoClient.Client, cfg.MongoDB.Database)
	chainRepo := mongodb.NewChainRepository(mongoClient.Client, cfg.MongoDB.Database)

	walletDomainRepo := adapters.NewWalletRepositoryAdapter(walletRepo)
	accountDomainRepo := adapters.NewAccountRepositoryAdapter(accountRepo)
	currencyDomainRepo := adapters.NewCurrencyRepositoryAdapter(currencyRepo)
	supportedCurrencyDomainRepo := adapters.NewSupportedCurrencyRepositoryAdapter(supportedCurrencyRepo)
	chainDomainRepo := adapters.NewChainRepositoryAdapter(chainRepo)

	accountUsecase := usecases.NewAccountUseCase(accountDomainRepo, walletDomainRepo, currencyDomainRepo, supportedCurrencyDomainRepo, chainDomainRepo, blockchainSvc, log, requestManager)

	worker := solBTCTrx.New(cfg, log, redisClient, queueSvc, mongoClient.Client, cacheManager, accountUsecase, requestManager)
	if err := worker.Start(ctx); err != nil {
		log.Error("SolBTCTrx worker failed", "error", err)
		os.Exit(1)
	}

	log.Info("SolBTCTrx worker shut down successfully")
}
