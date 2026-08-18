package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/mongodb"
	"bitbucket.org/zapspace/zap-go-server/config/redis"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/interfaces/grpc"
	"bitbucket.org/zapspace/zap-go-server/interfaces/http"
	httpHandlers "bitbucket.org/zapspace/zap-go-server/interfaces/http/handlers"
	httpmanager "bitbucket.org/zapspace/zap-go-server/managers/http"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain"
	"bitbucket.org/zapspace/zap-go-server/src/adapters"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/usecases"
	stdgrpc "google.golang.org/grpc"
)

func main() {
	// Initialize configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize logger
	appLogger := logger.NewLogger(cfg.LogLevel)
	appLogger.Info("Starting wallet service...")

	// Initialize MongoDB
	mongoClient, err := mongodb.NewClient(cfg.MongoDB.URI, cfg.MongoDB.Database)
	if err != nil {
		appLogger.Fatal("Failed to connect to MongoDB", "error", err)
	}
	defer mongoClient.Disconnect(context.Background())
	appLogger.Info("Connected to MongoDB")

	// Initialize Redis client
	redisClient, err := redis.GetInstance(cfg.Redis, appLogger)
	if err != nil {
		appLogger.Fatal("Failed to connect to Redis", "error", err)
	}
	defer redisClient.Close()
	appLogger.Info("Connected to Redis")

	// Initialize MongoDB repositories
	walletRepo := mongodb.NewWalletRepository(mongoClient.Client, cfg.MongoDB.Database)
	accountRepo := mongodb.NewAccountRepository(mongoClient.Client, cfg.MongoDB.Database)
	currencyRepo := mongodb.NewCurrencyRepository(mongoClient.Client, cfg.MongoDB.Database)
	supportedCurrencyRepo := mongodb.NewSupportedCurrencyRepository(mongoClient.Client, cfg.MongoDB.Database)
	chainRepo := mongodb.NewChainRepository(mongoClient.Client, cfg.MongoDB.Database)

	// Create adapters for domain repositories
	walletDomainRepo := adapters.NewWalletRepositoryAdapter(walletRepo)
	accountDomainRepo := adapters.NewAccountRepositoryAdapter(accountRepo)
	currencyDomainRepo := adapters.NewCurrencyRepositoryAdapter(currencyRepo)
	supportedCurrencyDomainRepo := adapters.NewSupportedCurrencyRepositoryAdapter(supportedCurrencyRepo)
	chainDomainRepo := adapters.NewChainRepositoryAdapter(chainRepo)

	// Initialize services (infrastructure layer)
	eventSvc := services.NewEventService(appLogger)
	queueSvc := services.NewQueueService(redisClient, appLogger)

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

	// Initialize RequestManager
	requestManager := httpmanager.Initialize(appLogger)
	appLogger.Info("RequestManager initialized")

	// Initialize use cases (application layer)
	walletUseCase := usecases.NewWalletUseCase(walletDomainRepo, appLogger)
	accountUseCase := usecases.NewAccountUseCase(
		accountDomainRepo,
		walletDomainRepo,
		currencyDomainRepo,
		supportedCurrencyDomainRepo,
		chainDomainRepo,
		blockchainSvc,
		appLogger,
		requestManager,
	)
	portfolioUseCase := usecases.NewPortfolioUseCase(accountDomainRepo, currencyDomainRepo, chainDomainRepo, blockchainSvc, appLogger)

	// Initialize HTTP handlers (interfaces layer)
	walletHandler := httpHandlers.NewWalletHandler(walletUseCase, portfolioUseCase, accountUseCase, eventSvc, appLogger)
	queueHandler := httpHandlers.NewQueueHandler(queueSvc, appLogger)

	// Initialize and setup HTTP router
	router := http.NewRouter(walletHandler, queueHandler, appLogger)
	router.SetupRoutes()

	// // Add this after initializing other clients and before starting servers
	// nodeSocketURL := cfg.NodeServer.URL // You'll need to add this to your config
	// authToken := "your-auth-token"      // Get from your auth service or config
	// userId := "system-wallet-service"   // A system user ID for the service

	// // Initialize the socket connection
	// nodeSocket := socket.NewNodeSocketConnection(nodeSocketURL, authToken, userId)

	// // Add event handlers for events you want to listen to
	// nodeSocket.AddEventHandler("transaction_update", func(data []byte) {
	// 	appLogger.Info("Received transaction update", "data", string(data))
	// 	// Process transaction update
	// })

	// nodeSocket.AddEventHandler("wallet_event", func(data []byte) {
	// 	appLogger.Info("Received wallet event", "data", string(data))
	// 	// Process wallet event
	// })

	// // Connect to the Node.js server
	// err = nodeSocket.Connect()
	// if err != nil {
	// 	appLogger.Error("Failed to connect to Node.js socket server", "error", err)
	// } else {
	// 	appLogger.Info("Connected to Node.js socket server")

	// 	// Emit a test event to confirm connectivity
	// 	testData := map[string]interface{}{
	// 		"service": "wallet-service",
	// 		"status":  "online",
	// 	}

	// 	if emitErr := nodeSocket.EmitEvent("service_online", testData); emitErr != nil {
	// 		appLogger.Error("Failed to emit service_online event", "error", emitErr)
	// 	}
	// }

	// Initialize gRPC handlers
	grpcImportHandler := grpc.NewImportHandler(accountUseCase, eventSvc, supportedCurrencyDomainRepo, appLogger)

	//add socket

	// Initialize gRPC server
	grpcServer, err := grpc.NewGrpcServer(appLogger, grpc.GrpcServerOptions{
		Port:             cfg.GrpcServer.Port,
		EnableTLS:        cfg.GrpcServer.EnableTLS,
		TLSCertFile:      cfg.GrpcServer.TLSCertFile,
		TLSKeyFile:       cfg.GrpcServer.TLSKeyFile,
		EnableReflection: true, // Enable reflection for development
	})
	if err != nil {
		appLogger.Fatal("Failed to create gRPC server", "error", err)
	}

	// Register gRPC services
	grpcServer.RegisterService(func(server *stdgrpc.Server) {
		// Register the wallet service

		// Register the import service
		grpc.RegisterImportServiceServer(server, grpcImportHandler)
		appLogger.Info("Registered ImportService gRPC server")
	})

	// Start gRPC server in a goroutine
	go func() {
		if err := grpcServer.Start(); err != nil {
			appLogger.Fatal("Failed to start gRPC server", "error", err)
		}
	}()
	appLogger.Info("gRPC server started", "port", cfg.GrpcServer.Port)

	// Start HTTP server in a goroutine
	go func() {
		if err := router.Run(cfg.Server.Port); err != nil {
			appLogger.Fatal("Failed to start HTTP server", "error", err)
		}
	}()
	appLogger.Info("HTTP server started", "port", cfg.Server.Port)

	// Wait for interrupt signal to gracefully shut down the servers
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	appLogger.Info("Shutting down servers...")

	// Stop the gRPC server
	grpcServer.Stop()

	// Create a deadline for the shutdown
	_, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()

	appLogger.Info("Servers exited")
}
