package workers

import (
	"context"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/redis"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/events"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

// Config holds configuration for blockchain workers
type BlockchainWorkersConfig struct {
	SolanaRpcURL  string
	TronAPIURL    string
	TronAPIKey    string
	EnableBitcoin bool
	EnableEVM     bool
	EnableSolana  bool
	EnableTron    bool
}

// QueueServiceWithSchedule defines the interface needed for blockchain workers
type QueueServiceWithSchedule interface {
	RegisterHandler(queueName string, handler func(ctx context.Context, data map[string]interface{}) error)
	ProcessJobs(ctx context.Context, queueName string, concurrency int) error
	ScheduleJob(ctx context.Context, queueName string, data map[string]interface{}, options map[string]interface{}) (string, error)
}

// RegisterBlockchainWorkers registers all blockchain workers with the queue service
func RegisterBlockchainWorkers(
	ctx context.Context,
	queueService interface{},
	redisClient *redis.Client,
	logger logger.Logger,
	config BlockchainWorkersConfig,
) error {
	logger.Info("Starting blockchain workers registration...")

	// Create cache manager
	cacheManager := cache.NewCacheManager(redisClient.GetRedisClient(), logger)

	// Create event manager
	eventManager := events.NewEventManager(logger)

	// Ensure queue service implements necessary interface
	qs, ok := queueService.(QueueServiceWithSchedule)
	if !ok {
		logger.Error("Queue service does not implement required interfaces")
		return nil
	}

	// Subscribe to block events
	eventManager.Subscribe("handleBlockFound", func(data interface{}) {
		logger.Info("Block found event received", "data", data)
	})

	// Track registration errors
	var registrationErrors []error

	// Register workers in parallel
	if config.EnableBitcoin {
		go func() {
			logger.Info("Registering Bitcoin block monitor...")
			if err := RegisterBitcoinBlockMonitor(ctx, qs, cacheManager, eventManager, logger); err != nil {
				logger.Error("Failed to register Bitcoin block monitor", "error", err)
				registrationErrors = append(registrationErrors, err)
			} else {
				logger.Info("Bitcoin block monitor registered successfully")
			}
		}()
	}

	if config.EnableEVM {
		go func() {
			logger.Info("Registering EVM block monitor...")
			if err := RegisterEVMBlockMonitor(ctx, qs, cacheManager, eventManager, logger); err != nil {
				logger.Error("Failed to register EVM block monitor", "error", err)
				registrationErrors = append(registrationErrors, err)
			} else {
				logger.Info("EVM block monitor registered successfully")
			}
		}()
	}

	if config.EnableSolana {
		go func() {
			logger.Info("Registering Solana block monitor...")
			if err := RegisterSolanaBlockMonitor(ctx, qs, cacheManager, eventManager, logger, config.SolanaRpcURL); err != nil {
				logger.Error("Failed to register Solana block monitor", "error", err)
				registrationErrors = append(registrationErrors, err)
			} else {
				logger.Info("Solana block monitor registered successfully")
			}
		}()
	}

	if config.EnableTron {
		go func() {
			logger.Info("Registering Tron block monitor...")
			if err := RegisterTronBlockMonitor(ctx, qs, cacheManager, eventManager, logger, config.TronAPIURL, config.TronAPIKey); err != nil {
				logger.Error("Failed to register Tron block monitor", "error", err)
				registrationErrors = append(registrationErrors, err)
			} else {
				logger.Info("Tron block monitor registered successfully")
			}
		}()
	}

	// Wait a short time for initial registrations to complete
	time.Sleep(2 * time.Second)

	if len(registrationErrors) > 0 {
		logger.Warn("Some blockchain workers failed to register", "errorCount", len(registrationErrors))
	} else {
		logger.Info("All blockchain workers registered successfully")
	}

	return nil
}
