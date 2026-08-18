package evmChangeNow

import (
	"context"
	"sync"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/redis"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/internal/worker"
	"bitbucket.org/zapspace/zap-go-server/internal/worker/evm"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/usecases"
	"go.mongodb.org/mongo-driver/mongo"
)

// Worker represents the evmChangeNow worker service that runs multiple workers
type Worker struct {
	cfg                  *config.Config
	evmConcurrency       int
	changenowConcurrency int
	logger               logger.Logger
	redisClient          *redis.Client
	queueSvc             *services.QueueService
	mongoClient          *mongo.Client
	cacheManager         *cache.CacheManager
	orderUsecase         *usecases.OrderUseCase
}

// New creates a new evmChangeNow worker
func New(cfg *config.Config, evmConcurrency, changenowConcurrency int, logger logger.Logger, redisClient *redis.Client, queueSvc *services.QueueService, mongoClient *mongo.Client, cacheManager *cache.CacheManager, orderUsecase *usecases.OrderUseCase) worker.Worker {
	return &Worker{
		cfg:                  cfg,
		evmConcurrency:       evmConcurrency,
		changenowConcurrency: changenowConcurrency,
		logger:               logger,
		redisClient:          redisClient,
		queueSvc:             queueSvc,
		mongoClient:          mongoClient,
		cacheManager:         cacheManager,
		orderUsecase:         orderUsecase,
	}
}

// Start initiates all workers concurrently and waits for them to complete
func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info("Starting EVM + ChangeNow Worker...")

	// Create a wait group to track all goroutines
	var wg sync.WaitGroup

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Track any errors from the workers
	errChan := make(chan error, 2)

	// Start EVM Worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.logger.Info("Starting EVM Worker...")
		evmWorker := evm.New(w.cfg, w.evmConcurrency, w.logger, w.redisClient.GetRedisClient(), w.queueSvc, w.mongoClient, w.cacheManager, w.cfg.EVMScheduledTime)
		err := evmWorker.Start(ctx)
		if err != nil {
			w.logger.Error("EVM Worker failed", "error", err)
			errChan <- err
			cancel() // Cancel all other workers if this one fails
		}
	}()

	// // Start ChangeNow Worker
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	w.logger.Info("Starting ChangeNow Worker...")
	// 	changenowWorker := changenow.New(w.cfg, w.changenowConcurrency, w.logger, w.redisClient.GetRedisClient(), w.queueSvc, w.mongoClient, w.cacheManager, w.orderUsecase)
	// 	err := changenowWorker.Start(ctx)
	// 	if err != nil {
	// 		w.logger.Error("ChangeNow Worker failed", "error", err)
	// 		errChan <- err
	// 		cancel() // Cancel all other workers if this one fails
	// 	}
	// }()

	// Start health checker
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.runHealthCheck(ctx)
	}()

	// Wait for all workers to finish
	wg.Wait()

	// Check if any errors occurred
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// runHealthCheck performs periodic health checks
func (w *Worker) runHealthCheck(ctx context.Context) {
	w.logger.Info("Starting health checker for evmChangeNow worker")

	// Health check every 30 seconds
	healthTicker := time.NewTicker(30 * time.Second)
	defer healthTicker.Stop()

	// Resource monitoring every 5 minutes
	resourceTicker := time.NewTicker(5 * time.Minute)
	defer resourceTicker.Stop()

	// Log initial resource usage
	w.logger.Info("Initial resource monitoring started - will log every 5 minutes")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Health checker shutting down")
			return
		case <-healthTicker.C:
			w.logger.Debug("Health check: evmChangeNow worker is running")
		case <-resourceTicker.C:
			w.logResourceUsage()
		}
	}
}

// logResourceUsage logs detailed resource usage information
func (w *Worker) logResourceUsage() {
	w.logger.Info("=== RESOURCE MONITORING REPORT ===")

	// Basic info
	w.logger.Info("Worker Configuration",
		"evmConcurrency", w.evmConcurrency,
		"changenowConcurrency", w.changenowConcurrency)

	// This would require accessing the EVM worker instance
	// For now, we'll log basic info and add a TODO for full monitoring
	w.logger.Info("Resource monitoring active - consider adding EVM worker reference for detailed stats")
	w.logger.Info("Recommendation: Monitor goroutine count, memory usage, and API rate limits")

	w.logger.Info("=== END RESOURCE REPORT ===")
}
