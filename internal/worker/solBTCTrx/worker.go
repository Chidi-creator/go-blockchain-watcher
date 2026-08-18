package solBTCTrx

import (
	"context"
	"sync"

	"bitbucket.org/zapspace/zap-go-server/config/redis"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/internal/worker"
	"bitbucket.org/zapspace/zap-go-server/internal/worker/btc"
	"bitbucket.org/zapspace/zap-go-server/internal/worker/sol"
	trx "bitbucket.org/zapspace/zap-go-server/internal/worker/trx"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/http"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/usecases"
	"go.mongodb.org/mongo-driver/mongo"
)

// Worker represents the solBTCTrx worker service that runs multiple workers
type Worker struct {
	cfg              *config.Config
	solConcurrency   int
	btcConcurrency   int
	trxConcurrency   int
	logger           logger.Logger
	redisClient      *redis.Client
	queueSvc         *services.QueueService
	mongoClient      *mongo.Client
	cacheManager     *cache.CacheManager
	accountUsecase   *usecases.AccountUseCase
	client           *http.RequestManager
	solScheduledTime int
	btcScheduledTime int
	trxScheduledTime int
}

// New creates a new SolBTCTrx worker
func New(cfg *config.Config, logger logger.Logger, redisClient *redis.Client, queueSvc *services.QueueService, mongoClient *mongo.Client, cacheManager *cache.CacheManager, accountUsecase *usecases.AccountUseCase, client *http.RequestManager) worker.Worker {
	return &Worker{
		cfg:              cfg,
		logger:           logger,
		redisClient:      redisClient,
		queueSvc:         queueSvc,
		mongoClient:      mongoClient,
		cacheManager:     cacheManager,
		accountUsecase:   accountUsecase,
		client:           client,
		solConcurrency:   cfg.SolConcurrency,
		btcConcurrency:   cfg.BTCConcurrency,
		trxConcurrency:   cfg.TRXConcurrency,
		solScheduledTime: cfg.SolScheduledTime,
		btcScheduledTime: cfg.BTCScheduledTime,
		trxScheduledTime: cfg.TRXScheduledTime,
	}
}

// Start initiates all workers concurrently and waits for them to complete
func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info("Starting BTC + TRX + SOL Worker...")

	// Create a wait group to track all goroutines
	var wg sync.WaitGroup

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Track any errors from the workers
	errChan := make(chan error, 2)

	// Start BTC Worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.logger.Info("Starting BTC Worker...")
		btcWorker := btc.New(w.cfg, w.btcConcurrency, w.logger, w.redisClient.GetRedisClient(), w.queueSvc, w.mongoClient, w.cacheManager, w.accountUsecase, w.client, w.btcScheduledTime)
		err := btcWorker.Start(ctx)
		if err != nil {
			w.logger.Error("BTC Worker failed", "error", err)
			errChan <- err
			cancel() // Cancel all other workers if this one fails
		}
	}()

	// Start SOL Worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.logger.Info("Starting SOL Worker...")
		solWorker := sol.New(w.cfg, w.solConcurrency, w.logger, w.redisClient.GetRedisClient(), w.queueSvc, w.mongoClient, w.cacheManager, w.accountUsecase, w.client, w.solScheduledTime)
		err := solWorker.Start(ctx)
		if err != nil {
			w.logger.Error("SOL Worker failed", "error", err)
			errChan <- err
			cancel() // Cancel all other workers if this one fails
		}
	}()

	// Start TRX Worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.logger.Info("Starting TRX Worker...")
		trxWorker := trx.New(w.cfg, w.trxConcurrency, w.logger, w.redisClient.GetRedisClient(), w.queueSvc, w.mongoClient, w.cacheManager, w.accountUsecase, w.client, w.trxScheduledTime)
		err := trxWorker.Start(ctx)
		if err != nil {
			w.logger.Error("TRX Worker failed", "error", err)
			errChan <- err
			cancel() // Cancel all other workers if this one fails
		}
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
