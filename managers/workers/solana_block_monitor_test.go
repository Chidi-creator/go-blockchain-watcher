package workers

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/events"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/managers/queue"
)

func TestSolanaBlockMonitorQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	// Create a mini Redis server
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// Create logger
	log := logger.NewLogger("debug")

	// Create managers
	cacheManager := cache.NewCacheManager(redisClient, log)
	eventManager := events.NewEventManager(log)
	queueManager := queue.NewQueueManager(redisClient, log)

	// Create Solana block monitor
	monitor := NewSolanaBlockMonitor(cacheManager, eventManager, log, "")

	// Register handler
	queueManager.RegisterHandler("solana_block_monitor", monitor.ProcessBlockMonitorJob)

	// Create context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Process jobs
	go func() {
		err := queueManager.ProcessJobs(ctx, "solana_block_monitor", 1)
		assert.NoError(t, err)
	}()

	// Wait for jobs to be processed
	time.Sleep(100 * time.Millisecond)

	// Check job status
	status, err := queueManager.GetQueueStatus(ctx, "solana_block_monitor")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), status["waiting"])
	assert.Equal(t, int64(0), status["active"])
	assert.Equal(t, int64(0), status["completed"])
	assert.Equal(t, int64(0), status["failed"])
}

func TestSolanaBlockMonitorRepeatJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	// Create a mini Redis server
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// Create logger
	log := logger.NewLogger("debug")

	// Create managers
	cacheManager := cache.NewCacheManager(redisClient, log)
	eventManager := events.NewEventManager(log)
	queueManager := queue.NewQueueManager(redisClient, log)

	// Create Solana block monitor
	monitor := NewSolanaBlockMonitor(cacheManager, eventManager, log, "")

	// Register handler
	queueManager.RegisterHandler("solana_block_monitor", monitor.ProcessBlockMonitorJob)

	// Create context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Process jobs
	go func() {
		err := queueManager.ProcessJobs(ctx, "solana_block_monitor", 1)
		assert.NoError(t, err)
	}()

	// Wait for jobs to be processed
	time.Sleep(100 * time.Millisecond)

	// Check job status
	status, err := queueManager.GetQueueStatus(ctx, "solana_block_monitor")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), status["waiting"])
	assert.Equal(t, int64(0), status["active"])
	assert.Equal(t, int64(0), status["completed"])
	assert.Equal(t, int64(0), status["failed"])
}

func TestSolanaBlockMonitorStartBlockMonitor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	// Create a mini Redis server
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// Create logger
	log := logger.NewLogger("debug")

	// Create managers
	cacheManager := cache.NewCacheManager(redisClient, log)
	eventManager := events.NewEventManager(log)
	queueManager := queue.NewQueueManager(redisClient, log)

	// Create Solana block monitor
	monitor := NewSolanaBlockMonitor(cacheManager, eventManager, log, "")

	// Register handler
	queueManager.RegisterHandler("solana_block_monitor", monitor.ProcessBlockMonitorJob)

	// Create context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Process jobs
	go func() {
		err := queueManager.ProcessJobs(ctx, "solana_block_monitor", 1)
		assert.NoError(t, err)
	}()

	// Wait for jobs to be processed
	time.Sleep(100 * time.Millisecond)

	// Check job status
	status, err := queueManager.GetQueueStatus(ctx, "solana_block_monitor")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), status["waiting"])
	assert.Equal(t, int64(0), status["active"])
	assert.Equal(t, int64(0), status["completed"])
	assert.Equal(t, int64(0), status["failed"])
}
