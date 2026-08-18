package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"github.com/go-redis/redis/v8"
)

var (
	instance *Client
	once     sync.Once
	mu       sync.Mutex
)

// Client wraps the go-redis client with methods specific to our application
type Client struct {
	rdb    *redis.Client
	logger logger.Logger
}

// GetInstance returns the singleton Redis client instance
func GetInstance(cfg config.RedisConfig, logger logger.Logger) (*Client, error) {
	var initErr error

	once.Do(func() {
		mu.Lock()
		defer mu.Unlock()

		// Configure Redis client
		rdb := redis.NewClient(&redis.Options{
			Addr:               fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
			Username:           cfg.Username,
			Password:           cfg.Password,
			DB:                 cfg.DB,
			PoolSize:           cfg.PoolSize,
			MinIdleConns:       cfg.MinIdleConns,
			DialTimeout:        cfg.DialTimeout,
			ReadTimeout:        cfg.ReadTimeout,
			WriteTimeout:       cfg.WriteTimeout,
			PoolTimeout:        cfg.PoolTimeout,
			MaxConnAge:         cfg.ConnMaxLifetime,
			MaxRetries:         3,
			MinRetryBackoff:    200 * time.Millisecond,
			MaxRetryBackoff:    2 * time.Second,
			IdleTimeout:        240 * time.Second,
			IdleCheckFrequency: 60 * time.Second,
		})

		logger.Info("Initializing Redis client",
			"addr", fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
			"poolSize", cfg.PoolSize)

		// Verify connection
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if _, err := rdb.Ping(ctx).Result(); err != nil {
			logger.Error("Failed to connect to Redis", "error", err)
			initErr = err
			return
		}

		logger.Info("Successfully connected to Redis")
		instance = &Client{rdb: rdb, logger: logger}
	})

	return instance, initErr
}

// Close closes the Redis client connection (use carefully with singleton)
func (c *Client) Close() error {
	mu.Lock()
	defer mu.Unlock()

	if instance != nil {
		err := c.rdb.Close()
		instance = nil // Reset instance to allow reinitialization if needed
		return err
	}
	return nil
}

// GetRedisClient returns the underlying redis client
func (c *Client) GetRedisClient() *redis.Client {
	return c.rdb
}
