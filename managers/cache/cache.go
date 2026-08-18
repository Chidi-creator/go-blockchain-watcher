package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

// CacheManager handles caching data in Redis
type CacheManager struct {
	redisClient *redis.Client
	logger      logger.Logger
}

// ResponseData represents cached data with its value
type ResponseData struct {
	Data interface{} `json:"data"`
}

// NewCacheManager creates a new cache manager
func NewCacheManager(redisClient *redis.Client, logger logger.Logger) *CacheManager {
	return &CacheManager{
		redisClient: redisClient,
		logger:      logger,
	}
}

// Set stores a value in the cache with the given key and expiry time in seconds
func (cm *CacheManager) Set(ctx context.Context, key string, value interface{}, expirySeconds int) error {
	data := ResponseData{
		Data: value,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		cm.logger.Error("Failed to marshal cache data", "key", key, "error", err)
		return fmt.Errorf("failed to marshal cache data: %w", err)
	}

	expiry := time.Duration(expirySeconds) * time.Second
	err = cm.redisClient.Set(ctx, key, string(jsonData), expiry).Err()
	if err != nil {
		cm.logger.Error("Failed to set cache value", "key", key, "error", err)
		return fmt.Errorf("failed to set cache value: %w", err)
	}

	cm.logger.Debug("Cache set successfully", "key", key, "expirySeconds", expirySeconds)
	return nil
}

// Get retrieves a value from the cache with the given key
func (cm *CacheManager) Get(ctx context.Context, key string) (ResponseData, error) {
	var response ResponseData

	val, err := cm.redisClient.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			// Key does not exist
			cm.logger.Debug("Cache miss", "key", key)
			return response, nil
		}
		cm.logger.Error("Failed to get cache value", "key", key, "error", err)
		return response, fmt.Errorf("failed to get cache value: %w", err)
	}

	err = json.Unmarshal([]byte(val), &response)
	if err != nil {
		cm.logger.Error("Failed to unmarshal cache data", "key", key, "error", err)
		return response, fmt.Errorf("failed to unmarshal cache data: %w", err)
	}

	cm.logger.Debug("Cache hit", "key", key)
	return response, nil
}

// Delete removes a key from the cache
func (cm *CacheManager) Delete(ctx context.Context, key string) error {
	err := cm.redisClient.Del(ctx, key).Err()
	if err != nil {
		cm.logger.Error("Failed to delete cache key", "key", key, "error", err)
		return fmt.Errorf("failed to delete cache key: %w", err)
	}

	cm.logger.Debug("Cache key deleted", "key", key)
	return nil
}
