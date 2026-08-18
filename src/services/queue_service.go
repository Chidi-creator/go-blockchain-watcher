package services

import (
	"context"

	"bitbucket.org/zapspace/zap-go-server/config/redis"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/managers/queue"
)

// QueueService provides queue functionality
type QueueService struct {
	queueManager *queue.QueueManager
	logger       logger.Logger
}

// NewQueueService creates a new QueueService
func NewQueueService(redisClient *redis.Client, logger logger.Logger) *QueueService {
	queueManager := queue.NewQueueManager(redisClient.GetRedisClient(), logger)
	return &QueueService{
		queueManager: queueManager,
		logger:       logger,
	}
}

// AddJob adds a job to a queue
func (s *QueueService) AddJob(ctx context.Context, queueName string, data map[string]interface{}, options map[string]interface{}) (string, error) {
	jobID, err := s.queueManager.AddJob(ctx, queueName, data, options)
	if err != nil {
		s.logger.Error("Failed to add job to queue", "queueName", queueName, "error", err)
		return "", err
	}
	return jobID, nil
}

// RegisterHandler registers a handler function for a queue
func (s *QueueService) RegisterHandler(queueName string, handler func(ctx context.Context, data map[string]interface{}) error) {
	s.queueManager.RegisterHandler(queueName, handler)
}

// ProcessJobs starts processing jobs for all registered queues
func (s *QueueService) ProcessJobs(ctx context.Context, queueName string, concurrency int) error {
	return s.queueManager.ProcessJobs(ctx, queueName, concurrency)
}

// GetQueueStatus returns the status of a queue
func (s *QueueService) GetQueueStatus(ctx context.Context, queueName string) (map[string]int64, error) {
	return s.queueManager.GetQueueStatus(ctx, queueName)
}

// ClearQueue clears all jobs from a queue
func (s *QueueService) ClearQueue(ctx context.Context, queueName string) error {
	return s.queueManager.ClearQueue(ctx, queueName)
}

// ScheduleJob schedules a job with options map
// This method explicitly matches the signature expected by QueueServiceWithSchedule
func (s *QueueService) ScheduleJob(ctx context.Context, queueName string, data map[string]interface{}, options map[string]interface{}) (string, error) {
	s.logger.Info("Scheduling job with options", "queueName", queueName)
	return s.queueManager.AddJob(ctx, queueName, data, options)
}

// ScheduleJobFlexible schedules a job
// This method accepts both integer seconds for legacy compatibility and a complex options map for Node.js compatibility
func (s *QueueService) ScheduleJobFlexible(ctx context.Context, queueName string, data map[string]interface{}, optsOrDelay interface{}) (string, error) {
	// Handle both delay seconds (int) and options map (map[string]interface{})
	switch opts := optsOrDelay.(type) {
	case int:
		// Legacy delay seconds int parameter
		jobOpts := map[string]interface{}{
			"delay": opts * 1000, // Convert to milliseconds for Bull compatibility
		}
		return s.queueManager.AddJob(ctx, queueName, data, jobOpts)
	case map[string]interface{}:
		// New style with full options map
		s.logger.Info("Scheduling job with full options", "queueName", queueName, "options", opts)
		return s.queueManager.AddJob(ctx, queueName, data, opts)
	default:
		s.logger.Error("Invalid options type for ScheduleJob", "type", optsOrDelay)
		return "", nil
	}
}

// ScheduleJobWithOptions schedules a job with complex options
// This allows compatibility with Node.js Bull queue format
func (s *QueueService) ScheduleJobWithOptions(ctx context.Context, queueName string, data map[string]interface{}, options map[string]interface{}) (string, error) {
	jobID, err := s.queueManager.AddJob(ctx, queueName, data, options)
	if err != nil {
		s.logger.Error("Failed to schedule job with options", "queueName", queueName, "error", err)
		return "", err
	}
	s.logger.Info("Scheduled job with options", "queueName", queueName, "jobId", jobID)
	return jobID, nil
}

// GetQueueManager returns the underlying queue manager
func (s *QueueService) GetQueueManager() *queue.QueueManager {
	return s.queueManager
}
