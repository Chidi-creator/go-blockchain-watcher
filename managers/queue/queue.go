package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

const (
	// Bull queue key prefixes
	bullKeyPrefix = "bull"
	waitKey       = "wait"
	activeKey     = "active"
	delayedKey    = "delayed"
	completedKey  = "completed"
	failedKey     = "failed"
	stallKey      = "stalled"
	priorityKey   = "priority"
	eventKey      = "events"
	repeatKey     = "repeat" // Prefix for Bull repeat jobs

	// Default values
	defaultJobOptions = "{}"
	defaultAttempts   = 1
)

// JobState represents the state of a job
type JobState string

const (
	JobStateWaiting   JobState = "waiting"
	JobStateActive    JobState = "active"
	JobStateCompleted JobState = "completed"
	JobStateFailed    JobState = "failed"
	JobStateDelayed   JobState = "delayed"
	JobStateStalled   JobState = "stalled"
)

// JobData contains job data and metadata
type JobData struct {
	ID          string                 `json:"id"`
	QueueName   string                 `json:"queueName"`
	Data        map[string]interface{} `json:"data"`
	Opts        map[string]interface{} `json:"opts,omitempty"`
	Timestamp   int64                  `json:"timestamp"`
	ProcessedOn int64                  `json:"processedOn,omitempty"`
	FinishedOn  int64                  `json:"finishedOn,omitempty"`
	Attempts    int                    `json:"attempts"`
	State       JobState               `json:"state"`
	Name        string                 `json:"name,omitempty"` // For Bull repeat jobs
}

// BullRepeatData contains data about a repeatable job in Bull format
type BullRepeatData struct {
	ID        string                 `json:"id"`
	QueueName string                 `json:"queueName"`
	Data      map[string]interface{} `json:"data"`
	Name      string                 `json:"name"`
	Opts      map[string]interface{} `json:"opts"`
	Every     int                    `json:"every"`
	EndDate   int64                  `json:"endDate,omitempty"`
	Tz        string                 `json:"tz,omitempty"`
}

// JobHandler represents a function that processes a job
type JobHandler func(ctx context.Context, jobData map[string]interface{}) error

// QueueManager manages job queues using Redis and Bull compatibility
type QueueManager struct {
	redisClient *redis.Client
	logger      logger.Logger
	handlers    map[string]JobHandler
}

// NewQueueManager creates a new QueueManager
func NewQueueManager(redisClient *redis.Client, logger logger.Logger) *QueueManager {
	return &QueueManager{
		redisClient: redisClient,
		logger:      logger,
		handlers:    make(map[string]JobHandler),
	}
}

// getBullKey returns the Redis key for a Bull queue
func getBullKey(queueName, suffix string) string {
	return fmt.Sprintf("%s:%s:%s", bullKeyPrefix, queueName, suffix)
}

// isRepeatJobID checks if a job ID follows the Bull repeat job pattern
func isRepeatJobID(jobID string) bool {
	// Bull repeat jobs typically have IDs with format: "repeat:{hash}:{timestamp}"
	return strings.HasPrefix(jobID, "repeat:")
}

// getRepeatJobKey returns the Redis key for a Bull repeat job
func getRepeatJobKey(queueName, jobID string) string {
	// For Bull repeat jobs, we need to look up the job data differently
	// Format: bull:queueName:repeat:jobId
	return fmt.Sprintf("%s:%s:%s:%s", bullKeyPrefix, queueName, repeatKey, jobID)
}

// parseRepeatJobID extracts components from a Bull repeat job ID
func parseRepeatJobID(jobID string) (key string, timestamp string, ok bool) {
	// Format: repeat:{hash}:{timestamp}
	parts := strings.Split(jobID, ":")
	if len(parts) == 3 && parts[0] == "repeat" {
		return parts[1], parts[2], true
	}
	return "", "", false
}

// AddJob adds a job to a queue
func (qm *QueueManager) AddJob(ctx context.Context, queueName string, data map[string]interface{}, opts map[string]interface{}) (string, error) {
	// Check if this is a repeat job
	repeatOpts, isRepeat := opts["repeat"].(map[string]interface{})

	var jobID string
	if isRepeat {
		// For repeat jobs, use a special ID format
		jobID = fmt.Sprintf("repeat:%s:%d", generateHash(queueName, data), time.Now().Unix())

		// Create repeat job data
		repeatJob := BullRepeatData{
			ID:        jobID,
			QueueName: queueName,
			Data:      data,
			Opts:      opts,
			Every:     int(repeatOpts["every"].(float64)),
		}

		// Convert to JSON
		repeatJobBytes, err := json.Marshal(repeatJob)
		if err != nil {
			return "", fmt.Errorf("failed to marshal repeat job: %w", err)
		}

		// Store repeat job definition
		repeatJobKey := getRepeatJobKey(queueName, jobID)
		if err := qm.redisClient.Set(ctx, repeatJobKey, string(repeatJobBytes), 0).Err(); err != nil {
			return "", fmt.Errorf("failed to store repeat job: %w", err)
		}

		qm.logger.Info("Added repeat job", "queueName", queueName, "jobID", jobID, "every", repeatJob.Every)
	} else {
		// Regular job
		jobID = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	// Create job object
	job := JobData{
		ID:        jobID,
		QueueName: queueName,
		Data:      data,
		Opts:      opts,
		Timestamp: time.Now().Unix(),
		Attempts:  defaultAttempts,
		State:     JobStateWaiting,
	}

	// Convert job to JSON
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return "", fmt.Errorf("failed to marshal job: %w", err)
	}

	// Start a Redis transaction
	pipe := qm.redisClient.Pipeline()

	// Add job to the wait list
	waitKey := getBullKey(queueName, waitKey)
	pipe.RPush(ctx, waitKey, jobID)

	// Store job data
	jobKey := fmt.Sprintf("%s:%s:%s", bullKeyPrefix, queueName, jobID)
	pipe.Set(ctx, jobKey, string(jobBytes), 0)

	// Add event for Bull to pick up
	eventKey := getBullKey(queueName, eventKey)
	pipe.Publish(ctx, eventKey, "added")

	_, err = pipe.Exec(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to execute Redis pipeline: %w", err)
	}

	qm.logger.Info("Added job to queue", "queueName", queueName, "jobID", jobID)
	return jobID, nil
}

// generateHash creates a simple hash for repeat job IDs
func generateHash(queueName string, data map[string]interface{}) string {
	// Create a string representation of the data
	dataStr := fmt.Sprintf("%s:%v", queueName, data)

	// Use a simple hash function
	hash := 0
	for i := 0; i < len(dataStr); i++ {
		hash = 31*hash + int(dataStr[i])
	}

	return fmt.Sprintf("%x", hash)
}

// GetJob retrieves a job by its ID
func (qm *QueueManager) GetJob(ctx context.Context, queueName, jobID string) (*JobData, error) {
	// Check if this is a Bull repeat job ID (format: repeat:{hash}:{timestamp})
	if isRepeatJobID(jobID) {
		return qm.getRepeatJob(ctx, queueName, jobID)
	}

	// Regular job processing
	jobKey := fmt.Sprintf("%s:%s:%s", bullKeyPrefix, queueName, jobID)
	jobJSON, err := qm.redisClient.Get(ctx, jobKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("job not found")
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	var job JobData
	if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	return &job, nil
}

// getRepeatJob handles retrieving Bull's repeat jobs
func (qm *QueueManager) getRepeatJob(ctx context.Context, queueName, jobID string) (*JobData, error) {
	// Parse the repeat job ID components
	hash, timestamp, ok := parseRepeatJobID(jobID)
	if !ok {
		return nil, fmt.Errorf("invalid repeat job ID format: %s", jobID)
	}

	qm.logger.Debug("Processing Bull repeat job",
		"queueName", queueName,
		"jobID", jobID,
		"timestamp", timestamp) // Use timestamp in a log to avoid linter warning

	// First try to get the repeat job definition
	repeatJobKey := getBullKey(queueName, "repeat:"+hash)

	// Try different data access methods
	repeatJobData, err := qm.redisClient.HGetAll(ctx, repeatJobKey).Result()
	if err != nil {
		// If HGETALL fails, try GET as a fallback
		jobJSON, getErr := qm.redisClient.Get(ctx, repeatJobKey).Result()
		if getErr != nil {
			if getErr == redis.Nil {
				qm.logger.Warn("Repeat job not found, creating synthetic job",
					"queueName", queueName,
					"jobID", jobID)

				// Create synthetic job data when we can't find the original
				return &JobData{
					ID:        jobID,
					QueueName: queueName,
					Data:      map[string]interface{}{},
					Opts:      map[string]interface{}{},
					Timestamp: time.Now().Unix(),
					Attempts:  defaultAttempts,
					State:     JobStateWaiting,
				}, nil
			}
			return nil, fmt.Errorf("failed to get repeat job: %w", getErr)
		}

		// Try to unmarshal the JSON data
		var repeatJob BullRepeatData
		if err := json.Unmarshal([]byte(jobJSON), &repeatJob); err != nil {
			qm.logger.Error("Failed to unmarshal repeat job JSON",
				"queueName", queueName,
				"jobID", jobID,
				"error", err)

			// Create synthetic job data on error
			return &JobData{
				ID:        jobID,
				QueueName: queueName,
				Data:      map[string]interface{}{},
				Opts:      map[string]interface{}{},
				Timestamp: time.Now().Unix(),
				Attempts:  defaultAttempts,
				State:     JobStateWaiting,
			}, nil
		}

		// Convert BullRepeatData to JobData
		return &JobData{
			ID:        jobID,
			QueueName: queueName,
			Data:      repeatJob.Data,
			Opts:      repeatJob.Opts,
			Timestamp: time.Now().Unix(),
			Attempts:  defaultAttempts,
			State:     JobStateWaiting,
			Name:      repeatJob.Name,
		}, nil
	}

	// If we got data from HGETALL
	if len(repeatJobData) > 0 {
		// Parse the data JSON field
		var jobData map[string]interface{}
		if dataStr, ok := repeatJobData["data"]; ok && dataStr != "" {
			if err := json.Unmarshal([]byte(dataStr), &jobData); err != nil {
				qm.logger.Error("Failed to unmarshal repeat job data field",
					"queueName", queueName,
					"jobID", jobID,
					"error", err)
				jobData = map[string]interface{}{}
			}
		} else {
			jobData = map[string]interface{}{}
		}

		// Parse the options
		var jobOpts map[string]interface{}
		if optsStr, ok := repeatJobData["opts"]; ok && optsStr != "" {
			if err := json.Unmarshal([]byte(optsStr), &jobOpts); err != nil {
				qm.logger.Error("Failed to unmarshal repeat job opts field",
					"queueName", queueName,
					"jobID", jobID,
					"error", err)
				jobOpts = map[string]interface{}{}
			}
		} else {
			jobOpts = map[string]interface{}{}
		}

		// Convert to JobData
		return &JobData{
			ID:        jobID,
			QueueName: queueName,
			Data:      jobData,
			Opts:      jobOpts,
			Timestamp: time.Now().Unix(),
			Attempts:  defaultAttempts,
			State:     JobStateWaiting,
			Name:      repeatJobData["name"],
		}, nil
	}

	// If all else fails, return a synthetic job
	qm.logger.Warn("Creating synthetic job for repeat job with no data",
		"queueName", queueName,
		"jobID", jobID)

	return &JobData{
		ID:        jobID,
		QueueName: queueName,
		Data:      map[string]interface{}{},
		Opts:      map[string]interface{}{},
		Timestamp: time.Now().Unix(),
		Attempts:  defaultAttempts,
		State:     JobStateWaiting,
	}, nil
}

// RegisterHandler registers a handler function for a queue
func (qm *QueueManager) RegisterHandler(queueName string, handler JobHandler) {
	qm.handlers[queueName] = handler
	qm.logger.Info("Registered handler for queue", "queueName", queueName)
}

// ProcessJobs starts processing jobs for a queue
// In your queue manager package (queue/queue.go)
func (qm *QueueManager) ProcessJobs(ctx context.Context, queueName string, concurrency int) error {
	qm.logger.Info("Starting job processing",
		"queue", queueName,
		"concurrency", concurrency)

	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			qm.logger.Debug("Worker started", "worker", workerID)
			for {
				result, err := qm.redisClient.BLPop(ctx, 0, queueName).Result()
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						qm.logger.Info("Worker exiting due to context cancellation", "worker", workerID)
						return
					}
					qm.logger.Error("BLPop failed", "worker", workerID, "error", err)
					continue
				}

				var data map[string]interface{}
				if err := json.Unmarshal([]byte(result[1]), &data); err != nil {
					qm.logger.Error("Failed to unmarshal job", "worker", workerID, "error", err)
					continue
				}

				qm.logger.Debug("Processing job", "worker", workerID, "data", data)

				handler, exists := qm.handlers[queueName]
				if !exists {
					qm.logger.Error("No handler for queue", "queue", queueName)
					continue
				}

				if err := handler(ctx, data); err != nil {
					qm.logger.Error("Job handler failed", "worker", workerID, "error", err)
				}
			}
		}(i)
	}
	return nil
}

// processQueue continuously processes jobs from a queue
func (qm *QueueManager) processQueue(ctx context.Context, queueName string, handler JobHandler) {
	waitKey := getBullKey(queueName, waitKey)
	activeKey := getBullKey(queueName, activeKey)

	// Create a separate context for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	defer shutdownCancel()

	// Track active jobs
	activeJobs := make(map[string]bool)
	var activeJobsMutex sync.Mutex

	// Start a goroutine to monitor context cancellation
	go func() {
		<-ctx.Done()
		qm.logger.Info("Context canceled, initiating graceful shutdown", "queueName", queueName)
		shutdownCancel()
	}()

	for {
		select {
		case <-shutdownCtx.Done():
			// Graceful shutdown - wait for active jobs to complete
			activeJobsMutex.Lock()
			activeJobCount := len(activeJobs)
			activeJobsMutex.Unlock()

			if activeJobCount > 0 {
				qm.logger.Info("Waiting for active jobs to complete during shutdown",
					"queueName", queueName,
					"activeJobCount", activeJobCount)

				// Give jobs a chance to complete
				time.Sleep(5 * time.Second)
			}

			qm.logger.Info("Queue processor shutting down", "queueName", queueName)
			return

		default:
			// Move job from wait to active queue with BLPOP and RPUSH
			result, err := qm.redisClient.BLPop(ctx, 5*time.Second, waitKey).Result()
			if err != nil {
				if err != redis.Nil {
					qm.logger.Error("Error getting job from wait queue", "queueName", queueName, "error", err)
				}
				continue
			}

			if len(result) < 2 {
				continue
			}

			jobID := result[1]

			// Track this job as active
			activeJobsMutex.Lock()
			activeJobs[jobID] = true
			activeJobsMutex.Unlock()

			// Get job data
			job, err := qm.GetJob(ctx, queueName, jobID)
			if err != nil {
				qm.logger.Error("Error getting job data", "queueName", queueName, "jobID", jobID, "error", err)

				// Remove from active jobs
				activeJobsMutex.Lock()
				delete(activeJobs, jobID)
				activeJobsMutex.Unlock()

				continue
			}

			// Update job state
			job.State = JobStateActive
			job.ProcessedOn = time.Now().Unix()
			jobBytes, _ := json.Marshal(job)

			// Skip job state storage for repeat jobs as it might fail
			if !isRepeatJobID(jobID) {
				// Start a Redis transaction
				pipe := qm.redisClient.Pipeline()

				// Update job data
				jobKey := fmt.Sprintf("%s:%s:%s", bullKeyPrefix, queueName, jobID)
				pipe.Set(ctx, jobKey, string(jobBytes), 0)

				// Add to active set
				pipe.RPush(ctx, activeKey, jobID)

				// Execute transaction
				if _, err := pipe.Exec(ctx); err != nil {
					qm.logger.Error("Error updating job state", "queueName", queueName, "jobID", jobID, "error", err)

					// Remove from active jobs
					activeJobsMutex.Lock()
					delete(activeJobs, jobID)
					activeJobsMutex.Unlock()

					continue
				}
			} else {
				qm.logger.Info("Processing repeat job", "queueName", queueName, "jobID", jobID)
			}

			// Process the job
			err = handler(ctx, job.Data)

			// Update job state based on processing result
			if err != nil {
				job.State = JobStateFailed
				qm.logger.Error("Job processing failed", "queueName", queueName, "jobID", jobID, "error", err)
			} else {
				job.State = JobStateCompleted
				qm.logger.Info("Job completed successfully", "queueName", queueName, "jobID", jobID)
			}

			// Handle repeat jobs
			if isRepeatJobID(jobID) {
				// Reschedule the repeat job
				if err := qm.rescheduleRepeatJob(ctx, queueName, jobID); err != nil {
					qm.logger.Error("Error rescheduling repeat job", "queueName", queueName, "jobID", jobID, "error", err)
				}

				// Remove from active jobs
				activeJobsMutex.Lock()
				delete(activeJobs, jobID)
				activeJobsMutex.Unlock()

				continue
			}

			job.FinishedOn = time.Now().Unix()
			jobBytes, _ = json.Marshal(job)

			// Start another Redis transaction
			pipe := qm.redisClient.Pipeline()

			// Update job data
			jobKey := fmt.Sprintf("%s:%s:%s", bullKeyPrefix, queueName, jobID)
			pipe.Set(ctx, jobKey, string(jobBytes), 0)

			// Remove from active set
			pipe.LRem(ctx, activeKey, 0, jobID)

			// Add to completed or failed set
			if err != nil {
				failedKey := getBullKey(queueName, failedKey)
				pipe.LPush(ctx, failedKey, jobID)
			} else {
				completedKey := getBullKey(queueName, completedKey)
				pipe.LPush(ctx, completedKey, jobID)
			}

			// Execute transaction
			if _, err := pipe.Exec(ctx); err != nil {
				qm.logger.Error("Error finalizing job", "queueName", queueName, "jobID", jobID, "error", err)
			}

			// Remove from active jobs
			activeJobsMutex.Lock()
			delete(activeJobs, jobID)
			activeJobsMutex.Unlock()
		}
	}
}

// rescheduleRepeatJob reschedules a repeat job after it's processed
func (qm *QueueManager) rescheduleRepeatJob(ctx context.Context, queueName, jobID string) error {
	// Parse the repeat job ID components
	hash, _, ok := parseRepeatJobID(jobID)
	if !ok {
		return fmt.Errorf("invalid repeat job ID format: %s", jobID)
	}

	// Get the repeat job definition
	repeatJobKey := getRepeatJobKey(queueName, jobID)
	repeatJobJSON, err := qm.redisClient.Get(ctx, repeatJobKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get repeat job: %w", err)
	}

	// Parse the repeat job
	var repeatJob BullRepeatData
	if err := json.Unmarshal([]byte(repeatJobJSON), &repeatJob); err != nil {
		return fmt.Errorf("failed to unmarshal repeat job: %w", err)
	}

	// Create a new job ID with the current timestamp
	newJobID := fmt.Sprintf("repeat:%s:%d", hash, time.Now().Unix())

	// Create a new job with the same data
	job := JobData{
		ID:        newJobID,
		QueueName: queueName,
		Data:      repeatJob.Data,
		Opts:      repeatJob.Opts,
		Timestamp: time.Now().Unix(),
		Attempts:  defaultAttempts,
		State:     JobStateWaiting,
	}

	// Convert job to JSON
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// Start a Redis transaction
	pipe := qm.redisClient.Pipeline()

	// Add job to the wait list
	waitKey := getBullKey(queueName, waitKey)
	pipe.RPush(ctx, waitKey, newJobID)

	// Store job data
	jobKey := fmt.Sprintf("%s:%s:%s", bullKeyPrefix, queueName, newJobID)
	pipe.Set(ctx, jobKey, string(jobBytes), 0)

	// Execute transaction
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to execute Redis pipeline: %w", err)
	}

	qm.logger.Info("Rescheduled repeat job", "queueName", queueName, "oldJobID", jobID, "newJobID", newJobID)
	return nil
}

// GetQueueStatus returns information about a queue
func (qm *QueueManager) GetQueueStatus(ctx context.Context, queueName string) (map[string]int64, error) {
	waitKey := getBullKey(queueName, waitKey)
	activeKey := getBullKey(queueName, activeKey)
	completedKey := getBullKey(queueName, completedKey)
	failedKey := getBullKey(queueName, failedKey)
	delayedKey := getBullKey(queueName, delayedKey)

	pipe := qm.redisClient.Pipeline()

	waitCmd := pipe.LLen(ctx, waitKey)
	activeCmd := pipe.LLen(ctx, activeKey)
	completedCmd := pipe.LLen(ctx, completedKey)
	failedCmd := pipe.LLen(ctx, failedKey)
	delayedCmd := pipe.LLen(ctx, delayedKey)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue status: %w", err)
	}

	return map[string]int64{
		"waiting":   waitCmd.Val(),
		"active":    activeCmd.Val(),
		"completed": completedCmd.Val(),
		"failed":    failedCmd.Val(),
		"delayed":   delayedCmd.Val(),
		"total":     waitCmd.Val() + activeCmd.Val() + completedCmd.Val() + failedCmd.Val() + delayedCmd.Val(),
	}, nil
}

// ClearQueue clears all jobs from a queue
func (qm *QueueManager) ClearQueue(ctx context.Context, queueName string) error {
	keys := []string{
		getBullKey(queueName, waitKey),
		getBullKey(queueName, activeKey),
		getBullKey(queueName, completedKey),
		getBullKey(queueName, failedKey),
		getBullKey(queueName, delayedKey),
		getBullKey(queueName, stallKey),
	}

	for _, key := range keys {
		if err := qm.redisClient.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("failed to clear queue %s key %s: %w", queueName, key, err)
		}
	}

	qm.logger.Info("Cleared queue", "queueName", queueName)
	return nil
}

// ScheduleJob schedules a job to be processed at a future time
func (qm *QueueManager) ScheduleJob(ctx context.Context, queueName string, data map[string]interface{}, options map[string]interface{}) (string, error) {
	qm.logger.Info("Scheduling job with options", "queueName", queueName, "options", options)
	return qm.AddJob(ctx, queueName, data, options)
}
