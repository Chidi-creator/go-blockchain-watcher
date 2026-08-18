package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/src/services"
)

// QueueHandler handles queue-related HTTP requests
type QueueHandler struct {
	queueService *services.QueueService
	logger       logger.Logger
}

// NewQueueHandler creates a new QueueHandler
func NewQueueHandler(queueService *services.QueueService, logger logger.Logger) *QueueHandler {
	return &QueueHandler{
		queueService: queueService,
		logger:       logger,
	}
}

// AddJobRequest represents a request to add a job to a queue
type AddJobRequest struct {
	QueueName string                 `json:"queueName" binding:"required"`
	Data      map[string]interface{} `json:"data" binding:"required"`
	Options   map[string]interface{} `json:"options,omitempty"`
}

// ScheduleJobRequest represents a request to schedule a job
type ScheduleJobRequest struct {
	QueueName    string                 `json:"queueName" binding:"required"`
	Data         map[string]interface{} `json:"data" binding:"required"`
	DelaySeconds int                    `json:"delaySeconds" binding:"required"`
}

// ScheduleJobWithOptionsRequest represents a request to schedule a job with complex options
type ScheduleJobWithOptionsRequest struct {
	QueueName string                 `json:"queueName" binding:"required"`
	Data      map[string]interface{} `json:"data" binding:"required"`
	Options   map[string]interface{} `json:"options" binding:"required"`
}

// AddJob adds a job to a queue
func (h *QueueHandler) AddJob(c *gin.Context) {
	var req AddJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	jobID, err := h.queueService.AddJob(c.Request.Context(), req.QueueName, req.Data, req.Options)
	if err != nil {
		h.logger.Error("Failed to add job", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to add job: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"jobId": jobID})
}

// ScheduleJob schedules a job to be processed at a future time
func (h *QueueHandler) ScheduleJob(c *gin.Context) {
	var req ScheduleJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	jobID, err := h.queueService.ScheduleJobFlexible(c.Request.Context(), req.QueueName, req.Data, req.DelaySeconds)
	if err != nil {
		h.logger.Error("Failed to schedule job", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to schedule job: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"jobId": jobID})
}

// ScheduleJobWithOptions schedules a job with complex options
func (h *QueueHandler) ScheduleJobWithOptions(c *gin.Context) {
	var req ScheduleJobWithOptionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	jobID, err := h.queueService.ScheduleJob(c.Request.Context(), req.QueueName, req.Data, req.Options)
	if err != nil {
		h.logger.Error("Failed to schedule job with options", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to schedule job: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"jobId": jobID})
}

// GetQueueStatus returns the status of a queue
func (h *QueueHandler) GetQueueStatus(c *gin.Context) {
	queueName := c.Param("queueName")
	if queueName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Queue name is required"})
		return
	}

	status, err := h.queueService.GetQueueStatus(c.Request.Context(), queueName)
	if err != nil {
		h.logger.Error("Failed to get queue status", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get queue status: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, status)
}

// ClearQueue clears all jobs from a queue
func (h *QueueHandler) ClearQueue(c *gin.Context) {
	queueName := c.Param("queueName")
	if queueName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Queue name is required"})
		return
	}

	if err := h.queueService.ClearQueue(c.Request.Context(), queueName); err != nil {
		h.logger.Error("Failed to clear queue", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to clear queue: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}
