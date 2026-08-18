package http

import (
	"github.com/gin-gonic/gin"
	"bitbucket.org/zapspace/zap-go-server/interfaces/http/handlers"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/managers/socket"
)

// Router sets up HTTP routes for the wallet service
type Router struct {
	engine        *gin.Engine
	walletHandler *handlers.WalletHandler
	queueHandler  *handlers.QueueHandler
	logger        logger.Logger
}

// NewRouter creates a new router instance
func NewRouter(walletHandler *handlers.WalletHandler, queueHandler *handlers.QueueHandler, logger logger.Logger) *Router {
	return &Router{
		engine:        gin.Default(),
		walletHandler: walletHandler,
		queueHandler:  queueHandler,
		logger:        logger,
	}
}

// SetupRoutes configures all routes for the wallet service
func (r *Router) SetupRoutes() {
	// Health check endpoint
	r.engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "UP",
		})
	})

	// WebSocket endpoint
	r.engine.GET("/ws", func(c *gin.Context) {
		// Get the socket manager instance
		socketManager := socket.Get()
		// Handle the WebSocket connection
		socketManager.HandleWebSocket(c.Writer, c.Request)
	})

	// API routes
	api := r.engine.Group("/api/v1")

	// Wallet routes
	walletRoutes := api.Group("/wallets")
	walletRoutes.GET("/portfolio/user/:userId", r.walletHandler.GetUserPortfolio)
	walletRoutes.GET("/portfolio/wallet/:walletId", r.walletHandler.GetWalletPortfolio)
	walletRoutes.GET("/portfolio/account/:accountId", r.walletHandler.GetAccountPortfolio)
	walletRoutes.GET("/balance/:accountId", r.walletHandler.GetAccountBalance)

	// Queue routes
	queueRoutes := api.Group("/queues")
	queueRoutes.POST("/job", r.queueHandler.AddJob)
	queueRoutes.POST("/schedule", r.queueHandler.ScheduleJob)
	queueRoutes.POST("/schedule-with-options", r.queueHandler.ScheduleJobWithOptions)
	queueRoutes.GET("/status/:queueName", r.queueHandler.GetQueueStatus)
	queueRoutes.DELETE("/:queueName", r.queueHandler.ClearQueue)

	// Node.js server connection routes
	nodeServerRoutes := api.Group("/node-server")
	nodeServerRoutes.GET("/status", func(c *gin.Context) {
		// Get the NodeSocketConnection instance from the socket manager
		// This is a simplification - in a real implementation, you'd need to
		// ensure this connection is accessible from here
		socketConn := socket.GetNodeConnection()

		if socketConn == nil {
			c.JSON(404, gin.H{
				"status":  "not_connected",
				"message": "No active Node.js server connection",
			})
			return
		}

		// Get connection info
		info := socketConn.GetConnectionInfo()
		c.JSON(200, info)
	})

	// Event-based communication endpoints
	events := api.Group("/events")
	events.POST("/accounts/import", r.walletHandler.HandleAccountsImported)
	events.POST("/balance/update", r.walletHandler.HandleBalanceUpdate)

	r.logger.Info("Router configured successfully")
}

// Run starts the HTTP server on the specified port
func (r *Router) Run(port string) error {
	r.logger.Info("Starting HTTP server", "port", port)
	return r.engine.Run(":" + port)
}
