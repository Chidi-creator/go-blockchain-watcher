package changenow

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/internal/worker"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/http"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/providers/cex/changenow"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/usecases"
)

// Worker represents the ChangeNow worker service
type Worker struct {
	cfg          *config.Config
	concurrency  int
	logger       logger.Logger
	redisClient  *redis.Client
	queueSvc     *services.QueueService
	mongoClient  *mongo.Client
	cacheManager *cache.CacheManager
	stopChan     chan struct{}
	orderUsecase *usecases.OrderUseCase
}

// New creates a new ChangeNow worker
func New(cfg *config.Config, concurrency int, logger logger.Logger, redisClient *redis.Client, queueSvc *services.QueueService, mongoClient *mongo.Client, cacheManager *cache.CacheManager, orderUsecase *usecases.OrderUseCase) worker.Worker {
	return &Worker{
		cfg:          cfg,
		concurrency:  concurrency,
		logger:       logger,
		redisClient:  redisClient,
		queueSvc:     queueSvc,
		mongoClient:  mongoClient,
		cacheManager: cacheManager,
		stopChan:     make(chan struct{}),
		orderUsecase: orderUsecase,
	}
}

// Start initiates the ChangeNow worker processing
func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info("Initializing ChangeNow Worker...")

	// Validate that orderUsecase is not nil
	if w.orderUsecase == nil {
		w.logger.Warn("orderUsecase is nil - order status updates will use direct database operations")
	}

	// Parse config values
	apiKey := w.cfg.ChangeNow.APIKey
	apiURL := w.cfg.ChangeNow.APIURL
	mongoURI := w.cfg.MongoDB.URI

	w.logger.Info("Connected to MongoDB", "uri", mongoURI, "database", w.cfg.MongoDB.Database)

	// Initialize queue manager
	queueManager := w.queueSvc.GetQueueManager()

	requestManager := http.Initialize(w.logger)

	// Initialize ChangeNow provider
	changeNowConfig := config.ChangeNowConfig{
		APIKey: apiKey,
		APIURL: apiURL,
	}

	provider, err := changenow.NewProvider(w.logger, changeNowConfig, queueManager, w.cacheManager, nil)
	if err != nil {
		w.logger.Error("Failed to initialize ChangeNow provider", "error", err)
		return err
	}
	w.logger.Info("ChangeNow provider initialized")

	// Get database
	db := w.mongoClient.Database(w.cfg.MongoDB.Database)

	// Start the periodic check in a separate goroutine
	go w.runPeriodicCheck(ctx, provider, db, requestManager, w.orderUsecase)

	// Also keep the queue processing functionality for backward compatibility
	queueName := constants.QueueNames["QueueChangeNowWatcher"]
	w.logger.Info("Using queue for ChangeNow watcher", "queueName", queueName)

	// Register ChangeNow watcher handler
	queueManager.RegisterHandler(queueName, func(ctx context.Context, data map[string]interface{}) error {
		w.logger.Debug("Handler invoked", "data", data)
		return processChangeNowTransaction(ctx, data, provider, db, w.logger, requestManager)
	})

	// Start processing jobs
	w.logger.Info("Starting to process ChangeNow watcher jobs", "concurrency", w.concurrency)
	err = queueManager.ProcessJobs(ctx, queueName, w.concurrency)
	if err != nil {
		w.logger.Error("Failed to start processing jobs", "error", err)
		return err
	}

	// Wait for context to be done
	<-ctx.Done()
	close(w.stopChan) // Signal the periodic check to stop
	w.logger.Info("ChangeNow worker shutting down")
	return nil
}

// runPeriodicCheck runs a check every 2 minutes for BTC BUY orders
func (w *Worker) runPeriodicCheck(ctx context.Context, provider *changenow.Provider, db *mongo.Database, requestManager *http.RequestManager, orderUsecase *usecases.OrderUseCase) {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	w.logger.Info("Starting periodic check for ChangeNow BTC BUY orders every 2 minutes")

	// Run an initial check immediately
	w.checkBTCBuyOrders(ctx, provider, db, requestManager, orderUsecase)

	for {
		select {
		case <-ticker.C:
			w.checkBTCBuyOrders(ctx, provider, db, requestManager, orderUsecase)
		case <-w.stopChan:
			w.logger.Info("Stopping periodic ChangeNow order checks")
			return
		case <-ctx.Done():
			w.logger.Info("Context cancelled, stopping periodic ChangeNow order checks")
			return
		}
	}
}

// Helper function to convert struct to map
func structToMap(obj interface{}) (map[string]interface{}, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// checkBTCBuyOrders checks all BTC BUY orders that need status updates
func (w *Worker) checkBTCBuyOrders(ctx context.Context, provider *changenow.Provider, db *mongo.Database, requestManager *http.RequestManager, orderUsecase *usecases.OrderUseCase) {
	w.logger.Info("Running check for ChangeNow BTC BUY orders")

	// First, get the BTC currency document to find its ObjectID
	currenciesCollection := db.Collection("currencies")
	var btcCurrency struct {
		ID primitive.ObjectID `bson:"_id"`
	}

	err := currenciesCollection.FindOne(ctx, bson.M{"code": "BTC"}).Decode(&btcCurrency)
	if err != nil {
		w.logger.Error("Failed to find BTC currency", "error", err)
		return
	}

	// Query all BUY orders for BTC that are in status that needs monitoring
	// Typically these would be PENDING, PROCESSING, etc.
	ordersCollection := db.Collection("orders")

	cursor, err := ordersCollection.Find(ctx, bson.M{
		"flow": "BUY",
		// "currencyId": btcCurrency.ID,
		"provider":        "ChangeNow",
		"providerOrderId": bson.M{"$exists": true, "$ne": ""},
		"status": bson.M{
			"$in": []string{
				"PENDING",
				"PROCESSING",
				constants.ORDER_STATUS_PENDING,
				constants.ORDER_STATUS_DEPOSIT_CONFIRMING,
			},
		},
	})

	if err != nil {
		w.logger.Error("Failed to query BTC BUY orders", "error", err)
		return
	}
	defer cursor.Close(ctx)

	// Process each order
	var orders []bson.M
	if err = cursor.All(ctx, &orders); err != nil {
		w.logger.Error("Failed to decode orders", "error", err)
		return
	}

	w.logger.Info("Found ChangeNow BUY orders to check", "count", len(orders))

	// Process each order
	for _, order := range orders {
		providerOrderId, ok := order["providerOrderId"].(string)
		if !ok || providerOrderId == "" {
			w.logger.Warn("Order missing providerOrderId", "orderId", order["_id"])
			continue
		}

		// Get transaction status from ChangeNow using providerOrderId
		txStatusResp, err := provider.GetTransactionStatus(ctx, providerOrderId)
		if err != nil {
			w.logger.Error("Failed to get transaction status", "error", err, "providerOrderId", providerOrderId)
			continue
		}

		// Convert the struct response to a map
		txStatus, err := structToMap(txStatusResp)
		if err != nil {
			w.logger.Error("Failed to convert transaction status to map", "error", err)
			continue
		}

		w.logger.Info("Retrieved ChangeNow transaction status",
			"orderId", order["_id"],
			"providerOrderId", providerOrderId,
			"status", txStatus["status"],
			"payinHash", txStatus["payinHash"],
			"payoutHash", txStatus["payoutHash"])

		// Process the order status based on the txStatus
		processOrderStatus(ctx, order, txStatus, db, w.logger, requestManager, orderUsecase)
	}
}

// processOrderStatus updates an order based on the ChangeNow transaction status
func processOrderStatus(
	ctx context.Context,
	order bson.M,
	txStatus map[string]interface{},
	db *mongo.Database,
	logger logger.Logger,
	requestManager *http.RequestManager,
	orderUsecase *usecases.OrderUseCase,
) {
	orderId, _ := order["_id"].(primitive.ObjectID)
	orderIdHex := orderId.Hex()
	ordersCollection := db.Collection("orders")

	// Update order status based on transaction status
	status, _ := txStatus["status"].(string)

	switch status {
	case "finished", "completed":
		// Transaction is complete

		// Create object to send to url
		orderObj := map[string]interface{}{
			"order":        order,
			"goWorkerType": "changeNow",
			"txData":       txStatus,
		}

		// Extract the amount from txStatus using the helper function
		amount := extractTransactionAmount(txStatus, logger)

		logger.Info("Extracted transaction amount", "amountTo", amount, "orderId", orderIdHex)

		// Check if orderUsecase is nil to prevent panic
		// if orderUsecase != nil {
		// 	//update order status to deposit confirming
		// 	updates := map[string]interface{}{
		// 		"status": constants.ORDER_STATUS_DEPOSIT_CONFIRMING,
		// 		"amount": amount,
		// 	}

		// 	err := orderUsecase.UpdateOrderFields(ctx, orderId, updates)
		// 	if err != nil {
		// 		logger.Error("Failed to update order status to deposit confirming", "error", err, "orderId", orderIdHex)
		// 		return
		// 	}
		// } else {
		// 	logger.Error("orderUsecase is nil, cannot update order status", "orderId", orderIdHex)
		// 	// Fallback to direct database update
		// 	update := bson.M{
		// 		"$set": bson.M{
		// 			"status":    constants.ORDER_STATUS_DEPOSIT_CONFIRMED,
		// 			"amount":    amount,
		// 			"updatedAt": time.Now(),
		// 		},
		// 	}

		// 	_, err := ordersCollection.UpdateByID(ctx, orderId, update)
		// 	if err != nil {
		// 		logger.Error("Failed to update order status via direct database update", "error", err, "orderId", orderIdHex)
		// 		return
		// 	}
		// 	logger.Info("Updated order status to deposit confirming via direct database update", "orderId", orderIdHex)
		// }

		request, err := requestManager.Post(ctx, "NODE_SERVER", "/orders/webhooks/watchAddress", orderObj, nil)
		if err != nil {
			logger.Error("Failed to update order status", "error", err, "orderId", orderIdHex)
		} else {
			logger.Info("Updated order status to completed", "orderId", orderIdHex)
		}

		fmt.Println("request", string(request))

	case "failed", "refunded", "expired":
		// Transaction failed
		update := bson.M{
			"$set": bson.M{
				"status":    constants.ORDER_STATUS_FAILED,
				"updatedAt": time.Now(),
				"error":     fmt.Sprintf("ChangeNow transaction %s", status),
			},
		}

		_, err := ordersCollection.UpdateByID(ctx, orderId, update)
		if err != nil {
			logger.Error("Failed to update order status", "error", err, "orderId", orderIdHex)
		} else {
			logger.Info("Updated order status to failed",
				"orderId", orderIdHex,
				"status", status)
		}

	case "waiting", "confirming", "exchanging", "sending":
		// Transaction is still in progress
		logger.Info("Transaction still in progress",
			"orderId", orderIdHex,
			"status", status)

		// If status is "waiting" for too long (more than 2 hours), mark as failed
		if status == "waiting" {
			// Try to get created time
			createdAtStr, ok := txStatus["createdAt"].(string)
			if ok && createdAtStr != "" {
				createdAt, err := time.Parse(time.RFC3339, createdAtStr)
				if err == nil && !createdAt.IsZero() {
					if time.Since(createdAt) > 2*time.Hour {
						update := bson.M{
							"$set": bson.M{
								"status":    constants.ORDER_STATUS_FAILED,
								"updatedAt": time.Now(),
								"error":     "ChangeNow transaction timed out in waiting status",
							},
						}

						_, err := ordersCollection.UpdateByID(ctx, orderId, update)
						if err != nil {
							logger.Error("Failed to update timed-out order", "error", err, "orderId", orderIdHex)
						} else {
							logger.Info("Updated timed-out order to failed", "orderId", orderIdHex)
						}
					}
				}
			}
		}
	default:
		logger.Warn("Unknown transaction status",
			"orderId", orderIdHex,
			"status", status)
	}
}

// extractTransactionAmount extracts the amount field from a transaction status response
// It handles different possible data types and returns the parsed float64 value
func extractTransactionAmount(txStatus map[string]interface{}, logger logger.Logger) float64 {
	// First, log ALL fields in the response for debugging
	logger.Info("=== DEBUGGING ChangeNow API Response ===")
	logger.Info("Total fields in response", "count", len(txStatus))
	for key, value := range txStatus {
		logger.Info("Response field", "key", key, "value", value, "type", fmt.Sprintf("%T", value))
	}
	logger.Info("=== END DEBUGGING ===")

	// We specifically want amountFrom - this is the actual amount ChangeNow received
	// This determines what we should pay the user
	// Fallback to expectedAmountFrom if amountFrom is not available (for pending transactions)
	amountFields := []string{"amountFrom", "expectedAmountFrom"}

	for _, field := range amountFields {
		if amountValue, exists := txStatus[field]; exists && amountValue != nil {
			logger.Info("Found amount field", "field", field, "value", amountValue, "type", fmt.Sprintf("%T", amountValue))

			// Try to extract the value based on its type
			switch v := amountValue.(type) {
			case float64:
				if v > 0 {
					logger.Info("Successfully extracted received amount as float64", "field", field, "amount", v)
					return v
				} else {
					logger.Warn("Amount field is zero", "field", field, "amount", v)
				}
			case int:
				floatVal := float64(v)
				if v > 0 {
					logger.Info("Successfully extracted received amount as int", "field", field, "amount", v)
					return floatVal
				} else {
					logger.Warn("Amount field is zero", "field", field, "amount", v)
				}
			case int64:
				floatVal := float64(v)
				if v > 0 {
					logger.Info("Successfully extracted received amount as int64", "field", field, "amount", v)
					return floatVal
				} else {
					logger.Warn("Amount field is zero", "field", field, "amount", v)
				}
			case string:
				if v != "" {
					parsedAmount, err := strconv.ParseFloat(v, 64)
					if err != nil {
						logger.Error("Failed to parse amount string as float", "error", err, "value", v, "field", field)
						continue
					}
					if parsedAmount > 0 {
						logger.Info("Successfully extracted received amount as string", "field", field, "amount", parsedAmount)
						return parsedAmount
					} else {
						logger.Warn("Parsed amount is zero", "field", field, "amount", parsedAmount)
					}
				}
			case json.Number:
				parsedAmount, err := v.Float64()
				if err != nil {
					logger.Error("Failed to parse amount json.Number as float", "error", err, "value", v, "field", field)
					continue
				}
				if parsedAmount > 0 {
					logger.Info("Successfully extracted received amount as json.Number", "field", field, "amount", parsedAmount)
					return parsedAmount
				} else {
					logger.Warn("Parsed amount is zero", "field", field, "amount", parsedAmount)
				}
			default:
				logger.Warn("Amount field in unexpected format", "type", fmt.Sprintf("%T", amountValue), "field", field, "value", amountValue)
				continue
			}
		} else {
			logger.Debug("Amount field not found or nil", "field", field)
		}
	}

	// Log all available fields for debugging
	logger.Error("No valid received amount found in transaction data",
		"availableFields", getMapKeys(txStatus),
		"note", "This function looks for 'amountFrom' (actual received) or 'expectedAmountFrom' (expected)")
	return 0
}

// Helper function to get map keys for debugging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// processChangeNowTransaction handles transaction jobs from the queue
func processChangeNowTransaction(
	ctx context.Context,
	data map[string]interface{},
	provider *changenow.Provider,
	db *mongo.Database,
	logger logger.Logger,
	requestManager *http.RequestManager,
) error {
	logger.Info("Processing ChangeNow transaction", "data", data)

	// Extract order data first
	orderData, ok := data["order"].(map[string]interface{})
	if !ok || orderData == nil {
		logger.Error("Missing or invalid order data in job data")
		return fmt.Errorf("missing or invalid order data in job data")
	}

	orderID, ok := orderData["id"].(string)
	if !ok {
		logger.Error("Invalid order ID format", "order", orderData)
		return fmt.Errorf("invalid order ID format")
	}

	// Get transaction status from ChangeNow
	txStatusResp, err := provider.GetTransactionStatus(ctx, orderID)
	if err != nil {
		logger.Error("Failed to get transaction status", "error", err, "providerId", orderID)
		// Don't return error so job will retry
		return nil
	}

	// Convert the struct response to a map
	txStatus, err := structToMap(txStatusResp)
	if err != nil {
		logger.Error("Failed to convert transaction status to map", "error", err)
		return nil
	}

	status, _ := txStatus["status"].(string)
	payinHash, _ := txStatus["payinHash"].(string)
	payoutHash, _ := txStatus["payoutHash"].(string)

	logger.Info("Retrieved ChangeNow transaction status",
		"orderId", orderID,
		"status", status,
		"amountFrom", txStatus["amountFrom"],
		"amountTo", txStatus["amountTo"],
		"payinHash", payinHash,
		"payoutHash", payoutHash)

	orderObjID, err := primitive.ObjectIDFromHex(orderID)
	if err != nil {
		logger.Error("Failed to convert order ID to ObjectID", "error", err, "orderId", orderID)
		return fmt.Errorf("failed to convert order ID to ObjectID: %w", err)
	}

	// Get the order from the database
	ordersCollection := db.Collection("orders")
	var order map[string]interface{}
	err = ordersCollection.FindOne(ctx, bson.M{"_id": orderObjID}).Decode(&order)
	if err != nil {
		logger.Error("Failed to get order", "error", err, "orderId", orderID)
		if err == mongo.ErrNoDocuments {
			return fmt.Errorf("order not found: %s", orderID)
		}
		return fmt.Errorf("failed to get order: %w", err)
	}

	// Update order status based on transaction status
	switch status {
	case "finished", "completed":
		// Transaction is complete

		//create object to send to url
		orderObj := map[string]interface{}{
			"order":        order,
			"goWorkerType": "changeNow",
			"txData":       txStatus,
		}

		request, err := requestManager.Post(ctx, "NODE_SERVER", "/orders/webhooks/watchAddress", orderObj, nil)
		if err != nil {
			logger.Error("Failed to update order status", "error", err, "orderId", orderID)
		} else {
			logger.Info("Updated order status to completed", "orderId", orderID)
		}

		fmt.Println("request", string(request))

	case "failed", "refunded", "expired":
		// Transaction failed
		update := bson.M{
			"$set": bson.M{
				"status":    constants.ORDER_STATUS_FAILED,
				"updatedAt": time.Now(),
				"error":     fmt.Sprintf("ChangeNow transaction %s", status),
			},
		}

		_, err = ordersCollection.UpdateByID(ctx, orderObjID, update)
		if err != nil {
			logger.Error("Failed to update order status", "error", err, "orderId", orderID)
			// Continue - we'll try again next time
		} else {
			logger.Info("Updated order status to failed",
				"orderId", orderID,
				"status", status)
		}

		// Job is complete, no need to run again
		return fmt.Errorf("job completed")
	case "waiting", "confirming", "exchanging", "sending":
		// Transaction is still in progress
		logger.Info("Transaction still in progress",
			"orderId", orderID,
			"status", status)

		// If status is "waiting" for too long (more than 2 hours), mark as failed
		if status == "waiting" {
			// Try to get created time
			createdAtStr, ok := txStatus["createdAt"].(string)
			if ok && createdAtStr != "" {
				createdAt, err := time.Parse(time.RFC3339, createdAtStr)
				if err == nil && !createdAt.IsZero() {
					if time.Since(createdAt) > 2*time.Hour {
						update := bson.M{
							"$set": bson.M{
								"status":    constants.ORDER_STATUS_FAILED,
								"updatedAt": time.Now(),
								"error":     "ChangeNow transaction timed out in waiting status",
							},
						}

						_, err := ordersCollection.UpdateByID(ctx, orderObjID, update)
						if err != nil {
							logger.Error("Failed to update timed-out order", "error", err, "orderId", orderID)
						} else {
							logger.Info("Updated timed-out order to failed", "orderId", orderID)
						}

						return fmt.Errorf("job completed due to timeout")
					}
				}
			}
		}
	default:
		logger.Warn("Unknown transaction status",
			"orderId", orderID,
			"status", status)
	}

	// Keep monitoring if transaction is not complete
	if status != "finished" && status != "completed" &&
		status != "failed" && status != "refunded" && status != "expired" {
		return nil // Continue monitoring
	}

	// We're done monitoring this transaction
	return fmt.Errorf("job completed")
}
