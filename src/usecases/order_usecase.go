package usecases

import (
	"context"
	"errors"
	"time"

	httpmanager "bitbucket.org/zapspace/zap-go-server/managers/http"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	domainerrors "bitbucket.org/zapspace/zap-go-server/src/errors"
	"bitbucket.org/zapspace/zap-go-server/src/repositories"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// OrderUseCase handles order related business logic
type OrderUseCase struct {
	orderRepo             repositories.OrderRepository
	walletRepo            repositories.WalletRepository
	accountRepo           repositories.AccountRepository
	currencyRepo          repositories.CurrencyRepository
	supportedCurrencyRepo repositories.SupportedCurrencyRepository
	chainRepo             repositories.ChainRepository
	blockchainService     *services.BlockchainService
	logger                logger.Logger
	requestManager        *httpmanager.RequestManager
}

// NewOrderUseCase creates a new order use case
func NewOrderUseCase(
	orderRepo repositories.OrderRepository,
	walletRepo repositories.WalletRepository,
	accountRepo repositories.AccountRepository,
	currencyRepo repositories.CurrencyRepository,
	supportedCurrencyRepo repositories.SupportedCurrencyRepository,
	chainRepo repositories.ChainRepository,
	blockchainService *services.BlockchainService,
	logger logger.Logger,
	requestManager *httpmanager.RequestManager,
) *OrderUseCase {
	return &OrderUseCase{
		orderRepo:             orderRepo,
		walletRepo:            walletRepo,
		accountRepo:           accountRepo,
		currencyRepo:          currencyRepo,
		supportedCurrencyRepo: supportedCurrencyRepo,
		chainRepo:             chainRepo,
		blockchainService:     blockchainService,
		logger:                logger,
		requestManager:        requestManager,
	}
}

// CreateOrder creates a new order
func (uc *OrderUseCase) CreateOrder(
	ctx context.Context,
	userID, currencyID primitive.ObjectID,
	flow string,
	amount, rate float64,
	depositAccountIDs, withdrawalAccountIDs []primitive.ObjectID,
	buyChain, sellChain primitive.ObjectID,
	provider string,
) (*types.Order, error) {
	// Validate currency exists
	currency, err := uc.currencyRepo.GetByID(ctx, currencyID)
	if err != nil {
		uc.logger.Error("Failed to get currency for order creation", "currencyId", currencyID.Hex(), "error", err)
		return nil, err
	}

	if currency == nil {
		return nil, domainerrors.ErrCurrencyNotFound
	}

	// Create new order
	order := types.NewOrder(userID, currencyID, flow, amount, rate)

	// Set additional fields
	if len(depositAccountIDs) > 0 {
		order.DepositAccountIDs = depositAccountIDs
	}

	if len(withdrawalAccountIDs) > 0 {
		order.WithdrawalAccountIDs = withdrawalAccountIDs
	}

	if !buyChain.IsZero() {
		order.BuyChain = buyChain
	}

	if !sellChain.IsZero() {
		order.SellChain = sellChain
	}

	if provider != "" {
		order.Provider = provider
	}

	// Default expiration time (24 hours)
	order.ExpiresAt = time.Now().Add(24 * time.Hour)

	// Save order
	err = uc.orderRepo.Create(ctx, order)
	if err != nil {
		uc.logger.Error("Failed to create order", "error", err)
		return nil, err
	}

	uc.logger.Info("Order created successfully",
		"orderId", order.ID.Hex(),
		"userId", userID.Hex(),
		"flow", flow,
		"amount", amount)

	return order, nil
}

// GetOrderByID retrieves an order by its ID
func (uc *OrderUseCase) GetOrderByID(ctx context.Context, orderID primitive.ObjectID) (*types.Order, error) {
	order, err := uc.orderRepo.GetByID(ctx, orderID)
	if err != nil {
		uc.logger.Error("Failed to get order by ID", "orderId", orderID.Hex(), "error", err)
		return nil, err
	}

	if order == nil {
		return nil, domainerrors.ErrOrderNotFound
	}

	return order, nil
}

// GetOrdersByUserID retrieves all orders for a user
func (uc *OrderUseCase) GetOrdersByUserID(ctx context.Context, userID primitive.ObjectID) ([]*types.Order, error) {
	orders, err := uc.orderRepo.GetByUserID(ctx, userID)
	if err != nil {
		uc.logger.Error("Failed to get orders by user ID", "userId", userID.Hex(), "error", err)
		return nil, err
	}

	return orders, nil
}

// GetOrderByDepositAddress retrieves an order by its deposit wallet address
func (uc *OrderUseCase) GetOrderByDepositAddress(ctx context.Context, depositAddress string) (*types.Order, error) {
	order, err := uc.orderRepo.GetByDepositAddress(ctx, depositAddress)
	if err != nil {
		uc.logger.Error("Failed to get order by deposit address", "address", depositAddress, "error", err)
		return nil, err
	}

	if order == nil {
		return nil, domainerrors.ErrOrderNotFound
	}

	return order, nil
}

// UpdateOrder updates an existing order
func (uc *OrderUseCase) UpdateOrder(ctx context.Context, order *types.Order) error {
	if order.ID.IsZero() {
		return errors.New("order ID is required")
	}

	// Check if order exists and get the current data
	existingOrder, err := uc.orderRepo.GetByID(ctx, order.ID)
	if err != nil {
		uc.logger.Error("Failed to get order for update", "orderId", order.ID.Hex(), "error", err)
		return err
	}

	if existingOrder == nil {
		return domainerrors.ErrOrderNotFound
	}

	// Only update fields that are explicitly set (non-zero values)
	// This prevents overwriting existing data with zero values
	if order.Status != "" {
		existingOrder.Status = order.Status
	}
	if order.Amount != 0 {
		existingOrder.Amount = order.Amount
	}
	if order.Rate != 0 {
		existingOrder.Rate = order.Rate
	}
	if !order.UserID.IsZero() {
		existingOrder.UserID = order.UserID
	}
	if !order.CurrencyID.IsZero() {
		existingOrder.CurrencyID = order.CurrencyID
	}
	if order.Flow != "" {
		existingOrder.Flow = order.Flow
	}
	if order.Platform != "" {
		existingOrder.Platform = order.Platform
	}
	if order.Provider != "" {
		existingOrder.Provider = order.Provider
	}
	if order.ProviderOrderID != "" {
		existingOrder.ProviderOrderID = order.ProviderOrderID
	}
	if !order.BuyChain.IsZero() {
		existingOrder.BuyChain = order.BuyChain
	}
	if !order.SellChain.IsZero() {
		existingOrder.SellChain = order.SellChain
	}
	if order.OpenAmount != 0 {
		existingOrder.OpenAmount = order.OpenAmount
	}
	if order.RefundAmount != 0 {
		existingOrder.RefundAmount = order.RefundAmount
	}
	if order.CalculatedAmount != 0 {
		existingOrder.CalculatedAmount = order.CalculatedAmount
	}
	if order.OpenRate != 0 {
		existingOrder.OpenRate = order.OpenRate
	}
	if order.CalculatedRate != 0 {
		existingOrder.CalculatedRate = order.CalculatedRate
	}
	if order.ProviderFeesPaid != 0 {
		existingOrder.ProviderFeesPaid = order.ProviderFeesPaid
	}
	if order.BlockchainFeesPaid != 0 {
		existingOrder.BlockchainFeesPaid = order.BlockchainFeesPaid
	}
	if order.FeesGained != 0 {
		existingOrder.FeesGained = order.FeesGained
	}
	if order.ProviderFeesToPay != 0 {
		existingOrder.ProviderFeesToPay = order.ProviderFeesToPay
	}
	if order.BlockchainFeesToPay != 0 {
		existingOrder.BlockchainFeesToPay = order.BlockchainFeesToPay
	}
	if order.FeesToGain != 0 {
		existingOrder.FeesToGain = order.FeesToGain
	}
	if order.CalculatedProviderFeesPaid != 0 {
		existingOrder.CalculatedProviderFeesPaid = order.CalculatedProviderFeesPaid
	}
	if order.CalculatedBlockchainFeesPaid != 0 {
		existingOrder.CalculatedBlockchainFeesPaid = order.CalculatedBlockchainFeesPaid
	}
	if order.CalculatedFeesGained != 0 {
		existingOrder.CalculatedFeesGained = order.CalculatedFeesGained
	}
	if order.Margin != 0 {
		existingOrder.Margin = order.Margin
	}
	if len(order.DepositAccountIDs) > 0 {
		existingOrder.DepositAccountIDs = order.DepositAccountIDs
	}
	if len(order.WithdrawalAccountIDs) > 0 {
		existingOrder.WithdrawalAccountIDs = order.WithdrawalAccountIDs
	}
	if !order.RefundAccountID.IsZero() {
		existingOrder.RefundAccountID = order.RefundAccountID
	}
	if len(order.TransactionIDs) > 0 {
		existingOrder.TransactionIDs = order.TransactionIDs
	}
	if len(order.TransactionTypes) > 0 {
		existingOrder.TransactionTypes = order.TransactionTypes
	}
	if !order.ParentOrderID.IsZero() {
		existingOrder.ParentOrderID = order.ParentOrderID
	}
	if !order.ChildOrderID.IsZero() {
		existingOrder.ChildOrderID = order.ChildOrderID
	}
	if order.BuyProvider != "" {
		existingOrder.BuyProvider = order.BuyProvider
	}
	if order.SellProvider != "" {
		existingOrder.SellProvider = order.SellProvider
	}
	if order.RateID != "" {
		existingOrder.RateID = order.RateID
	}
	if !order.ExpiresAt.IsZero() {
		existingOrder.ExpiresAt = order.ExpiresAt
	}
	if order.StatusTimestamps != nil {
		existingOrder.StatusTimestamps = order.StatusTimestamps
	}

	// Update order with the merged data
	err = uc.orderRepo.Update(ctx, existingOrder)
	if err != nil {
		uc.logger.Error("Failed to update order", "orderId", order.ID.Hex(), "error", err)
		return err
	}

	uc.logger.Info("Order updated successfully", "orderId", order.ID.Hex())
	return nil
}

// UpdateOrderFields updates specific fields of an existing order
// This method is safer for partial updates as it only updates the specified fields
func (uc *OrderUseCase) UpdateOrderFields(ctx context.Context, orderID primitive.ObjectID, updates map[string]interface{}) error {
	if orderID.IsZero() {
		return errors.New("order ID is required")
	}

	// Check if order exists
	existingOrder, err := uc.orderRepo.GetByID(ctx, orderID)
	if err != nil {
		uc.logger.Error("Failed to get order for field update", "orderId", orderID.Hex(), "error", err)
		return err
	}

	if existingOrder == nil {
		return domainerrors.ErrOrderNotFound
	}

	// Apply updates to the existing order
	for field, value := range updates {
		switch field {
		case "status":
			if v, ok := value.(string); ok {
				existingOrder.Status = v
			}
		case "amount":
			if v, ok := value.(float64); ok {
				existingOrder.Amount = v
			}
		case "rate":
			if v, ok := value.(float64); ok {
				existingOrder.Rate = v
			}
		case "providerOrderId":
			if v, ok := value.(string); ok {
				existingOrder.ProviderOrderID = v
			}
		case "provider":
			if v, ok := value.(string); ok {
				existingOrder.Provider = v
			}
		// Add more fields as needed
		default:
			uc.logger.Warn("Unknown field in update", "field", field, "orderId", orderID.Hex())
		}
	}

	// Update order
	err = uc.orderRepo.Update(ctx, existingOrder)
	if err != nil {
		uc.logger.Error("Failed to update order fields", "orderId", orderID.Hex(), "error", err)
		return err
	}

	uc.logger.Info("Order fields updated successfully", "orderId", orderID.Hex(), "fields", updates)
	return nil
}

// DeleteOrder soft deletes an order
func (uc *OrderUseCase) DeleteOrder(ctx context.Context, orderID primitive.ObjectID) error {
	order, err := uc.orderRepo.GetByID(ctx, orderID)
	if err != nil {
		uc.logger.Error("Failed to get order for deletion", "orderId", orderID.Hex(), "error", err)
		return err
	}

	if order == nil {
		return domainerrors.ErrOrderNotFound
	}

	order.MarkAsDeleted()

	err = uc.orderRepo.Update(ctx, order)
	if err != nil {
		uc.logger.Error("Failed to mark order as deleted", "orderId", orderID.Hex(), "error", err)
		return err
	}

	uc.logger.Info("Order marked as deleted", "orderId", orderID.Hex())
	return nil
}

// UpdateOrderStatus updates the status of an order and records the timestamp
func (uc *OrderUseCase) UpdateOrderStatus(ctx context.Context, orderID primitive.ObjectID, status string) (*types.Order, error) {
	order, err := uc.orderRepo.GetByID(ctx, orderID)
	if err != nil {
		uc.logger.Error("Failed to get order for status update", "orderId", orderID.Hex(), "error", err)
		return nil, err
	}

	if order == nil {
		return nil, domainerrors.ErrOrderNotFound
	}

	order.UpdateStatus(status)

	err = uc.orderRepo.Update(ctx, order)
	if err != nil {
		uc.logger.Error("Failed to update order status", "orderId", orderID.Hex(), "status", status, "error", err)
		return nil, err
	}

	uc.logger.Info("Order status updated successfully", "orderId", orderID.Hex(), "status", status)
	return order, nil
}
