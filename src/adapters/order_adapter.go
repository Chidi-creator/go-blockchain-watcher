package adapters

import (
	"context"

	"bitbucket.org/zapspace/zap-go-server/config/mongodb"
	"bitbucket.org/zapspace/zap-go-server/models"
	domainrepo "bitbucket.org/zapspace/zap-go-server/src/repositories"
	"bitbucket.org/zapspace/zap-go-server/src/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// OrderRepositoryAdapter adapts mongodb.OrderRepository to domain interface
type OrderRepositoryAdapter struct {
	repo mongodb.OrderRepository
}

// NewOrderRepositoryAdapter creates a new order repository adapter
func NewOrderRepositoryAdapter(repo mongodb.OrderRepository) domainrepo.OrderRepository {
	return &OrderRepositoryAdapter{repo: repo}
}

// GetByID retrieves an order by its ID
func (a *OrderRepositoryAdapter) GetByID(ctx context.Context, id primitive.ObjectID) (*types.Order, error) {
	order, err := a.repo.GetOrderByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return convertOrderModelToDomain(order), nil
}

// GetByUserID retrieves all orders for a user
func (a *OrderRepositoryAdapter) GetByUserID(ctx context.Context, userID primitive.ObjectID) ([]*types.Order, error) {
	orders, err := a.repo.GetOrdersByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}

	domainOrders := make([]*types.Order, len(orders))
	for i, order := range orders {
		domainOrders[i] = convertOrderModelToDomain(&order)
	}
	return domainOrders, nil
}

// GetByDepositAddress retrieves an order by deposit address
func (a *OrderRepositoryAdapter) GetByDepositAddress(ctx context.Context, depositAddress string) (*types.Order, error) {
	order, err := a.repo.GetOrderByDepositAddress(ctx, depositAddress)
	if err != nil {
		return nil, err
	}
	return convertOrderModelToDomain(order), nil
}

// Create creates a new order
func (a *OrderRepositoryAdapter) Create(ctx context.Context, order *types.Order) error {
	model := convertOrderDomainToModel(order)
	result, err := a.repo.CreateOrder(ctx, model)
	if err != nil {
		return err
	}

	// Update ID if it was generated
	order.ID = result.ID
	return nil
}

// Update updates an existing order
func (a *OrderRepositoryAdapter) Update(ctx context.Context, order *types.Order) error {
	model := convertOrderDomainToModel(order)
	return a.repo.UpdateOrder(ctx, model)
}

// Delete deletes an order
func (a *OrderRepositoryAdapter) Delete(ctx context.Context, id primitive.ObjectID) error {
	return a.repo.DeleteOrder(ctx, id)
}

// Helper functions for order conversions
func convertOrderModelToDomain(order *models.Order) *types.Order {
	if order == nil {
		return nil
	}

	return &types.Order{
		ID:                           order.ID,
		Platform:                     order.Platform,
		V1UserID:                     order.V1UserID,
		UserID:                       order.UserID,
		Type:                         order.Type,
		Flow:                         order.Flow,
		CurrencyID:                   order.CurrencyID,
		BuyChain:                     order.BuyChain,
		SellChain:                    order.SellChain,
		OpenAmount:                   order.OpenAmount,
		Amount:                       order.Amount,
		RefundAmount:                 order.RefundAmount,
		CalculatedAmount:             order.CalculatedAmount,
		OpenRate:                     order.OpenRate,
		Rate:                         order.Rate,
		CalculatedRate:               order.CalculatedRate,
		ProviderFeesPaid:             order.ProviderFeesPaid,
		BlockchainFeesPaid:           order.BlockchainFeesPaid,
		FeesGained:                   order.FeesGained,
		ProviderFeesToPay:            order.ProviderFeesToPay,
		BlockchainFeesToPay:          order.BlockchainFeesToPay,
		FeesToGain:                   order.FeesToGain,
		CalculatedProviderFeesPaid:   order.CalculatedProviderFeesPaid,
		CalculatedBlockchainFeesPaid: order.CalculatedBlockchainFeesPaid,
		CalculatedFeesGained:         order.CalculatedFeesGained,
		Status:                       order.Status,
		StatusTimestamps:             order.StatusTimestamps,
		V1ID:                         order.V1ID,
		DepositAccountIDs:            order.DepositAccountIDs,
		WithdrawalAccountIDs:         order.WithdrawalAccountIDs,
		RefundAccountID:              order.RefundAccountID,
		TransactionIDs:               order.TransactionIDs,
		TransactionTypes:             order.TransactionTypes,
		ParentOrderID:                order.ParentOrder,
		ChildOrderID:                 order.ChildOrder,
		Provider:                     order.Provider,
		ProviderOrderID:              order.ProviderOrderID,
		BuyProvider:                  order.BuyProvider,
		SellProvider:                 order.SellProvider,
		Margin:                       order.Margin,
		RateID:                       order.RateID,
		ExpiresAt:                    order.ExpiresAt,
		DeletedAt:                    order.DeletedAt,
		CreatedAt:                    order.CreatedAt,
		UpdatedAt:                    order.UpdatedAt,
	}
}

func convertOrderDomainToModel(order *types.Order) *models.Order {
	if order == nil {
		return nil
	}

	return &models.Order{
		ID:                           order.ID,
		Platform:                     order.Platform,
		V1UserID:                     order.V1UserID,
		UserID:                       order.UserID,
		Type:                         order.Type,
		Flow:                         order.Flow,
		CurrencyID:                   order.CurrencyID,
		BuyChain:                     order.BuyChain,
		SellChain:                    order.SellChain,
		OpenAmount:                   order.OpenAmount,
		Amount:                       order.Amount,
		RefundAmount:                 order.RefundAmount,
		CalculatedAmount:             order.CalculatedAmount,
		OpenRate:                     order.OpenRate,
		Rate:                         order.Rate,
		CalculatedRate:               order.CalculatedRate,
		ProviderFeesPaid:             order.ProviderFeesPaid,
		BlockchainFeesPaid:           order.BlockchainFeesPaid,
		FeesGained:                   order.FeesGained,
		ProviderFeesToPay:            order.ProviderFeesToPay,
		BlockchainFeesToPay:          order.BlockchainFeesToPay,
		FeesToGain:                   order.FeesToGain,
		CalculatedProviderFeesPaid:   order.CalculatedProviderFeesPaid,
		CalculatedBlockchainFeesPaid: order.CalculatedBlockchainFeesPaid,
		CalculatedFeesGained:         order.CalculatedFeesGained,
		Status:                       order.Status,
		StatusTimestamps:             order.StatusTimestamps,
		V1ID:                         order.V1ID,
		DepositAccountIDs:            order.DepositAccountIDs,
		WithdrawalAccountIDs:         order.WithdrawalAccountIDs,
		RefundAccountID:              order.RefundAccountID,
		TransactionIDs:               order.TransactionIDs,
		TransactionTypes:             order.TransactionTypes,
		ParentOrder:                  order.ParentOrderID,
		ChildOrder:                   order.ChildOrderID,
		Provider:                     order.Provider,
		ProviderOrderID:              order.ProviderOrderID,
		BuyProvider:                  order.BuyProvider,
		SellProvider:                 order.SellProvider,
		Margin:                       order.Margin,
		RateID:                       order.RateID,
		ExpiresAt:                    order.ExpiresAt,
		DeletedAt:                    order.DeletedAt,
		CreatedAt:                    order.CreatedAt,
		UpdatedAt:                    order.UpdatedAt,
	}
}
