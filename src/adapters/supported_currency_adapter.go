package adapters

import (
	"context"

	"bitbucket.org/zapspace/zap-go-server/config/mongodb"
	"bitbucket.org/zapspace/zap-go-server/models"
	domainrepo "bitbucket.org/zapspace/zap-go-server/src/repositories"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// SupportedCurrencyRepositoryAdapter adapts mongodb.SupportedCurrencyRepository to domain interface
type SupportedCurrencyRepositoryAdapter struct {
	repo mongodb.SupportedCurrencyRepository
}

// NewSupportedCurrencyRepositoryAdapter creates a new supported currency repository adapter
func NewSupportedCurrencyRepositoryAdapter(repo mongodb.SupportedCurrencyRepository) domainrepo.SupportedCurrencyRepository {
	return &SupportedCurrencyRepositoryAdapter{repo: repo}
}

// GetByID retrieves a supported currency by its ID
func (a *SupportedCurrencyRepositoryAdapter) GetByID(ctx context.Context, id primitive.ObjectID) (*models.SupportedCurrency, error) {
	return a.repo.GetSupportedCurrencyByID(ctx, id)
}

// GetBySymbol retrieves a supported currency by its symbol
func (a *SupportedCurrencyRepositoryAdapter) GetBySymbol(ctx context.Context, symbol string) (*models.SupportedCurrency, error) {
	return a.repo.GetSupportedCurrencyBySymbol(ctx, symbol)
}

// GetAll retrieves all supported currencies
func (a *SupportedCurrencyRepositoryAdapter) GetAll(ctx context.Context) ([]*models.SupportedCurrency, error) {
	supportedCurrencies, err := a.repo.GetSupportedCurrencies(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*models.SupportedCurrency, len(supportedCurrencies))
	for i, currency := range supportedCurrencies {
		curr := currency // Create a copy to avoid pointer issues
		result[i] = &curr
	}
	return result, nil
}

// GetBatch retrieves a batch of supported currencies with pagination
func (a *SupportedCurrencyRepositoryAdapter) GetBatch(ctx context.Context, limit, skip int) ([]*models.SupportedCurrency, error) {
	// Fetch all currencies
	supportedCurrencies, err := a.repo.GetSupportedCurrencies(ctx)
	if err != nil {
		return nil, err
	}

	// Apply pagination manually since we don't have repository support yet
	// Calculate end index with bounds checking
	startIdx := skip
	endIdx := skip + limit

	// Bounds check
	if startIdx >= len(supportedCurrencies) {
		return []*models.SupportedCurrency{}, nil
	}

	if endIdx > len(supportedCurrencies) {
		endIdx = len(supportedCurrencies)
	}

	// Create sliced result
	sliced := supportedCurrencies[startIdx:endIdx]
	result := make([]*models.SupportedCurrency, len(sliced))

	for i, currency := range sliced {
		curr := currency // Create a copy to avoid pointer issues
		result[i] = &curr
	}

	return result, nil
}
