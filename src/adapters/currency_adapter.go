package adapters

import (
	"context"

	"bitbucket.org/zapspace/zap-go-server/config/mongodb"
	"bitbucket.org/zapspace/zap-go-server/models"
	domainrepo "bitbucket.org/zapspace/zap-go-server/src/repositories"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// CurrencyRepositoryAdapter adapts mongodb.CurrencyRepository to domain interface
type CurrencyRepositoryAdapter struct {
	repo mongodb.CurrencyRepository
}

// NewCurrencyRepositoryAdapter creates a new currency repository adapter
func NewCurrencyRepositoryAdapter(repo mongodb.CurrencyRepository) domainrepo.CurrencyRepository {
	return &CurrencyRepositoryAdapter{repo: repo}
}

// GetByID retrieves a currency by its ID
func (a *CurrencyRepositoryAdapter) GetByID(ctx context.Context, id primitive.ObjectID) (*models.Currency, error) {
	return a.repo.GetCurrencyByID(ctx, id)
}

// GetBySymbol retrieves a currency by its symbol
func (a *CurrencyRepositoryAdapter) GetBySymbol(ctx context.Context, symbol string) (*models.Currency, error) {
	return a.repo.GetCurrencyBySymbol(ctx, symbol)
}

// GetAll retrieves all currencies
func (a *CurrencyRepositoryAdapter) GetAll(ctx context.Context) ([]*models.Currency, error) {
	currencies, err := a.repo.GetCurrencies(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*models.Currency, len(currencies))
	for i, currency := range currencies {
		curr := currency // Create a copy to avoid pointer issues
		result[i] = &curr
	}
	return result, nil
}
