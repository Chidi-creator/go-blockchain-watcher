package adapters

import (
	"context"

	"bitbucket.org/zapspace/zap-go-server/config/mongodb"
	"bitbucket.org/zapspace/zap-go-server/models"
	domainrepo "bitbucket.org/zapspace/zap-go-server/src/repositories"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ChainRepositoryAdapter adapts mongodb.ChainRepository to domain interface
type ChainRepositoryAdapter struct {
	repo mongodb.ChainRepository
}

// NewChainRepositoryAdapter creates a new chain repository adapter
func NewChainRepositoryAdapter(repo mongodb.ChainRepository) domainrepo.ChainRepository {
	return &ChainRepositoryAdapter{repo: repo}
}

// GetByID retrieves a chain by its ID
func (a *ChainRepositoryAdapter) GetByID(ctx context.Context, id primitive.ObjectID) (*models.Chain, error) {
	return a.repo.GetChainByID(ctx, id)
}

// GetBySymbol retrieves a chain by its symbol
func (a *ChainRepositoryAdapter) GetBySymbol(ctx context.Context, symbol string) (*models.Chain, error) {
	return a.repo.GetChainBySymbol(ctx, symbol)
}

// GetAll retrieves all chains
func (a *ChainRepositoryAdapter) GetAll(ctx context.Context) ([]*models.Chain, error) {
	chains, err := a.repo.GetChains(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*models.Chain, len(chains))
	for i, chain := range chains {
		ch := chain // Create a copy to avoid pointer issues
		result[i] = &ch
	}
	return result, nil
}
