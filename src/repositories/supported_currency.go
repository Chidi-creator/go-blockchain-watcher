package repositories

import (
	"context"

	"bitbucket.org/zapspace/zap-go-server/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// SupportedCurrencyRepository defines the interface for supported currency domain operations
type SupportedCurrencyRepository interface {
	GetByID(ctx context.Context, id primitive.ObjectID) (*models.SupportedCurrency, error)
	GetBySymbol(ctx context.Context, symbol string) (*models.SupportedCurrency, error)
	GetAll(ctx context.Context) ([]*models.SupportedCurrency, error)
	// GetBatch retrieves a batch of supported currencies with pagination
	GetBatch(ctx context.Context, limit, skip int) ([]*models.SupportedCurrency, error)
}
