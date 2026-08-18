package repositories

import (
	"context"

	"bitbucket.org/zapspace/zap-go-server/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// CurrencyRepository defines the interface for currency domain operations
type CurrencyRepository interface {
	GetByID(ctx context.Context, id primitive.ObjectID) (*models.Currency, error)
	GetBySymbol(ctx context.Context, symbol string) (*models.Currency, error)
	GetAll(ctx context.Context) ([]*models.Currency, error)
}
