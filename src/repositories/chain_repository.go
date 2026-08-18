package repositories

import (
	"context"

	"bitbucket.org/zapspace/zap-go-server/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ChainRepository defines the interface for chain domain operations
type ChainRepository interface {
	GetByID(ctx context.Context, id primitive.ObjectID) (*models.Chain, error)
	GetBySymbol(ctx context.Context, symbol string) (*models.Chain, error)
	GetAll(ctx context.Context) ([]*models.Chain, error)
}
