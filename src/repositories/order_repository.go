package repositories

import (
	"context"

	"bitbucket.org/zapspace/zap-go-server/src/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// OrderRepository defines the interface for order domain operations
type OrderRepository interface {
	GetByID(ctx context.Context, id primitive.ObjectID) (*types.Order, error)
	GetByUserID(ctx context.Context, userID primitive.ObjectID) ([]*types.Order, error)
	GetByDepositAddress(ctx context.Context, depositAddress string) (*types.Order, error)
	Create(ctx context.Context, order *types.Order) error
	Update(ctx context.Context, order *types.Order) error
	Delete(ctx context.Context, id primitive.ObjectID) error
}
