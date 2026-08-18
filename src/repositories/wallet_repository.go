package repositories

import (
	"context"

	entities "bitbucket.org/zapspace/zap-go-server/src/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// WalletRepository defines the interface for wallet domain operations
type WalletRepository interface {
	GetByID(ctx context.Context, id primitive.ObjectID) (*entities.Wallet, error)
	GetByUserID(ctx context.Context, userID primitive.ObjectID) ([]*entities.Wallet, error)
	Create(ctx context.Context, wallet *entities.Wallet) error
	Update(ctx context.Context, wallet *entities.Wallet) error
	Delete(ctx context.Context, id primitive.ObjectID) error
}
