package repositories

import (
	"context"

	entities "bitbucket.org/zapspace/zap-go-server/src/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// AccountRepository defines the interface for account domain operations
type AccountRepository interface {
	GetByID(ctx context.Context, id primitive.ObjectID) (*entities.Account, error)
	GetByWalletID(ctx context.Context, walletID primitive.ObjectID) ([]*entities.Account, error)
	GetByAddress(ctx context.Context, address string) (*entities.Account, error)
	CountByAddress(ctx context.Context, address string) (int64, error)
	CheckDuplicateAccount(ctx context.Context, walletAddress string, userId, chainId, currencyId primitive.ObjectID) (bool, error)
	Create(ctx context.Context, account *entities.Account) error
	Update(ctx context.Context, account *entities.Account) error
	Delete(ctx context.Context, id primitive.ObjectID) error
}
