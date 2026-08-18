package adapters

import (
	"context"

	"bitbucket.org/zapspace/zap-go-server/config/mongodb"
	"bitbucket.org/zapspace/zap-go-server/models"
	domainrepo "bitbucket.org/zapspace/zap-go-server/src/repositories"
	"bitbucket.org/zapspace/zap-go-server/src/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// WalletRepositoryAdapter adapts mongodb.WalletRepository to domain interface
type WalletRepositoryAdapter struct {
	repo mongodb.WalletRepository
}

// NewWalletRepositoryAdapter creates a new wallet repository adapter
func NewWalletRepositoryAdapter(repo mongodb.WalletRepository) domainrepo.WalletRepository {
	return &WalletRepositoryAdapter{repo: repo}
}

// GetByID retrieves a wallet by its ID
func (a *WalletRepositoryAdapter) GetByID(ctx context.Context, id primitive.ObjectID) (*types.Wallet, error) {
	wallet, err := a.repo.GetWalletByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return convertWalletModelToDomain(wallet), nil
}

// GetByUserID retrieves all wallets for a user
func (a *WalletRepositoryAdapter) GetByUserID(ctx context.Context, userID primitive.ObjectID) ([]*types.Wallet, error) {
	wallets, err := a.repo.GetWalletsByUserId(ctx, userID)
	if err != nil {
		return nil, err
	}

	domainWallets := make([]*types.Wallet, len(wallets))
	for i, wallet := range wallets {
		domainWallets[i] = convertWalletModelToDomain(&wallet)
	}
	return domainWallets, nil
}

// Create creates a new wallet
func (a *WalletRepositoryAdapter) Create(ctx context.Context, wallet *types.Wallet) error {
	model := convertWalletDomainToModel(wallet)
	result, err := a.repo.CreateWallet(ctx, model)
	if err != nil {
		return err
	}

	// Update ID if it was generated
	wallet.ID = result.ID
	return nil
}

// Update updates an existing wallet
func (a *WalletRepositoryAdapter) Update(ctx context.Context, wallet *types.Wallet) error {
	model := convertWalletDomainToModel(wallet)
	return a.repo.UpdateWallet(ctx, model)
}

// Delete deletes a wallet
func (a *WalletRepositoryAdapter) Delete(ctx context.Context, id primitive.ObjectID) error {
	return a.repo.DeleteWallet(ctx, id)
}

// Helper functions for wallet conversions
func convertWalletModelToDomain(wallet *models.Wallet) *types.Wallet {
	if wallet == nil {
		return nil
	}

	return &types.Wallet{
		ID:        wallet.ID,
		UserID:    wallet.UserID,
		Name:      wallet.Name,
		CreatedAt: wallet.CreatedAt,
		UpdatedAt: wallet.UpdatedAt,
	}
}

func convertWalletDomainToModel(wallet *types.Wallet) *models.Wallet {
	if wallet == nil {
		return nil
	}

	return &models.Wallet{
		ID:        wallet.ID,
		UserID:    wallet.UserID,
		Name:      wallet.Name,
		CreatedAt: wallet.CreatedAt,
		UpdatedAt: wallet.UpdatedAt,
	}
}
