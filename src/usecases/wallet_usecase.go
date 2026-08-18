package usecases

import (
	"context"
	"errors"

	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/src/repositories"
	"bitbucket.org/zapspace/zap-go-server/src/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Common errors
var (
	ErrWalletNotFound = errors.New("wallet not found")
	ErrInvalidUserID  = errors.New("invalid user ID")
)

// WalletUseCase handles wallet related business logic
type WalletUseCase struct {
	walletRepo repositories.WalletRepository
	logger     logger.Logger
}

// NewWalletUseCase creates a new wallet use case
func NewWalletUseCase(walletRepo repositories.WalletRepository, logger logger.Logger) *WalletUseCase {
	return &WalletUseCase{
		walletRepo: walletRepo,
		logger:     logger,
	}
}

// CreateWallet creates a new wallet
func (uc *WalletUseCase) CreateWallet(ctx context.Context, userID primitive.ObjectID, name, hashedSeedPhrase string, accountDepth int) (*types.Wallet, error) {
	if userID.IsZero() {
		return nil, ErrInvalidUserID
	}

	wallet := types.NewWallet(userID, name, hashedSeedPhrase, accountDepth)

	err := uc.walletRepo.Create(ctx, wallet)
	if err != nil {
		uc.logger.Error("Failed to create wallet", "error", err)
		return nil, err
	}

	return wallet, nil
}

// GetWalletByID retrieves a wallet by its ID
func (uc *WalletUseCase) GetWalletByID(ctx context.Context, walletID primitive.ObjectID) (*types.Wallet, error) {
	wallet, err := uc.walletRepo.GetByID(ctx, walletID)
	if err != nil {
		uc.logger.Error("Failed to get wallet by ID", "walletId", walletID.Hex(), "error", err)
		return nil, err
	}

	if wallet == nil {
		return nil, ErrWalletNotFound
	}

	return wallet, nil
}

// GetWalletsByUserID retrieves all wallets for a user
func (uc *WalletUseCase) GetWalletsByUserID(ctx context.Context, userID primitive.ObjectID) ([]*types.Wallet, error) {
	if userID.IsZero() {
		return nil, ErrInvalidUserID
	}

	wallets, err := uc.walletRepo.GetByUserID(ctx, userID)
	if err != nil {
		uc.logger.Error("Failed to get wallets by user ID", "userId", userID.Hex(), "error", err)
		return nil, err
	}

	return wallets, nil
}

// UpdateWallet updates an existing wallet
func (uc *WalletUseCase) UpdateWallet(ctx context.Context, wallet *types.Wallet) error {
	if wallet.ID.IsZero() {
		return errors.New("wallet ID is required")
	}

	// First check if wallet exists
	_, err := uc.walletRepo.GetByID(ctx, wallet.ID)
	if err != nil {
		uc.logger.Error("Failed to get wallet for update", "walletId", wallet.ID.Hex(), "error", err)
		return err
	}

	// Update wallet
	err = uc.walletRepo.Update(ctx, wallet)
	if err != nil {
		uc.logger.Error("Failed to update wallet", "walletId", wallet.ID.Hex(), "error", err)
		return err
	}

	return nil
}

// DeleteWallet soft deletes a wallet
func (uc *WalletUseCase) DeleteWallet(ctx context.Context, walletID primitive.ObjectID) error {
	wallet, err := uc.walletRepo.GetByID(ctx, walletID)
	if err != nil {
		uc.logger.Error("Failed to get wallet for deletion", "walletId", walletID.Hex(), "error", err)
		return err
	}

	if wallet == nil {
		return ErrWalletNotFound
	}

	wallet.MarkAsDeleted()

	err = uc.walletRepo.Update(ctx, wallet)
	if err != nil {
		uc.logger.Error("Failed to mark wallet as deleted", "walletId", walletID.Hex(), "error", err)
		return err
	}

	return nil
}
