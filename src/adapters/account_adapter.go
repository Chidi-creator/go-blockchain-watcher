package adapters

import (
	"context"

	"bitbucket.org/zapspace/zap-go-server/config/mongodb"
	"bitbucket.org/zapspace/zap-go-server/models"
	domainrepo "bitbucket.org/zapspace/zap-go-server/src/repositories"
	"bitbucket.org/zapspace/zap-go-server/src/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// AccountRepositoryAdapter adapts mongodb.AccountRepository to domain interface
type AccountRepositoryAdapter struct {
	repo mongodb.AccountRepository
}

// NewAccountRepositoryAdapter creates a new account repository adapter
func NewAccountRepositoryAdapter(repo mongodb.AccountRepository) domainrepo.AccountRepository {
	return &AccountRepositoryAdapter{repo: repo}
}

// GetByID retrieves an account by its ID
func (a *AccountRepositoryAdapter) GetByID(ctx context.Context, id primitive.ObjectID) (*types.Account, error) {
	account, err := a.repo.GetAccountByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return convertAccountModelToDomain(account), nil
}

// GetByWalletID retrieves all accounts for a wallet
func (a *AccountRepositoryAdapter) GetByWalletID(ctx context.Context, walletID primitive.ObjectID) ([]*types.Account, error) {
	accounts, err := a.repo.GetAccountsByWalletID(ctx, walletID)
	if err != nil {
		return nil, err
	}

	domainAccounts := make([]*types.Account, len(accounts))
	for i, account := range accounts {
		domainAccounts[i] = convertAccountModelToDomain(&account)
	}
	return domainAccounts, nil
}

// GetByAddress retrieves an account by its address
func (a *AccountRepositoryAdapter) GetByAddress(ctx context.Context, address string) (*types.Account, error) {
	account, err := a.repo.GetAccountByAddress(ctx, address)
	if err != nil {
		return nil, err
	}
	return convertAccountModelToDomain(account), nil
}

// CountByAddress counts the number of accounts by address
func (a *AccountRepositoryAdapter) CountByAddress(ctx context.Context, address string) (int64, error) {
	count, err := a.repo.CountByAddress(ctx, address)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// CheckDuplicateAccount checks if an account already exists for a given wallet address and user
func (a *AccountRepositoryAdapter) CheckDuplicateAccount(ctx context.Context, walletAddress string, userID, chainID, currencyID primitive.ObjectID) (bool, error) {
	return a.repo.CheckDuplicateAccount(ctx, walletAddress, userID, chainID, currencyID)
}

// Create creates a new account
func (a *AccountRepositoryAdapter) Create(ctx context.Context, account *types.Account) error {
	model := convertAccountDomainToModel(account)
	result, err := a.repo.CreateAccount(ctx, model)
	if err != nil {
		return err
	}

	// Update ID if it was generated
	account.ID = result.ID
	return nil
}

// Update updates an existing account
func (a *AccountRepositoryAdapter) Update(ctx context.Context, account *types.Account) error {
	model := convertAccountDomainToModel(account)
	return a.repo.UpdateAccount(ctx, model)
}

// Delete deletes an account
func (a *AccountRepositoryAdapter) Delete(ctx context.Context, id primitive.ObjectID) error {
	return a.repo.DeleteAccount(ctx, id)
}

// Helper functions for account conversions
func convertAccountModelToDomain(account *models.Account) *types.Account {
	if account == nil {
		return nil
	}

	return &types.Account{
		ID:                  account.ID,
		CurrencyID:          account.CurrencyID,
		ChainID:             account.ChainID,
		WalletID:            account.WalletID,
		UserID:              account.UserID,
		SupportedCurrencyID: account.SupportedCurrencyID,
		Type:                account.Type,
		WalletAddress:       account.WalletAddress,
		Balance:             account.Balance,
		EncryptedPrivateKey: account.EncryptedPrivateKey,
		DeletedAt:           account.DeletedAt,
		CreatedAt:           account.CreatedAt,
		UpdatedAt:           account.UpdatedAt,
	}
}

func convertAccountDomainToModel(account *types.Account) *models.Account {
	if account == nil {
		return nil
	}

	return &models.Account{
		ID:                  account.ID,
		CurrencyID:          account.CurrencyID,
		ChainID:             account.ChainID,
		WalletID:            account.WalletID,
		UserID:              account.UserID,
		SupportedCurrencyID: account.SupportedCurrencyID,
		Type:                account.Type,
		WalletAddress:       account.WalletAddress,
		Balance:             account.Balance,
		EncryptedPrivateKey: account.EncryptedPrivateKey,
		DeletedAt:           account.DeletedAt,
		CreatedAt:           account.CreatedAt,
		UpdatedAt:           account.UpdatedAt,
	}
}
