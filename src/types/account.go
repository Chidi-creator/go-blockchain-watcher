package types

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Account represents a wallet account entity in the domain
type Account struct {
	ID                  primitive.ObjectID
	CurrencyID          primitive.ObjectID
	ChainID             primitive.ObjectID
	WalletID            primitive.ObjectID
	UserID              primitive.ObjectID
	SupportedCurrencyID primitive.ObjectID
	Type                string
	WalletAddress       string
	Balance             float64
	EncryptedPrivateKey string
	DeletedAt           *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// NewAccount creates a new account entity
func NewAccount(
	walletID, userID, currencyID, chainID primitive.ObjectID,
	walletAddress, encryptedPrivateKey string,
) *Account {
	now := time.Now()
	return &Account{
		ID:                  primitive.NewObjectID(),
		WalletID:            walletID,
		UserID:              userID,
		CurrencyID:          currencyID,
		ChainID:             chainID,
		WalletAddress:       walletAddress,
		EncryptedPrivateKey: encryptedPrivateKey,
		Balance:             0,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
}

// UpdateBalance updates the account balance
func (a *Account) UpdateBalance(balance float64) {
	a.Balance = balance
	a.UpdatedAt = time.Now()
}

// MarkAsDeleted marks the account as deleted
func (a *Account) MarkAsDeleted() {
	now := time.Now()
	a.DeletedAt = &now
	a.UpdatedAt = now
}

// IsDeleted checks if the account is deleted
func (a *Account) IsDeleted() bool {
	return a.DeletedAt != nil
}

// HasWalletAddress checks if the account has a wallet address
func (a *Account) HasWalletAddress() bool {
	return a.WalletAddress != ""
}
