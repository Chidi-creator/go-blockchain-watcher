package types

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Wallet represents a user's wallet entity in the domain
type Wallet struct {
	ID               primitive.ObjectID
	UserID           primitive.ObjectID
	Name             string
	WalletGroup      string
	HashedSeedPhrase string
	AccountDepth     int
	PrimaryWallet    bool
	IsLinked         bool
	ZapExchangeID    primitive.ObjectID
	IsDeleted        bool
	DeletedAt        *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// NewWallet creates a new wallet entity
func NewWallet(userID primitive.ObjectID, name, hashedSeedPhrase string, accountDepth int) *Wallet {
	now := time.Now()
	return &Wallet{
		ID:               primitive.NewObjectID(),
		UserID:           userID,
		Name:             name,
		WalletGroup:      "Group " + string(rune(accountDepth)),
		HashedSeedPhrase: hashedSeedPhrase,
		AccountDepth:     accountDepth,
		PrimaryWallet:    false,
		IsLinked:         false,
		IsDeleted:        false,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

// MarkAsDeleted marks the wallet as deleted
func (w *Wallet) MarkAsDeleted() {
	now := time.Now()
	w.IsDeleted = true
	w.DeletedAt = &now
	w.UpdatedAt = now
}

// SetAsPrimary sets this wallet as the primary wallet
func (w *Wallet) SetAsPrimary() {
	w.PrimaryWallet = true
	w.UpdatedAt = time.Now()
}

// IncreaseAccountDepth increases the account depth by 1
func (w *Wallet) IncreaseAccountDepth() {
	w.AccountDepth++
	w.UpdatedAt = time.Now()
}

// LinkWallet marks the wallet as linked to an external system
func (w *Wallet) LinkWallet(zapExchangeID primitive.ObjectID) {
	w.IsLinked = true
	w.ZapExchangeID = zapExchangeID
	w.UpdatedAt = time.Now()
}

// UpdateWallet updates wallet details
func (w *Wallet) UpdateWallet(name string, walletGroup string) {
	if name != "" {
		w.Name = name
	}

	if walletGroup != "" {
		w.WalletGroup = walletGroup
	}

	w.UpdatedAt = time.Now()
}
