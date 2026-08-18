package types

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TokenData represents token data in a portfolio
type TokenData struct {
	Symbol   string
	Balance  float64
	Price    float64
	UsdValue float64
	ChainID  ChainReference
}

// ChainReference represents a reference to a chain
type ChainReference struct {
	ID     primitive.ObjectID
	Symbol string
}

// AccountPortfolio represents a portfolio for a single account
type AccountPortfolio struct {
	AccountID     primitive.ObjectID
	WalletAddress string
	Token         TokenData
	TotalUsdValue float64
}

// WalletPortfolio represents a portfolio for a wallet
type WalletPortfolio struct {
	WalletID       primitive.ObjectID
	Accounts       []AccountPortfolio
	WalletUsdValue float64
}

// WalletPortfolioResponse represents the response structure for a wallet portfolio
type WalletPortfolioResponse struct {
	WalletID        primitive.ObjectID
	Name            string
	Identifier      string
	WalletPortfolio []AccountPortfolio
	WalletUsdValue  float64
}

// NewTokenData creates a new token data value object
func NewTokenData(symbol string, balance, price float64, chainID ChainReference) TokenData {
	return TokenData{
		Symbol:   symbol,
		Balance:  balance,
		Price:    price,
		UsdValue: balance * price,
		ChainID:  chainID,
	}
}

// NewChainReference creates a new chain reference
func NewChainReference(id primitive.ObjectID, symbol string) ChainReference {
	return ChainReference{
		ID:     id,
		Symbol: symbol,
	}
}

// NewAccountPortfolio creates a new account portfolio
func NewAccountPortfolio(accountID primitive.ObjectID, walletAddress string, token TokenData) AccountPortfolio {
	return AccountPortfolio{
		AccountID:     accountID,
		WalletAddress: walletAddress,
		Token:         token,
		TotalUsdValue: token.UsdValue,
	}
}

// NewWalletPortfolio creates a new wallet portfolio
func NewWalletPortfolio(walletID primitive.ObjectID, accounts []AccountPortfolio) WalletPortfolio {
	var totalUsdValue float64
	for _, account := range accounts {
		totalUsdValue += account.TotalUsdValue
	}

	return WalletPortfolio{
		WalletID:       walletID,
		Accounts:       accounts,
		WalletUsdValue: totalUsdValue,
	}
}

// NewWalletPortfolioResponse creates a new wallet portfolio response
func NewWalletPortfolioResponse(walletID primitive.ObjectID, name, identifier string, portfolio WalletPortfolio) WalletPortfolioResponse {
	return WalletPortfolioResponse{
		WalletID:        walletID,
		Name:            name,
		Identifier:      identifier,
		WalletPortfolio: portfolio.Accounts,
		WalletUsdValue:  portfolio.WalletUsdValue,
	}
}
