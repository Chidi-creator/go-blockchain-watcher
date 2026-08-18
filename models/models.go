package models

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Wallet represents a user's wallet
type Wallet struct {
	ID               primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID           primitive.ObjectID `bson:"userId" json:"userId"`
	Name             string             `bson:"name" json:"name"`
	WalletGroup      string             `bson:"walletGroup" json:"walletGroup"`
	HashedSeedPhrase string             `bson:"hashedSeedPhrase,omitempty" json:"hashedSeedPhrase,omitempty"`
	AccountDepth     int                `bson:"accountDepth" json:"accountDepth"`
	PrimaryWallet    bool               `bson:"primaryWallet" json:"primaryWallet"`
	IsLinked         bool               `bson:"isLinked" json:"isLinked"`
	ZapExchangeID    primitive.ObjectID `bson:"zapExchangeId,omitempty" json:"zapExchangeId,omitempty"`
	IsDeleted        bool               `bson:"isDeleted" json:"isDeleted"`
	DeletedAt        *time.Time         `bson:"deletedAt,omitempty" json:"deletedAt,omitempty"`
	CreatedAt        time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt        time.Time          `bson:"updatedAt" json:"updatedAt"`
}

// Account represents a wallet account
type Account struct {
	ID                  primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	CurrencyID          primitive.ObjectID `bson:"currencyId,omitempty" json:"currencyId,omitempty"`
	CountryID           primitive.ObjectID `bson:"countryId,omitempty" json:"countryId,omitempty"`
	BankID              primitive.ObjectID `bson:"bankId,omitempty" json:"bankId,omitempty"`
	WalletID            primitive.ObjectID `bson:"walletId,omitempty" json:"walletId,omitempty"`
	ChainID             primitive.ObjectID `bson:"chainId,omitempty" json:"chainId,omitempty"`
	UserID              primitive.ObjectID `bson:"userId,omitempty" json:"userId,omitempty"`
	PhysicalAddressID   primitive.ObjectID `bson:"physicalAddressId,omitempty" json:"physicalAddressId,omitempty"`
	SupportedCurrencyID primitive.ObjectID `bson:"supportedCurrencyId,omitempty" json:"supportedCurrencyId,omitempty"`
	ZapID               string             `bson:"zapId,omitempty" json:"zapId,omitempty"`
	Name                string             `bson:"name,omitempty" json:"name,omitempty"`
	HolderName          string             `bson:"holderName,omitempty" json:"holderName,omitempty"`
	Number              string             `bson:"number,omitempty" json:"number,omitempty"`
	WalletAddress       string             `bson:"walletAddress,omitempty" json:"walletAddress,omitempty"`
	IsPlayer            bool               `bson:"isPlayer" json:"isPlayer"`
	InflowSingleLimit   float64            `bson:"inflowSingleLimit" json:"inflowSingleLimit"`
	InflowDailyLimit    float64            `bson:"inflowDailyLimit" json:"inflowDailyLimit"`
	OutflowSingleLimit  float64            `bson:"outflowSingleLimit" json:"outflowSingleLimit"`
	OutflowDailyLimit   float64            `bson:"outflowDailyLimit" json:"outflowDailyLimit"`
	Balance             float64            `bson:"balance" json:"balance"`
	Type                string             `bson:"type,omitempty" json:"type,omitempty"`
	Position            string             `bson:"position,omitempty" json:"position,omitempty"`
	BenchStatus         bool               `bson:"benchStatus" json:"benchStatus"`
	Notes               string             `bson:"notes,omitempty" json:"notes,omitempty"`
	DeletedAt           *time.Time         `bson:"deletedAt,omitempty" json:"deletedAt,omitempty"`
	EncryptedPrivateKey string             `bson:"encryptedPrivateKey,omitempty" json:"encryptedPrivateKey,omitempty"`
	CreatedAt           time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt           time.Time          `bson:"updatedAt" json:"updatedAt"`
}

// Currency represents a currency
type Currency struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Name        string             `bson:"name" json:"name"`
	Symbol      string             `bson:"symbol" json:"symbol"`
	IsCrypto    bool               `bson:"isCrypto" json:"isCrypto"`
	Logo        string             `bson:"logo,omitempty" json:"logo,omitempty"`
	Website     string             `bson:"website,omitempty" json:"website,omitempty"`
	Explorer    string             `bson:"explorer,omitempty" json:"explorer,omitempty"`
	Description string             `bson:"description,omitempty" json:"description,omitempty"`
	CreatedAt   time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt   time.Time          `bson:"updatedAt" json:"updatedAt"`
}

// Chain represents a blockchain network
type Chain struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Name        string             `bson:"name" json:"name"`
	Symbol      string             `bson:"symbol" json:"symbol"`
	ChainID     interface{}        `bson:"chainId,omitempty" json:"chainId,omitempty"`
	NetworkType string             `bson:"networkType,omitempty" json:"networkType,omitempty"`
	RpcURL      string             `bson:"rpcUrl" json:"rpcUrl"`
	ExplorerURL string             `bson:"explorerUrl" json:"explorerUrl"`
	IconURL     string             `bson:"iconUrl,omitempty" json:"iconUrl,omitempty"`
	Status      string             `bson:"status" json:"status"`
	CreatedAt   time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt   time.Time          `bson:"updatedAt" json:"updatedAt"`
}

// GetChainIDString returns the chain ID as a string, handling different types
func (c *Chain) GetChainIDString() string {
	if c.ChainID == nil {
		return ""
	}

	switch v := c.ChainID.(type) {
	case string:
		return v
	case int:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%d", int(v))
	default:
		return fmt.Sprintf("%v", v)
	}
}

// SupportedCurrency represents a supported currency on a chain
type SupportedCurrency struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	ChainID      primitive.ObjectID `bson:"chainId" json:"chainId"`
	CurrencyID   primitive.ObjectID `bson:"currencyId" json:"currencyId"`
	TokenAddress string             `bson:"tokenAddress,omitempty" json:"tokenAddress,omitempty"`
	Decimals     int                `bson:"decimals" json:"decimals"`
	Status       string             `bson:"status" json:"status"`
	CreatedAt    time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt    time.Time          `bson:"updatedAt" json:"updatedAt"`
}

// TokenDetails represents details of a token on a blockchain
type TokenDetails struct {
	Name         string `json:"name"`
	Symbol       string `json:"symbol"`
	Decimals     int    `json:"decimals"`
	TotalSupply  string `json:"totalSupply"`
	TokenAddress string `json:"tokenAddress"`
	IconURL      string `json:"iconUrl,omitempty"`
	Website      string `json:"website,omitempty"`
	ChainID      string `json:"chainId"`
}

// TokenData represents token data in a portfolio
type TokenData struct {
	Symbol   string         `json:"symbol"`
	Balance  float64        `json:"balance"`
	Price    float64        `json:"price"`
	UsdValue float64        `json:"usdValue"`
	ChainID  ChainReference `json:"chainId"`
}

// ChainReference represents a reference to a chain
type ChainReference struct {
	ID     primitive.ObjectID `json:"id"`
	Symbol string             `json:"symbol"`
}

// AccountPortfolio represents a portfolio for a single account
type AccountPortfolio struct {
	AccountID     primitive.ObjectID `json:"accountId"`
	WalletAddress string             `json:"walletAddress,omitempty"`
	Token         TokenData          `json:"token"`
	TotalUsdValue float64            `json:"totalUsdValue"`
}

// WalletPortfolio represents a portfolio for a wallet
type WalletPortfolio struct {
	WalletID       primitive.ObjectID `json:"walletId"`
	Accounts       []AccountPortfolio `json:"accounts"`
	WalletUsdValue float64            `json:"walletUsdValue"`
}

// WalletPortfolioResponse represents the response structure for a wallet portfolio
type WalletPortfolioResponse struct {
	WalletID        primitive.ObjectID `json:"walletId"`
	Name            string             `json:"name"`
	Identifier      string             `json:"identifier"`
	WalletPortfolio []AccountPortfolio `json:"walletPortfolio"`
	WalletUsdValue  float64            `json:"walletUsdValue"`
}

// UpdateBalance updates the account balance and sets the updated time
func (a *Account) UpdateBalance(balance float64) {
	a.Balance = balance
	a.UpdatedAt = time.Now()
}

// Transaction represents a blockchain transaction
type Transaction struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	InitiatorId primitive.ObjectID `bson:"initiatorId" json:"initiatorId"`
	SenderId    primitive.ObjectID `bson:"senderId" json:"senderId"`
	ReceiverId  primitive.ObjectID `bson:"receiverId" json:"receiverId"`
	CurrencyID  primitive.ObjectID `bson:"currencyId" json:"currencyId"`
	ChainID     primitive.ObjectID `bson:"chainId" json:"chainId"`
	Hash        string             `bson:"hash,omitempty" json:"hash,omitempty"` // Used by some chains like ETH
	TxDate      time.Time          `bson:"txDate" json:"txDate"`
	Block       int64              `bson:"block,omitempty" json:"block,omitempty"`
	// FromAddress string             `bson:"fromAddress" json:"fromAddress"`
	// ToAddress   string             `bson:"toAddress" json:"toAddress"`
	Amount     float64   `bson:"amount" json:"amount"`
	Type       string    `bson:"type,omitempty" json:"type,omitempty"` // WALLET EXCHANGE
	NetworkFee float64   `bson:"networkFee,omitempty" json:"networkFee,omitempty"`
	Direction  string    `bson:"direction,omitempty" json:"direction,omitempty"` // SEND, RECEIVE
	Status     int       `bson:"status,omitempty" json:"status,omitempty"`       // Status CONFIRMED, PENDING, FAILED, etc.
	Success    bool      `bson:"success" json:"success"`                         // Transaction successful or failed
	CreatedAt  time.Time `bson:"createdAt" json:"createdAt"`
	UpdatedAt  time.Time `bson:"updatedAt" json:"updatedAt"`
}

// Order represents an order
type Order struct {
	ID                           primitive.ObjectID     `bson:"_id,omitempty" json:"id"`
	Platform                     string                 `bson:"platform" json:"platform"`
	V1UserID                     primitive.ObjectID     `bson:"v1userId,omitempty" json:"v1userId,omitempty"`
	UserID                       primitive.ObjectID     `bson:"userId" json:"userId"`
	Type                         string                 `bson:"type,omitempty" json:"type,omitempty"`
	Flow                         string                 `bson:"flow" json:"flow"`
	CurrencyID                   primitive.ObjectID     `bson:"currencyId" json:"currencyId"`
	BuyChain                     primitive.ObjectID     `bson:"buyChain,omitempty" json:"buyChain,omitempty"`
	SellChain                    primitive.ObjectID     `bson:"sellChain,omitempty" json:"sellChain,omitempty"`
	OpenAmount                   float64                `bson:"openAmount" json:"openAmount"`
	Amount                       float64                `bson:"amount" json:"amount"`
	RefundAmount                 float64                `bson:"refundAmount" json:"refundAmount"`
	CalculatedAmount             float64                `bson:"calculatedAmount" json:"calculatedAmount"`
	OpenRate                     float64                `bson:"openRate" json:"openRate"`
	Rate                         float64                `bson:"rate" json:"rate"`
	CalculatedRate               float64                `bson:"calculatedRate" json:"calculatedRate"`
	ProviderFeesPaid             float64                `bson:"providerFeesPaid" json:"providerFeesPaid"`
	BlockchainFeesPaid           float64                `bson:"blockchainFeesPaid" json:"blockchainFeesPaid"`
	FeesGained                   float64                `bson:"feesGained" json:"feesGained"`
	ProviderFeesToPay            float64                `bson:"providerFeesToPay" json:"providerFeesToPay"`
	BlockchainFeesToPay          float64                `bson:"blockchainFeesToPay" json:"blockchainFeesToPay"`
	FeesToGain                   float64                `bson:"feesToGain" json:"feesToGain"`
	CalculatedProviderFeesPaid   float64                `bson:"calculatedProviderFeesPaid" json:"calculatedProviderFeesPaid"`
	CalculatedBlockchainFeesPaid float64                `bson:"calculatedBlockchainFeesPaid" json:"calculatedBlockchainFeesPaid"`
	CalculatedFeesGained         float64                `bson:"calculatedFeesGained" json:"calculatedFeesGained"`
	Status                       string                 `bson:"status" json:"status"`
	StatusTimestamps             map[string]interface{} `bson:"statusTimestamps" json:"statusTimestamps"`
	V1ID                         string                 `bson:"v1Id,omitempty" json:"v1Id,omitempty"`
	DepositAccountIDs            []primitive.ObjectID   `bson:"depositAccountIds" json:"depositAccountIds"`
	WithdrawalAccountIDs         []primitive.ObjectID   `bson:"withdrawalAccountIds" json:"withdrawalAccountIds"`
	RefundAccountID              primitive.ObjectID     `bson:"refundAccountId,omitempty" json:"refundAccountId,omitempty"`
	TransactionIDs               []primitive.ObjectID   `bson:"transactionIds" json:"transactionIds"`
	TransactionTypes             []string               `bson:"transactionTypes" json:"transactionTypes"`
	ParentOrder                  primitive.ObjectID     `bson:"parentOrder,omitempty" json:"parentOrder,omitempty"`
	ChildOrder                   primitive.ObjectID     `bson:"childOrder,omitempty" json:"childOrder,omitempty"`
	Provider                     string                 `bson:"provider,omitempty" json:"provider,omitempty"`
	ProviderOrderID              string                 `bson:"providerOrderId,omitempty" json:"providerOrderId,omitempty"`
	BuyProvider                  string                 `bson:"buyProvider,omitempty" json:"buyProvider,omitempty"`
	SellProvider                 string                 `bson:"sellProvider,omitempty" json:"sellProvider,omitempty"`
	Margin                       float64                `bson:"margin" json:"margin"`
	RateID                       string                 `bson:"rateId,omitempty" json:"rateId,omitempty"`
	ExpiresAt                    time.Time              `bson:"expiresAt" json:"expiresAt"`
	DeletedAt                    *time.Time             `bson:"deletedAt,omitempty" json:"deletedAt,omitempty"`
	CreatedAt                    time.Time              `bson:"createdAt" json:"createdAt"`
	UpdatedAt                    time.Time              `bson:"updatedAt" json:"updatedAt"`
}
