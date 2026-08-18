package changenow

import (
	"time"

	"bitbucket.org/zapspace/zap-go-server/models"
)

// ChangeNowRateResponse represents the response from the ChangeNow estimated-amount endpoint
type ChangeNowRateResponse struct {
	FromAmount    float64   `json:"fromAmount"`
	ToAmount      float64   `json:"toAmount"`
	Flow          string    `json:"flow"`
	Type          string    `json:"type"`
	DepositFee    float64   `json:"depositFee"`
	WithdrawalFee float64   `json:"withdrawalFee"`
	RateID        string    `json:"rateId"`
	ValidUntil    time.Time `json:"validUntil"`
}

// ChangeNowRangeResponse represents the response from the ChangeNow range endpoint
type ChangeNowRangeResponse struct {
	MinAmount float64 `json:"minAmount"`
	MaxAmount float64 `json:"maxAmount"`
}

// ChangeNowExchangeRequest represents the request to create an exchange
type ChangeNowExchangeRequest struct {
	FromCurrency  string  `json:"fromCurrency"`
	ToCurrency    string  `json:"toCurrency"`
	FromNetwork   string  `json:"fromNetwork"`
	ToNetwork     string  `json:"toNetwork"`
	Address       string  `json:"address"`
	Flow          string  `json:"flow"`
	RateID        string  `json:"rateId"`
	FromAmount    float64 `json:"fromAmount,omitempty"`
	ToAmount      float64 `json:"toAmount,omitempty"`
	RefundAddress string  `json:"refundAddress,omitempty"`
	ExtraID       string  `json:"extraId,omitempty"`
	RefundExtraID string  `json:"refundExtraId,omitempty"`
	UserID        string  `json:"userId,omitempty"`
	Payload       string  `json:"payload,omitempty"`
	ContactEmail  string  `json:"contactEmail,omitempty"`
	Source        string  `json:"source,omitempty"`
	Type          string  `json:"type"`
}

// ChangeNowExchangeResponse represents the response from creating an exchange
type ChangeNowExchangeResponse struct {
	PayinAddress  string  `json:"payinAddress"`
	FromAmount    float64 `json:"fromAmount"`
	ToAmount      float64 `json:"toAmount"`
	ID            string  `json:"id"`
	ExpiresAt     string  `json:"expiresAt,omitempty"`
	FromCurrency  string  `json:"fromCurrency,omitempty"`
	ToCurrency    string  `json:"toCurrency,omitempty"`
	PayoutAddress string  `json:"payoutAddress,omitempty"`
}

// ChangeNowTransactionStatusResponse represents the response from the transaction status endpoint
type ChangeNowTransactionStatusResponse struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	FromCurrency  string `json:"fromCurrency"`
	ToCurrency    string `json:"toCurrency"`
	PayinAddress  string `json:"payinAddress"`
	PayoutAddress string `json:"payoutAddress"`
	PayinExtraID  string `json:"payinExtraId,omitempty"`
	Amount        string `json:"amount,omitempty"`
	PayoutAmount  string `json:"payoutAmount,omitempty"`
	PayoutExtraID string `json:"payoutExtraId,omitempty"`
	CreatedAt     string `json:"createdAt,omitempty"`
	UpdatedAt     string `json:"updatedAt,omitempty"`
	ExpiresAt     string `json:"expiresAt,omitempty"`
	PayinHash     string `json:"payinHash,omitempty"`
	PayoutHash    string `json:"payoutHash,omitempty"`
}

// ChangeNowCurrency represents a currency from the ChangeNow available currencies endpoint
type ChangeNowCurrency struct {
	Ticker            string   `json:"ticker"`
	Name              string   `json:"name"`
	Image             string   `json:"image"`
	HasExternalID     bool     `json:"has_external_id"`
	IsFiat            bool     `json:"is_fiat"`
	Featured          bool     `json:"featured"`
	IsStable          bool     `json:"is_stable"`
	SupportsFixedRate bool     `json:"supportsFixedRate"`
	Networks          []string `json:"networks"`
}

// ChangeNowJobData represents job data for watching transactions
type ChangeNowJobData struct {
	SupportedCurrency *models.SupportedCurrency `json:"supportedCurrency"`
	Address           string                    `json:"address"`
	Order             map[string]interface{}    `json:"order,omitempty"`
	ProviderID        string                    `json:"providerId,omitempty"`
}

// ChangeNowMarket represents currency and chain information for a transaction
type ChangeNowMarket struct {
	From         ChangeNowMarketPair
	To           ChangeNowMarketPair
	BuyCurrency  *models.Currency
	SellCurrency *models.Currency
	BuyChain     *models.Chain
	SellChain    *models.Chain
}

// ChangeNowMarketPair represents a currency/chain pair for ChangeNow API
type ChangeNowMarketPair struct {
	Currency string
	Chain    string
}

// WithdrawResult contains the result of a withdrawal transaction
type WithdrawResult struct {
	TxHash string
}

// GetOrderRatesProviderParams contains parameters for getting order rates
type GetOrderRatesProviderParams struct {
	SupportedCurrency *models.SupportedCurrency
	Amount            float64
	IsReversed        bool
	OrderFlow         string
}

// OrderRateProviderData contains provider rate information
type OrderRateProviderData struct {
	Rate         float64
	MinMax       MinMaxAmount
	ProviderFees float64
	ChainFees    float64
	Expiry       time.Time
	RateID       string
	USDChain     string
}

// MinMaxAmount represents minimum and maximum amounts
type MinMaxAmount struct {
	Min float64
	Max float64
}

// CreateDepositAccountParams contains parameters for creating a deposit account
type CreateDepositAccountParams struct {
	SupportedCurrency *models.SupportedCurrency
	Amount            float64
	IsReversed        bool
	RefundAddress     string
	WithdrawalAddress string
	RateID            string
	OrderFlow         string
}

// CreateDepositAccountData contains the deposit account information
type CreateDepositAccountData struct {
	DepositAddress    string
	BuyAmount         float64
	SellAmount        float64
	USDChain          string
	WithdrawalAddress string
	ProviderID        string
}
