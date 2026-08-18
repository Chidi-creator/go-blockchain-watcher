package changenow

import (
	"time"

	"bitbucket.org/zapspace/zap-go-server/models"
)

// ChangeNowCurrency represents a currency supported by ChangeNow
type ChangeNowCurrency struct {
	Ticker          string   `json:"ticker"`
	Name            string   `json:"name"`
	Image           string   `json:"image"`
	HasExternalID   bool     `json:"has_external_id"`
	IsFiat          bool     `json:"is_fiat"`
	Featured        bool     `json:"featured"`
	IsStable        bool     `json:"is_stable"`
	SupportsFixRate bool     `json:"supports_fixed_rate"`
	Network         string   `json:"network"`
	TokenContract   string   `json:"token_contract"`
	AvailableForBuy bool     `json:"available_for_buy"`
	BuyNetworks     []string `json:"buy_networks"`
}

// ChangeNowRangeResponse is the response from the range endpoint
type ChangeNowRangeResponse struct {
	MinAmount float64 `json:"minAmount"`
	MaxAmount float64 `json:"maxAmount"`
}

// ChangeNowRateResponse is the response from the rate endpoint
type ChangeNowRateResponse struct {
	FromAmount    float64   `json:"fromAmount"`
	ToAmount      float64   `json:"toAmount"`
	Flow          string    `json:"flow"`
	Type          string    `json:"type"`
	RateID        string    `json:"rateId"`
	ValidUntil    time.Time `json:"validUntil"`
	Transactions  []string  `json:"transactionsAvailable"`
	DepositFee    float64   `json:"depositFee"`
	WithdrawalFee float64   `json:"withdrawalFee"`
}

// ChangeNowExchangeRequest is the request to create an exchange
type ChangeNowExchangeRequest struct {
	FromCurrency  string  `json:"fromCurrency"`
	ToCurrency    string  `json:"toCurrency"`
	FromNetwork   string  `json:"fromNetwork"`
	ToNetwork     string  `json:"toNetwork"`
	FromAmount    float64 `json:"fromAmount,omitempty"`
	ToAmount      float64 `json:"toAmount,omitempty"`
	Address       string  `json:"address"`
	Flow          string  `json:"flow"`
	Type          string  `json:"type"`
	RateID        string  `json:"rateId,omitempty"`
	RefundAddress string  `json:"refundAddress,omitempty"`
	ExtraID       string  `json:"extraId,omitempty"`
	RefundExtraID string  `json:"refundExtraId,omitempty"`
	UserID        string  `json:"userId,omitempty"`
	Payload       string  `json:"payload,omitempty"`
	ContactEmail  string  `json:"contactEmail,omitempty"`
	Source        string  `json:"source,omitempty"`
}

// ChangeNowExchangeResponse is the response from the exchange endpoint
type ChangeNowExchangeResponse struct {
	ID               string    `json:"id"`
	PayinAddress     string    `json:"payinAddress"`
	PayinExtraID     string    `json:"payinExtraId"`
	PayoutAddress    string    `json:"payoutAddress"`
	PayoutExtraID    string    `json:"payoutExtraId"`
	FromCurrency     string    `json:"fromCurrency"`
	ToCurrency       string    `json:"toCurrency"`
	FromNetwork      string    `json:"fromNetwork"`
	ToNetwork        string    `json:"toNetwork"`
	AmountFrom       float64   `json:"expectedAmountFrom"`
	AmountTo         float64   `json:"expectedAmountTo"`
	PayTill          time.Time `json:"payTill"`
	RateID           string    `json:"rateId"`
	Flow             string    `json:"flow"`
	Type             string    `json:"type"`
	FromAmount       float64   `json:"fromAmount"`
	ToAmount         float64   `json:"toAmount"`
	CreatedAt        time.Time `json:"createdAt"`
	RefundAddress    string    `json:"refundAddress"`
	RefundExtraID    string    `json:"refundExtraId"`
	ProviderFee      float64   `json:"providerFee"`
	ProviderFeeCurr  string    `json:"providerFeeCurrency"`
	NetworkFee       float64   `json:"networkFee"`
	NetworkFeeCurr   string    `json:"networkFeeCurrency"`
	EstimatedConfirm float64   `json:"estimatedFormConfirmTime"`
}

// ChangeNowTransactionStatusResponse is the response from the status endpoint
type ChangeNowTransactionStatusResponse struct {
	ID                string    `json:"id"`
	Status            string    `json:"status"`
	AmountFrom        float64   `json:"amountFrom"`
	AmountTo          float64   `json:"amountTo"`
	ExpectedFrom      float64   `json:"expectedAmountFrom"`
	ExpectedTo        float64   `json:"expectedAmountTo"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
	PayinHash         string    `json:"payinHash"`
	PayoutHash        string    `json:"payoutHash"`
	PayinExplorerURL  string    `json:"payinExplorerLink"`
	PayoutExplorerURL string    `json:"payoutExplorerLink"`
	RefundHash        string    `json:"refundHash"`
	RefundExplorerURL string    `json:"refundExplorerLink"`
	FromCurrency      string    `json:"fromCurrency"`
	ToCurrency        string    `json:"toCurrency"`
	FromNetwork       string    `json:"fromNetwork"`
	ToNetwork         string    `json:"toNetwork"`
}

// ChangeNowJobData is the data passed to the watcher job
type ChangeNowJobData struct {
	SupportedCurrency *models.SupportedCurrency `json:"supportedCurrency"`
	Address           string                    `json:"address"`
	Order             map[string]interface{}    `json:"order"`
	ProviderID        string                    `json:"providerId"`
}

// ChangeNowMarketPair represents a currency-chain pair
type ChangeNowMarketPair struct {
	Currency string
	Chain    string
}

// ChangeNowMarket represents a market with from/to currency-chain pairs
type ChangeNowMarket struct {
	From         ChangeNowMarketPair
	To           ChangeNowMarketPair
	BuyCurrency  *models.Currency
	SellCurrency *models.Currency
	BuyChain     *models.Chain
	SellChain    *models.Chain
}

// GetOrderRatesProviderParams defines parameters for getting order rates
type GetOrderRatesProviderParams struct {
	SupportedCurrency *models.SupportedCurrency
	Amount            float64
	IsReversed        bool
	OrderFlow         string
}

// CreateDepositAccountParams defines parameters for creating a deposit account
type CreateDepositAccountParams struct {
	SupportedCurrency *models.SupportedCurrency
	Amount            float64
	IsReversed        bool
	OrderFlow         string
	WithdrawalAddress string
	RefundAddress     string
	RateID            string
}

// MinMaxAmount represents minimum and maximum amount constraints
type MinMaxAmount struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

// OrderRateProviderData represents the data returned from a rate query
type OrderRateProviderData struct {
	Rate         float64      `json:"rate"`
	MinMax       MinMaxAmount `json:"minMax"`
	ProviderFees float64      `json:"providerFees"`
	ChainFees    float64      `json:"chainFees"`
	Expiry       time.Time    `json:"expiry"`
	RateID       string       `json:"rateId"`
	USDChain     string       `json:"usdChain"`
}

// CreateDepositAccountData represents the data returned when creating a deposit account
type CreateDepositAccountData struct {
	DepositAddress    string  `json:"depositAddress"`
	BuyAmount         float64 `json:"buyAmount"`
	SellAmount        float64 `json:"sellAmount"`
	WithdrawalAddress string  `json:"withdrawalAddress"`
	ProviderID        string  `json:"providerId"`
	USDChain          string  `json:"usdChain"`
}
