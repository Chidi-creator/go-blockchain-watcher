package changenow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/managers/queue"
	"bitbucket.org/zapspace/zap-go-server/models"
)

// Using the ProviderName defined in constants.go

const (
	// Environment variable keys
	EnvChangeNowAPIKey = "CHANGE_NOW_API_KEY"
	EnvChangeNowAPIURL = "CHANGE_NOW_API_URL"

	// Default values
	DefaultOrderMinAmount  = 10.0
	DefaultOrderMaxAmount  = 5000.0
	DefaultChangeNowAPIURL = "https://api.changenow.io"

	// API endpoints
	ExchangeEstimatedAmountEndpoint = "/exchange/estimated-amount"
	ExchangeRangeEndpoint           = "/exchange/range"
	ExchangeEndpoint                = "/exchange"
	ExchangeCurrenciesEndpoint      = "/exchange/currencies"
	ExchangeStatusEndpoint          = "/v2/exchange/by-id"
)

// Provider implements the ChangeNow provider
type Provider struct {
	log             logger.Logger
	httpClient      *http.Client
	baseURL         string
	apiKey          string
	orderMinAmount  float64
	orderMaxAmount  float64
	availCurrencies []ChangeNowCurrency
	currenciesMu    sync.RWMutex
	queueManager    *queue.QueueManager
	cacheManager    *cache.CacheManager
	store           interface{} // Generic interface instead of domain.OrderStore
}

// NewProvider creates a new ChangeNow provider
func NewProvider(
	log logger.Logger,
	cfg config.ChangeNowConfig,
	queueManager *queue.QueueManager,
	cacheManager *cache.CacheManager,
	store interface{}, // Generic interface instead of domain.OrderStore
) (*Provider, error) {
	// Get API key from environment or config
	apiKey := os.Getenv(EnvChangeNowAPIKey)
	if apiKey == "" {
		apiKey = cfg.APIKey
		if apiKey == "" {
			log.Warn("No ChangeNow API key provided, API requests may be rate-limited")
		}
	}

	// Get API URL from environment or config, or use default
	baseURL := os.Getenv(EnvChangeNowAPIURL)
	if baseURL == "" {
		baseURL = cfg.APIURL
		if baseURL == "" {
			baseURL = DefaultChangeNowAPIURL
			log.Info("Using default ChangeNow API URL", "url", baseURL)
		}
	}

	// Get order min/max amounts from config
	orderMinAmount := DefaultOrderMinAmount
	if cfg.OrderMinAmount > 0 {
		orderMinAmount = cfg.OrderMinAmount
	}

	orderMaxAmount := DefaultOrderMaxAmount
	if cfg.OrderMaxAmount > 0 {
		orderMaxAmount = cfg.OrderMaxAmount
	}

	// Create HTTP client with timeout
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 50,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			DisableKeepAlives:   false,
			ForceAttemptHTTP2:   true,
		},
	}

	return &Provider{
		log:             log,
		httpClient:      httpClient,
		baseURL:         baseURL,
		apiKey:          apiKey,
		orderMinAmount:  orderMinAmount,
		orderMaxAmount:  orderMaxAmount,
		availCurrencies: []ChangeNowCurrency{},
		queueManager:    queueManager,
		cacheManager:    cacheManager,
		store:           store,
	}, nil
}

// GetProviderName returns the name of this provider
func (p *Provider) GetProviderName() string {
	// Use the value directly to avoid possible redeclaration issues
	return "changenow"
}

// GetBalance retrieves the balance of a specific address (not supported by ChangeNow)
func (p *Provider) GetBalance(ctx context.Context, address string, currency *models.Currency) (float64, error) {
	return 0, fmt.Errorf("ChangeNow doesn't support direct address balance retrieval")
}

// ValidateAddress checks if an address is valid (basic checks only)
func (p *Provider) ValidateAddress(address string) bool {
	// Basic check - more comprehensive validation would require chain-specific logic
	return len(address) > 10 && !strings.Contains(address, " ")
}

// GetTokenDetails returns details about a token (not directly supported)
func (p *Provider) GetTokenDetails(ctx context.Context, tokenAddress string) (*models.TokenDetails, error) {
	// This is not directly supported by ChangeNow
	return nil, fmt.Errorf("token details retrieval not supported by ChangeNow")
}

// GetOrderRates retrieves exchange rates from ChangeNow
func (p *Provider) GetOrderRates(params GetOrderRatesProviderParams) (*OrderRateProviderData, error) {
	ctx := context.Background()
	var err error

	// Get amount and if the order is reversed
	amount := params.Amount
	isReversed := params.IsReversed
	orderType := "direct"
	if isReversed {
		orderType = "reverse"
	}

	// Load available currencies if needed
	if err = p.loadAvailableCurrencies(ctx); err != nil {
		return nil, fmt.Errorf("failed to load available currencies: %w", err)
	}

	// Extract market info from currency
	market, err := p.extractMarketFromCurrency(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to extract market: %w", err)
	}

	// Check if buying USDT (for fee calculation)
	buyingUsdt := strings.ToLower(market.BuyCurrency.Symbol) == "usdt"

	// Get exchange range (min/max amounts)
	rangeEndpoint := fmt.Sprintf("%s%s?fromCurrency=%s&toCurrency=%s&fromNetwork=%s&toNetwork=%s&flow=fixed-rate",
		p.baseURL, ExchangeRangeEndpoint,
		market.From.Currency, market.To.Currency,
		market.From.Chain, market.To.Chain,
	)

	headers := p.getHeaders()

	// Make the API call
	rangeReq, err := http.NewRequestWithContext(ctx, http.MethodGet, rangeEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create range request: %w", err)
	}

	for key, value := range headers {
		rangeReq.Header.Set(key, value)
	}

	rangeResp, err := p.httpClient.Do(rangeReq)
	if err != nil {
		return nil, fmt.Errorf("failed to execute range request: %w", err)
	}
	defer rangeResp.Body.Close()

	if rangeResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(rangeResp.Body)
		return nil, fmt.Errorf("unexpected status code from range endpoint: %d, response: %s",
			rangeResp.StatusCode, string(bodyBytes))
	}

	// Parse range response
	var rangeData ChangeNowRangeResponse
	if err = json.NewDecoder(rangeResp.Body).Decode(&rangeData); err != nil {
		return nil, fmt.Errorf("failed to parse range response: %w", err)
	}

	// Get min and max amounts
	minAmount := rangeData.MinAmount
	maxAmount := rangeData.MaxAmount

	// Apply Zap min/max in USDT/USD
	zapMinAmount := p.orderMinAmount
	zapMaxAmount := p.orderMaxAmount

	if buyingUsdt {
		minAmount = max(zapMinAmount, minAmount)
		maxAmount = min(zapMaxAmount, maxAmount)
	}

	// Calculate actual amount to use based on min/max
	var usedAmount float64
	if !isReversed {
		usedAmount = min(max(amount, minAmount*1.01), maxAmount*0.99)
	} else {
		// For reversed orders, need to calculate equivalent min/max
		rateEndpoint := fmt.Sprintf("%s%s?fromCurrency=%s&toCurrency=%s&fromNetwork=%s&toNetwork=%s&flow=fixed-rate&type=direct&fromAmount=%f",
			p.baseURL, ExchangeEstimatedAmountEndpoint,
			market.From.Currency, market.To.Currency,
			market.From.Chain, market.To.Chain,
			minAmount,
		)

		minRateReq, err := http.NewRequestWithContext(ctx, http.MethodGet, rateEndpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create min rate request: %w", err)
		}

		for key, value := range headers {
			minRateReq.Header.Set(key, value)
		}

		minRateResp, err := p.httpClient.Do(minRateReq)
		if err != nil {
			return nil, fmt.Errorf("failed to execute min rate request: %w", err)
		}
		defer minRateResp.Body.Close()

		if minRateResp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(minRateResp.Body)
			return nil, fmt.Errorf("unexpected status code from min rate endpoint: %d, response: %s",
				minRateResp.StatusCode, string(bodyBytes))
		}

		var minRateData ChangeNowRateResponse
		if err = json.NewDecoder(minRateResp.Body).Decode(&minRateData); err != nil {
			return nil, fmt.Errorf("failed to parse min rate response: %w", err)
		}

		// Calculate min rate and sell min/max amounts
		minRate := (minRateData.ToAmount + minRateData.WithdrawalFee) / (minRateData.FromAmount - minRateData.DepositFee)
		sellMinAmount := minRateData.ToAmount
		sellMaxAmount := maxAmount * minRate

		usedAmount = min(max(amount, sellMinAmount*1.01), sellMaxAmount*0.99)
	}

	// Calculate estimated amount
	rateEndpoint := fmt.Sprintf("%s%s?fromCurrency=%s&toCurrency=%s&fromNetwork=%s&toNetwork=%s&flow=fixed-rate&type=%s",
		p.baseURL, ExchangeEstimatedAmountEndpoint,
		market.From.Currency, market.To.Currency,
		market.From.Chain, market.To.Chain,
		orderType,
	)

	// Add amount parameter based on order type
	if !isReversed {
		rateEndpoint += fmt.Sprintf("&fromAmount=%f", usedAmount)
	} else {
		rateEndpoint += fmt.Sprintf("&toAmount=%f", usedAmount)
	}

	// Make the API call
	rateReq, err := http.NewRequestWithContext(ctx, http.MethodGet, rateEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create rate request: %w", err)
	}

	for key, value := range headers {
		rateReq.Header.Set(key, value)
	}

	rateResp, err := p.httpClient.Do(rateReq)
	if err != nil {
		return nil, fmt.Errorf("failed to execute rate request: %w", err)
	}
	defer rateResp.Body.Close()

	if rateResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(rateResp.Body)
		return nil, fmt.Errorf("unexpected status code from rate endpoint: %d, response: %s",
			rateResp.StatusCode, string(bodyBytes))
	}

	// Parse rate response
	var rateData ChangeNowRateResponse
	if err = json.NewDecoder(rateResp.Body).Decode(&rateData); err != nil {
		return nil, fmt.Errorf("failed to parse rate response: %w", err)
	}

	// Calculate provider rate and fees
	providerRate := (rateData.ToAmount + rateData.WithdrawalFee) / (rateData.FromAmount - rateData.DepositFee)

	var providerFees float64
	if buyingUsdt {
		providerFees = rateData.DepositFee + (rateData.WithdrawalFee / providerRate)
	} else {
		providerFees = (rateData.DepositFee * providerRate) + rateData.WithdrawalFee
	}

	// Adjust min/max for zapMinAmount and zapMaxAmount
	if !buyingUsdt {
		zapMinAmount = zapMinAmount / providerRate
		zapMaxAmount = zapMaxAmount / providerRate
	}

	minAmount = max(minAmount, zapMinAmount)
	maxAmount = min(maxAmount, zapMaxAmount)

	// Build response
	result := &OrderRateProviderData{
		Rate:         providerRate,
		MinMax:       MinMaxAmount{Min: minAmount, Max: maxAmount},
		ProviderFees: providerFees,
		ChainFees:    1, // Default chain fee
		Expiry:       rateData.ValidUntil,
		RateID:       rateData.RateID,
		USDChain:     "",
	}

	// Set USDChain
	if buyingUsdt {
		if market.BuyChain != nil && market.BuyChain.Symbol != "" {
			result.USDChain = market.BuyChain.Symbol
		}
	} else {
		if market.SellChain != nil && market.SellChain.Symbol != "" {
			result.USDChain = market.SellChain.Symbol
		}
	}

	return result, nil
}

// CreateDepositAccount creates a deposit account for an exchange
func (p *Provider) CreateDepositAccount(params CreateDepositAccountParams) (*CreateDepositAccountData, error) {
	ctx := context.Background()

	// Get parameters
	amount := params.Amount
	isReversed := params.IsReversed
	refundAddress := params.RefundAddress
	withdrawalAddress := params.WithdrawalAddress
	rateID := params.RateID

	// Validate withdrawal address
	if withdrawalAddress == "" {
		return nil, fmt.Errorf("withdrawalAddress is required for creating a fixed-rate exchange")
	}

	// Load available currencies
	if err := p.loadAvailableCurrencies(ctx); err != nil {
		return nil, fmt.Errorf("failed to load available currencies: %w", err)
	}

	// Extract market info
	market, err := p.extractMarketFromCurrency(ctx, GetOrderRatesProviderParams{
		SupportedCurrency: params.SupportedCurrency,
		Amount:            amount,
		IsReversed:        isReversed,
		OrderFlow:         params.OrderFlow,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to extract market: %w", err)
	}

	// Check if buying USDT
	buyingUsdt := strings.ToLower(market.BuyCurrency.Symbol) == "usdt"

	// Create exchange request
	endpoint := fmt.Sprintf("%s%s", p.baseURL, ExchangeEndpoint)

	// Create request body
	body := ChangeNowExchangeRequest{
		FromCurrency:  market.From.Currency,
		ToCurrency:    market.To.Currency,
		FromNetwork:   market.From.Chain,
		ToNetwork:     market.To.Chain,
		Address:       withdrawalAddress,
		Flow:          "fixed-rate",
		Type:          "direct",
		ExtraID:       "",
		RefundExtraID: "",
		UserID:        "",
		Payload:       "",
		ContactEmail:  "",
		Source:        "",
	}

	// Add rate ID if provided, otherwise get one
	if rateID != "" {
		body.RateID = rateID
	} else {
		var err error
		body.RateID, err = p.getRateID(ctx, market.From.Currency, market.To.Currency,
			market.From.Chain, market.To.Chain, amount, isReversed)
		if err != nil {
			return nil, fmt.Errorf("failed to get rate ID: %w", err)
		}
	}

	// Add amount based on order type
	if isReversed {
		body.ToAmount = amount
	} else {
		body.FromAmount = amount
	}

	// Add refund address if provided
	if refundAddress != "" {
		body.RefundAddress = refundAddress
	}

	// Convert request to JSON
	reqBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Make the API call
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create exchange request: %w", err)
	}

	headers := p.getHeaders()
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute exchange request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code from exchange endpoint: %d, response: %s",
			resp.StatusCode, string(bodyBytes))
	}

	// Parse response
	var response ChangeNowExchangeResponse
	if err = json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to parse exchange response: %w", err)
	}

	// Build result
	result := &CreateDepositAccountData{
		DepositAddress:    response.PayinAddress,
		BuyAmount:         response.FromAmount,
		SellAmount:        response.ToAmount,
		WithdrawalAddress: withdrawalAddress,
		ProviderID:        response.ID,
	}

	// Set USDChain
	if buyingUsdt && market.BuyChain != nil && market.BuyChain.Symbol != "" {
		result.USDChain = market.BuyChain.Symbol
	} else if market.SellChain != nil && market.SellChain.Symbol != "" {
		result.USDChain = market.SellChain.Symbol
	}

	return result, nil
}

// WatchTransactionStatus schedules a job to periodically check the status of a transaction
func (p *Provider) WatchTransactionStatus(
	supportedCurrency *models.SupportedCurrency,
	address string,
	order map[string]interface{},
	providerId string,
) error {
	// Create job data
	jobData := map[string]interface{}{
		"supportedCurrency": supportedCurrency,
		"address":           address,
		"order":             order,
		"providerId":        providerId,
	}

	// Schedule job with queue manager
	orderId := ""
	if order != nil {
		if id, ok := order["_id"].(string); ok {
			orderId = id
		}
	}

	// Create job options for QueueManager
	options := map[string]interface{}{
		"jobId": fmt.Sprintf("changenow-tx:%s:%s", providerId, orderId),
		"repeat": map[string]interface{}{
			"every": int(90 * time.Second / time.Millisecond), // Convert to milliseconds for Bull compatibility
		},
		"removeOnComplete": true,
		"removeOnFail":     false,
	}

	// Schedule the job
	if p.queueManager != nil {
		ctx := context.Background()
		jobID, err := p.queueManager.ScheduleJob(ctx, constants.QueueChangeNowWatcher, jobData, options)
		if err != nil {
			return fmt.Errorf("failed to schedule transaction watch job: %w", err)
		}

		p.log.Info("Scheduled watch for transaction",
			"providerId", providerId,
			"orderId", orderId,
			"jobID", jobID)
		return nil
	}

	return fmt.Errorf("queue manager not initialized")
}

// GetTransactionStatus retrieves the current status of a transaction
func (p *Provider) GetTransactionStatus(ctx context.Context, txID string) (*ChangeNowTransactionStatusResponse, error) {
	url := fmt.Sprintf("%s%s?id=%s", p.baseURL, ExchangeStatusEndpoint, txID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create status request: %w", err)
	}

	headers := p.getHeaders()
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute status request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code from status endpoint: %d, response: %s",
			resp.StatusCode, string(bodyBytes))
	}

	var status ChangeNowTransactionStatusResponse
	if err = json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to parse status response: %w", err)
	}

	return &status, nil
}

// Helper methods

// loadAvailableCurrencies loads the list of available currencies from ChangeNow
func (p *Provider) loadAvailableCurrencies(ctx context.Context) error {
	p.currenciesMu.RLock()
	if len(p.availCurrencies) > 0 {
		p.currenciesMu.RUnlock()
		return nil
	}
	p.currenciesMu.RUnlock()

	// Need to load currencies
	p.currenciesMu.Lock()
	defer p.currenciesMu.Unlock()

	// Check again in case another goroutine loaded currencies while we were waiting
	if len(p.availCurrencies) > 0 {
		return nil
	}

	// Endpoint for currencies
	endpoint := fmt.Sprintf("%s%s?active=&flow=standard&buy=&sell=", p.baseURL, ExchangeCurrenciesEndpoint)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create currencies request: %w", err)
	}

	headers := p.getHeaders()
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute currencies request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code from currencies endpoint: %d, response: %s",
			resp.StatusCode, string(bodyBytes))
	}

	// Parse response
	var currencies []ChangeNowCurrency
	if err = json.NewDecoder(resp.Body).Decode(&currencies); err != nil {
		return fmt.Errorf("failed to parse currencies response: %w", err)
	}

	p.availCurrencies = currencies
	p.log.Info("Loaded ChangeNow currencies", "count", len(currencies))

	return nil
}

// extractMarketFromCurrency extracts the necessary market information from the supported currency
func (p *Provider) extractMarketFromCurrency(ctx context.Context, params GetOrderRatesProviderParams) (*ChangeNowMarket, error) {
	supportedCurrency := params.SupportedCurrency
	orderFlow := params.OrderFlow

	// Default values for USDT on BSC
	buyCurrency := &models.Currency{Symbol: "usdt"}
	buyChain := &models.Chain{Symbol: "bsc"}
	sellCurrency := &models.Currency{Symbol: "usdt"}
	sellChain := &models.Chain{Symbol: "bsc"}

	// Extract chain and currency information
	if supportedCurrency != nil {
		// Fetch chain and currency objects using their IDs

		// For chain - assuming we check for zero value of ObjectID or use another method
		// In a real implementation, check if the ID is valid and lookup the chain
		chain := &models.Chain{Symbol: "bsc"} // Default chain

		if orderFlow == constants.ORDER_FLOW_BUY {
			buyChain = chain
		} else {
			sellChain = chain
		}

		// For currency - assuming we check for zero value of ObjectID or use another method
		// In a real implementation, check if the ID is valid and lookup the currency
		currency := &models.Currency{Symbol: "usdt"} // Default currency

		if orderFlow == constants.ORDER_FLOW_BUY {
			buyCurrency = currency
		} else {
			sellCurrency = currency
		}
	}

	// Create market pairs
	from := ChangeNowMarketPair{
		Currency: strings.ToLower(buyCurrency.Symbol),
		Chain:    strings.ToLower(buyChain.Symbol),
	}

	to := ChangeNowMarketPair{
		Currency: strings.ToLower(sellCurrency.Symbol),
		Chain:    strings.ToLower(sellChain.Symbol),
	}

	// Handle special cases for chain names
	if from.Chain == "arb" {
		from.Chain = "arbitrum"
	}

	if to.Chain == "arb" {
		to.Chain = "arbitrum"
	}

	if from.Chain == "pls" {
		from.Chain = "eth"
	}

	if to.Chain == "pls" {
		to.Chain = "eth"
	}

	if from.Currency == "pls" {
		from.Currency = "eth"
	}

	if to.Currency == "pls" {
		to.Currency = "eth"
	}

	// Create the market
	market := &ChangeNowMarket{
		From:         from,
		To:           to,
		BuyCurrency:  buyCurrency,
		SellCurrency: sellCurrency,
		BuyChain:     buyChain,
		SellChain:    sellChain,
	}

	return market, nil
}

// getRateID gets a rate ID from ChangeNow for a specific exchange
func (p *Provider) getRateID(
	ctx context.Context,
	fromCurrency, toCurrency string,
	fromNetwork, toNetwork string,
	amount float64,
	isReversed bool,
) (string, error) {
	// Build URL
	endpoint := fmt.Sprintf("%s%s", p.baseURL, ExchangeEstimatedAmountEndpoint)

	// Create query params
	params := map[string]string{
		"fromCurrency": fromCurrency,
		"toCurrency":   toCurrency,
		"fromNetwork":  fromNetwork,
		"toNetwork":    toNetwork,
		"flow":         "fixed-rate",
		"useRateId":    "true",
	}

	// Add amount based on direction
	if isReversed {
		params["type"] = "reverse"
		params["toAmount"] = fmt.Sprintf("%f", amount)
	} else {
		params["type"] = "direct"
		params["fromAmount"] = fmt.Sprintf("%f", amount)
	}

	// Build query string
	query := ""
	for key, value := range params {
		if query != "" {
			query += "&"
		}
		query += fmt.Sprintf("%s=%s", key, value)
	}

	// Make request
	url := fmt.Sprintf("%s?%s", endpoint, query)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create rate ID request: %w", err)
	}

	headers := p.getHeaders()
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to execute rate ID request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("unexpected status code from rate ID endpoint: %d, response: %s",
			resp.StatusCode, string(bodyBytes))
	}

	// Parse response
	var rateData ChangeNowRateResponse
	if err = json.NewDecoder(resp.Body).Decode(&rateData); err != nil {
		return "", fmt.Errorf("failed to parse rate ID response: %w", err)
	}

	if rateData.RateID == "" {
		return "", fmt.Errorf("no rate ID returned from ChangeNow")
	}

	return rateData.RateID, nil
}

// getHeaders returns the standard headers for ChangeNow API requests
func (p *Provider) getHeaders() map[string]string {
	headers := map[string]string{
		"Content-Type": "application/json",
	}

	if p.apiKey != "" {
		headers["x-changenow-api-key"] = p.apiKey
		headers["x-api-key"] = p.apiKey
	}

	return headers
}

// Helper functions for min/max
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
