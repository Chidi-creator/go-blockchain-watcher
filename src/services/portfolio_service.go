package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/mongodb"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// PortfolioService represents the portfolio service
type PortfolioService struct {
	logger            logger.Logger
	accountRepo       mongodb.AccountRepository
	currencyRepo      mongodb.CurrencyRepository
	chainRepo         mongodb.ChainRepository
	blockchainService *BlockchainService
	cacheTTL          time.Duration
	portfolioCache    *sync.Map
}

// NewPortfolioService creates a new portfolio service instance
func NewPortfolioService(
	logger logger.Logger,
	accountRepo mongodb.AccountRepository,
	currencyRepo mongodb.CurrencyRepository,
	chainRepo mongodb.ChainRepository,
	blockchainService *BlockchainService,
) *PortfolioService {
	return &PortfolioService{
		logger:            logger,
		accountRepo:       accountRepo,
		currencyRepo:      currencyRepo,
		chainRepo:         chainRepo,
		blockchainService: blockchainService,
		cacheTTL:          5 * time.Minute,
		portfolioCache:    &sync.Map{},
	}
}

// GetWalletPortfolio fetches the portfolio for a specific wallet
func (s *PortfolioService) GetWalletPortfolio(ctx context.Context, walletID primitive.ObjectID) (*models.WalletPortfolio, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("wallet-portfolio:%s", walletID.Hex())
	if cached, ok := s.portfolioCache.Load(cacheKey); ok {
		cachedPortfolio := cached.(cachedPortfolio)
		if time.Since(cachedPortfolio.timestamp) < s.cacheTTL {
			return cachedPortfolio.portfolio, nil
		}
	}

	s.logger.Info("Fetching wallet portfolio", "walletId", walletID.Hex())

	// Fetch all accounts for this wallet
	accounts, err := s.accountRepo.GetAccountsByWalletID(ctx, walletID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch wallet accounts: %w", err)
	}

	if len(accounts) == 0 {
		s.logger.Info("No accounts found for wallet", "walletId", walletID.Hex())
		return &models.WalletPortfolio{
			WalletID:       walletID,
			Accounts:       []models.AccountPortfolio{},
			WalletUsdValue: 0,
		}, nil
	}

	s.logger.Info("Found accounts for wallet", "walletId", walletID.Hex(), "count", len(accounts))

	// Process accounts in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex

	accountPortfolios := make([]models.AccountPortfolio, 0, len(accounts))

	for _, account := range accounts {
		wg.Add(1)
		go func(acc models.Account) {
			defer wg.Done()

			portfolio, err := s.GetAccountPortfolio(ctx, acc.ID)
			if err != nil {
				s.logger.Error("Failed to get account portfolio",
					"accountId", acc.ID.Hex(),
					"walletId", walletID.Hex(),
					"error", err)
				return
			}

			if portfolio != nil {
				mu.Lock()
				accountPortfolios = append(accountPortfolios, *portfolio)
				mu.Unlock()
			}
		}(account)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Calculate total USD value
	var totalUsdValue float64
	for _, portfolio := range accountPortfolios {
		totalUsdValue += portfolio.TotalUsdValue
	}

	result := &models.WalletPortfolio{
		WalletID:       walletID,
		Accounts:       accountPortfolios,
		WalletUsdValue: totalUsdValue,
	}

	// Cache the result
	s.portfolioCache.Store(cacheKey, cachedPortfolio{
		portfolio: result,
		timestamp: time.Now(),
	})

	return result, nil
}

// GetAccountPortfolio calculates the portfolio for a single account
func (s *PortfolioService) GetAccountPortfolio(ctx context.Context, accountID primitive.ObjectID) (*models.AccountPortfolio, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("account-portfolio:%s", accountID.Hex())
	if cached, ok := s.portfolioCache.Load(cacheKey); ok {
		cachedPortfolio := cached.(cachedPortfolio)
		if time.Since(cachedPortfolio.timestamp) < s.cacheTTL {
			return cachedPortfolio.accountPortfolio, nil
		}
	}

	// Fetch account data
	account, err := s.accountRepo.GetAccountByID(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch account: %w", err)
	}

	if account == nil {
		return nil, fmt.Errorf("account not found: %s", accountID.Hex())
	}

	// Validate required fields
	if account.CurrencyID.IsZero() && account.SupportedCurrencyID.IsZero() {
		s.logger.Error("Account has no currency or supported currency ID",
			"accountId", accountID.Hex())
		return nil, fmt.Errorf("account has no currency ID")
	}

	// Use currencyID or supportedCurrencyID
	var currencyID primitive.ObjectID
	if !account.CurrencyID.IsZero() {
		currencyID = account.CurrencyID
	} else {
		currencyID = account.SupportedCurrencyID
	}

	// Fetch currency data
	currency, err := s.currencyRepo.GetCurrencyByID(ctx, currencyID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch currency: %w", err)
	}

	if currency == nil {
		return nil, fmt.Errorf("currency not found: %s", currencyID.Hex())
	}

	// Get price data
	price, err := s.GetTokenPrice(ctx, currency)
	if err != nil {
		s.logger.Warn("Failed to get token price, using 0",
			"currency", currency.Symbol,
			"error", err)
		price = 0
	}

	// Get chain data if available
	var chainSymbol string
	if !account.ChainID.IsZero() {
		chain, err := s.chainRepo.GetChainByID(ctx, account.ChainID)
		if err != nil {
			s.logger.Warn("Failed to get chain data",
				"chainId", account.ChainID.Hex(),
				"error", err)
		} else if chain != nil {
			chainSymbol = chain.Symbol
		}
	}

	// Calculate USD value
	balance := account.Balance
	usdValue := balance * price

	// Create token data
	token := models.TokenData{
		Symbol:   currency.Symbol,
		Balance:  balance,
		Price:    price,
		UsdValue: usdValue,
		ChainID: models.ChainReference{
			ID:     account.ChainID,
			Symbol: chainSymbol,
		},
	}

	result := &models.AccountPortfolio{
		AccountID:     account.ID,
		WalletAddress: account.WalletAddress,
		Token:         token,
		TotalUsdValue: usdValue,
	}

	// Cache the result
	s.portfolioCache.Store(cacheKey, cachedPortfolio{
		accountPortfolio: result,
		timestamp:        time.Now(),
	})

	return result, nil
}

// GetPortfolioBalance fetches the current balance for an account from the blockchain
func (s *PortfolioService) GetPortfolioBalance(ctx context.Context, account *models.Account) (float64, error) {
	if account == nil {
		return 0, fmt.Errorf("account is nil")
	}

	if account.WalletAddress == "" {
		return 0, fmt.Errorf("account has no wallet address")
	}

	if account.ChainID.IsZero() {
		return 0, fmt.Errorf("account has no chain ID")
	}

	// Get chain data
	chain, err := s.chainRepo.GetChainByID(ctx, account.ChainID)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch chain: %w", err)
	}

	if chain == nil {
		return 0, fmt.Errorf("chain not found: %s", account.ChainID.Hex())
	}

	// Get currency data
	var currencyID primitive.ObjectID
	if !account.CurrencyID.IsZero() {
		currencyID = account.CurrencyID
	} else if !account.SupportedCurrencyID.IsZero() {
		currencyID = account.SupportedCurrencyID
	} else {
		return 0, fmt.Errorf("account has no currency ID")
	}

	currency, err := s.currencyRepo.GetCurrencyByID(ctx, currencyID)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch currency: %w", err)
	}

	if currency == nil {
		return 0, fmt.Errorf("currency not found: %s", currencyID.Hex())
	}

	// Use blockchain service to get balance
	return s.blockchainService.FetchAccountBalance(ctx, account.WalletAddress, currency, chain, nil)
}

// GetBalance fetches the current balance for an account from the blockchain (for backward compatibility)
func (s *PortfolioService) GetBalance(ctx context.Context, account *models.Account) (float64, error) {
	return s.GetPortfolioBalance(ctx, account)
}

// GetTokenPrice gets the current price of a token
func (s *PortfolioService) GetTokenPrice(ctx context.Context, currency *models.Currency) (float64, error) {
	// Implementation will depend on the rate engine you choose
	// For now, we'll use a simple mock
	// In production, you would integrate with your existing rate engine

	// TODO: Implement real integration with rate engine
	// Mock prices for common tokens
	mockPrices := map[string]float64{
		"BTC":  40000.0,
		"ETH":  2000.0,
		"SOL":  100.0,
		"TRX":  0.1,
		"USDT": 1.0,
		"USDC": 1.0,
	}

	if price, ok := mockPrices[currency.Symbol]; ok {
		return price, nil
	}

	// Default price if not in the mock list
	return 0.0, nil
}

// cachedPortfolio holds a cached portfolio with timestamp
type cachedPortfolio struct {
	portfolio        *models.WalletPortfolio
	accountPortfolio *models.AccountPortfolio
	timestamp        time.Time
}
