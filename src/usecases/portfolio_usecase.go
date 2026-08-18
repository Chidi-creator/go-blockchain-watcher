package usecases

import (
	"context"
	"errors"
	"sync"
	"time"

	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/models"
	"bitbucket.org/zapspace/zap-go-server/src/repositories"
	"bitbucket.org/zapspace/zap-go-server/src/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Common errors
var (
	ErrAccountNotFound   = errors.New("account not found")
	ErrCurrencyNotFound  = errors.New("currency not found")
	ErrChainNotFound     = errors.New("chain not found")
	ErrNoWalletAddress   = errors.New("account has no wallet address")
	ErrContextDeadline   = errors.New("context deadline exceeded")
	ErrPortfolioNotFound = errors.New("portfolio not found")
)

// BlockchainService interface for blockchain operations
type BlockchainService interface {
	GetBalance(ctx context.Context, chainSymbol string, address string, currency *models.Currency, supportedCurrency *models.SupportedCurrency) (float64, error)
	ValidateAddress(ctx context.Context, chainSymbol string, address string) (bool, error)
	GetTokenDetails(ctx context.Context, chainSymbol string, tokenAddress string) (*models.TokenDetails, error)
}

// cachedPortfolio holds a cached portfolio with timestamp
type cachedPortfolio struct {
	portfolio        *types.WalletPortfolio
	accountPortfolio *types.AccountPortfolio
	timestamp        time.Time
}

// PortfolioUseCase handles portfolio related business logic
type PortfolioUseCase struct {
	accountRepo    repositories.AccountRepository
	currencyRepo   repositories.CurrencyRepository
	chainRepo      repositories.ChainRepository
	blockchainSvc  BlockchainService
	logger         logger.Logger
	cacheTTL       time.Duration
	portfolioCache *sync.Map
}

// NewPortfolioUseCase creates a new portfolio use case
func NewPortfolioUseCase(
	accountRepo repositories.AccountRepository,
	currencyRepo repositories.CurrencyRepository,
	chainRepo repositories.ChainRepository,
	blockchainSvc BlockchainService,
	logger logger.Logger,
) *PortfolioUseCase {
	return &PortfolioUseCase{
		accountRepo:    accountRepo,
		currencyRepo:   currencyRepo,
		chainRepo:      chainRepo,
		blockchainSvc:  blockchainSvc,
		logger:         logger,
		cacheTTL:       5 * time.Minute,
		portfolioCache: &sync.Map{},
	}
}

// GetWalletPortfolio fetches the portfolio for a specific wallet
func (uc *PortfolioUseCase) GetWalletPortfolio(ctx context.Context, walletID primitive.ObjectID) (*types.WalletPortfolio, error) {
	// Check cache first
	cacheKey := "wallet-portfolio:" + walletID.Hex()
	if cached, ok := uc.portfolioCache.Load(cacheKey); ok {
		cachedPortfolio := cached.(cachedPortfolio)
		if time.Since(cachedPortfolio.timestamp) < uc.cacheTTL {
			return cachedPortfolio.portfolio, nil
		}
	}

	uc.logger.Info("Fetching wallet portfolio", "walletId", walletID.Hex())

	// Fetch all accounts for this wallet
	accounts, err := uc.accountRepo.GetByWalletID(ctx, walletID)
	if err != nil {
		return nil, err
	}

	if len(accounts) == 0 {
		uc.logger.Info("No accounts found for wallet", "walletId", walletID.Hex())
		return &types.WalletPortfolio{
			WalletID:       walletID,
			Accounts:       []types.AccountPortfolio{},
			WalletUsdValue: 0,
		}, nil
	}

	uc.logger.Info("Found accounts for wallet", "walletId", walletID.Hex(), "count", len(accounts))

	// Process accounts in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex

	accountPortfolios := make([]types.AccountPortfolio, 0, len(accounts))

	for _, account := range accounts {
		wg.Add(1)
		go func(acc *types.Account) {
			defer wg.Done()

			portfolio, err := uc.GetAccountPortfolio(ctx, acc.ID)
			if err != nil {
				uc.logger.Error("Failed to get account portfolio",
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

	result := types.NewWalletPortfolio(walletID, accountPortfolios)

	// Cache the result
	uc.portfolioCache.Store(cacheKey, cachedPortfolio{
		portfolio: &result,
		timestamp: time.Now(),
	})

	return &result, nil
}

// GetAccountPortfolio calculates the portfolio for a single account
func (uc *PortfolioUseCase) GetAccountPortfolio(ctx context.Context, accountID primitive.ObjectID) (*types.AccountPortfolio, error) {
	// Check cache first
	cacheKey := "account-portfolio:" + accountID.Hex()
	if cached, ok := uc.portfolioCache.Load(cacheKey); ok {
		cachedPortfolio := cached.(cachedPortfolio)
		if time.Since(cachedPortfolio.timestamp) < uc.cacheTTL {
			return cachedPortfolio.accountPortfolio, nil
		}
	}

	// Fetch account data
	account, err := uc.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return nil, err
	}

	if account == nil {
		return nil, ErrAccountNotFound
	}

	// Fetch currency data
	currency, err := uc.currencyRepo.GetByID(ctx, account.CurrencyID)
	if err != nil {
		return nil, err
	}

	if currency == nil {
		return nil, ErrCurrencyNotFound
	}

	// Get chain data if available
	var chainSymbol string
	if !account.ChainID.IsZero() {
		chain, err := uc.chainRepo.GetByID(ctx, account.ChainID)
		if err != nil {
			uc.logger.Warn("Failed to get chain data",
				"chainId", account.ChainID.Hex(),
				"error", err)
		} else if chain != nil {
			chainSymbol = chain.Symbol
		}
	}

	// Get price data (mock for now)
	price, err := uc.getTokenPrice(ctx, currency)
	if err != nil {
		uc.logger.Warn("Failed to get token price, using 0",
			"currency", currency.Symbol,
			"error", err)
		price = 0
	}

	// Create token data
	chainRef := types.NewChainReference(account.ChainID, chainSymbol)
	token := types.NewTokenData(currency.Symbol, account.Balance, price, chainRef)

	result := types.NewAccountPortfolio(account.ID, account.WalletAddress, token)

	// Cache the result
	uc.portfolioCache.Store(cacheKey, cachedPortfolio{
		accountPortfolio: &result,
		timestamp:        time.Now(),
	})

	return &result, nil
}

// GetBalance fetches the current balance for an account from the blockchain
func (uc *PortfolioUseCase) GetBalance(ctx context.Context, accountID primitive.ObjectID) (float64, error) {
	account, err := uc.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return 0, err
	}

	if account == nil {
		return 0, ErrAccountNotFound
	}

	if account.WalletAddress == "" {
		return 0, ErrNoWalletAddress
	}

	if account.ChainID.IsZero() {
		return 0, ErrChainNotFound
	}

	// Get chain data
	chain, err := uc.chainRepo.GetByID(ctx, account.ChainID)
	if err != nil {
		return 0, err
	}

	if chain == nil {
		return 0, ErrChainNotFound
	}

	// Get currency data
	currency, err := uc.currencyRepo.GetByID(ctx, account.CurrencyID)
	if err != nil {
		return 0, err
	}

	if currency == nil {
		return 0, ErrCurrencyNotFound
	}

	// Use blockchain service to get balance
	balance, err := uc.blockchainSvc.GetBalance(ctx, chain.Symbol, account.WalletAddress, currency, nil)
	if err != nil {
		return 0, err
	}

	// Update account with new balance using the UpdateBalance method
	account.UpdateBalance(balance)
	err = uc.accountRepo.Update(ctx, account)
	if err != nil {
		uc.logger.Error("Failed to update account balance", "accountId", accountID.Hex(), "error", err)
		// Return balance even if update fails
	}

	return balance, nil
}

// getTokenPrice gets the current price of a token (mock implementation)
func (uc *PortfolioUseCase) getTokenPrice(ctx context.Context, currency *models.Currency) (float64, error) {
	// Mock prices for common tokens
	mockPrices := map[string]float64{
		"BTC":  40000.0,
		"ETH":  2000.0,
		"SOL":  100.0,
		"TRX":  0.1,
		"USDT": 1.0,
		"USDC": 1.0,
	}

	ctx.Done()

	if price, ok := mockPrices[currency.Symbol]; ok {
		return price, nil
	}

	// Default price if not in the mock list
	return 0.0, nil
}
