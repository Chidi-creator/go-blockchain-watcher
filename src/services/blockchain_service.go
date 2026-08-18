package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/models"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain/bitcoin"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// BlockUpdate and TransactionData types have been moved to the bitcoin package

// TransactionParams contains parameters for creating a transaction
type TransactionParams = bitcoin.TransactionParams

// BlockUpdate contains information about a block update
type BlockUpdate = bitcoin.BlockUpdate

// TransactionData contains information about a transaction
type TransactionData = bitcoin.TransactionData

// BlockchainService provides methods for interacting with blockchains
type BlockchainService struct {
	logger          logger.Logger
	providerFactory *blockchain.ProviderFactory
	providers       map[string]bitcoin.ChainProvider
	initMutex       sync.Mutex
	initStatus      map[string]bool
}

// ChainProvider defines the interface for chain-specific implementations
type ChainProvider interface {
	GetBalance(ctx context.Context, address string, currency *models.Currency) (float64, error)
	ValidateAddress(address string) bool
	GetTokenDetails(ctx context.Context, tokenAddress string) (*models.TokenDetails, error)
	WatchAddress(ctx context.Context, address string, onUpdate func(update BlockUpdate))
	CreateTransaction(ctx context.Context, params TransactionParams) (string, error)
}

// NewBlockchainService creates a new blockchain service
func NewBlockchainService(
	logger logger.Logger,
	providerFactory *blockchain.ProviderFactory,
) *BlockchainService {
	return &BlockchainService{
		logger:          logger,
		providerFactory: providerFactory,
		providers:       make(map[string]bitcoin.ChainProvider),
		initStatus:      make(map[string]bool),
	}
}

// GetProvider initializes and returns a chain provider for the given chain symbol
func (s *BlockchainService) GetProvider(ctx context.Context, chainSymbol string) (bitcoin.ChainProvider, error) {
	s.initMutex.Lock()
	defer s.initMutex.Unlock()

	// Check if provider already initialized
	if provider, ok := s.providers[chainSymbol]; ok {
		return provider, nil
	}

	// Check if initialization is in progress
	if s.initStatus[chainSymbol] {
		return nil, fmt.Errorf("chain provider '%s' initialization in progress", chainSymbol)
	}

	// Mark initialization as in progress
	s.initStatus[chainSymbol] = true

	s.logger.Info("Creating mock provider for", "chain", chainSymbol)

	// Create a mock provider for now
	provider := &mockProvider{
		logger:      s.logger,
		chainSymbol: chainSymbol,
	}

	// Store provider
	s.providers[chainSymbol] = provider
	return provider, nil
}

// FetchAccountBalance fetches the current balance for an account using supportedCurrency
func (s *BlockchainService) FetchAccountBalance(
	ctx context.Context,
	walletAddress string,
	currency *models.Currency,
	chain *models.Chain,
	supportedCurrency *models.SupportedCurrency,
) (float64, error) {
	s.logger.Info("Fetching account balance - detailed",
		"address", walletAddress,
		"currency", currency.Symbol,
		"chain", chain.Symbol,
		"chainID", chain.ChainID,
		"chainIDType", fmt.Sprintf("%T", chain.ChainID),
		"chainIDString", chain.GetChainIDString(),
		"networkType", chain.NetworkType)

	// Get the appropriate blockchain provider
	provider, err := s.providerFactory.GetProviderForCurrency(currency, chain)
	if err != nil {
		s.logger.Error("Failed to get blockchain provider",
			"error", err,
			"currency", currency.Symbol,
			"chain", chain.Symbol,
			"chainID", chain.ChainID)
		return 0, fmt.Errorf("failed to get provider: %w", err)
	}

	s.logger.Info("Got blockchain provider successfully",
		"currency", currency.Symbol,
		"chain", chain.Symbol)

	// If supportedCurrency contains a token address, add it to the context
	contextWithValues := ctx
	var tokenAddress string
	if supportedCurrency != nil && supportedCurrency.TokenAddress != "" {
		tokenAddress = supportedCurrency.TokenAddress
		// Create a new context with the token address
		contextWithValues = context.WithValue(ctx, constants.TokenAddressKey, tokenAddress)

		s.logger.Info("Added token address to context",
			"currency", currency.Symbol,
			"tokenAddress", tokenAddress)
	}

	if supportedCurrency != nil && supportedCurrency.Decimals > 0 {
		s.logger.Info("Added decimals to context",
			"currency", currency.Symbol,
			"decimals", supportedCurrency.Decimals)
		contextWithValues = context.WithValue(contextWithValues, constants.DecimalsKey, supportedCurrency.Decimals)
	}

	// Fetch the balance
	s.logger.Info("Calling provider.GetBalance",
		"currency", currency.Symbol,
		"address", walletAddress,
		"hasTokenAddress", tokenAddress != "")

	balance, err := provider.GetBalance(contextWithValues, walletAddress, currency)
	if err != nil {
		s.logger.Error("Failed to fetch balance",
			"error", err,
			"address", walletAddress,
			"currency", currency.Symbol)
		return 0, fmt.Errorf("failed to fetch balance: %w", err)
	}

	s.logger.Info("Retrieved balance successfully",
		"address", walletAddress,
		"currency", currency.Symbol,
		"chain", chain.Symbol,
		"balance", balance)

	return balance, nil
}

// FetchBalanceByIDs fetches balance using IDs instead of full objects
func (s *BlockchainService) FetchBalanceByIDs(
	ctx context.Context,
	walletAddress string,
	currencyID,
	chainID primitive.ObjectID,
	supportedCurrencyID primitive.ObjectID,
	currencyRepo interface{}, // Replace with your actual repository interfaces
	chainRepo interface{},
	supportedCurrencyRepo interface{},
) (float64, error) {
	// This method would require looking up the currency and chain details
	// from their respective repositories before calling FetchAccountBalance
	// Implementation depends on your repository interfaces

	// Example (pseudo-code):
	/*
		currency, err := currencyRepo.GetByID(ctx, currencyID)
		if err != nil {
			return 0, err
		}

		chain, err := chainRepo.GetByID(ctx, chainID)
		if err != nil {
			return 0, err
		}

		var supportedCurrency *models.SupportedCurrency
		if !supportedCurrencyID.IsZero() {
			supportedCurrency, err = supportedCurrencyRepo.GetByID(ctx, supportedCurrencyID)
			if err != nil {
				return 0, err
			}
		}

		return s.FetchAccountBalance(ctx, walletAddress, currency, chain, supportedCurrency)
	*/

	return 0, fmt.Errorf("not implemented")
}

// ValidateAddress for interface compatibility (string-based chain symbol)
func (s *BlockchainService) ValidateAddress(ctx context.Context, address string, chainSymbol string) (bool, error) {
	// Get a provider for the chain
	provider, err := s.providerFactory.GetProviderForSymbols("", chainSymbol)
	if err != nil {
		return false, err
	}

	return provider.ValidateAddress(address), nil
}

// GetTokenDetails retrieves details about a token on a specific chain
func (s *BlockchainService) GetTokenDetails(ctx context.Context, chainSymbol string, tokenAddress string) (*models.TokenDetails, error) {
	provider, err := s.GetProvider(ctx, chainSymbol)
	if err != nil {
		return nil, err
	}

	details, err := provider.GetTokenDetails(ctx, tokenAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get token details for address %s on chain %s: %w", tokenAddress, chainSymbol, err)
	}

	return details, nil
}

// WatchAddress sets up a watch for specific address on a chain
func (s *BlockchainService) WatchAddress(ctx context.Context, chainSymbol string, address string, callback func(update BlockUpdate)) error {
	provider, err := s.GetProvider(ctx, chainSymbol)
	if err != nil {
		return err
	}

	provider.WatchAddress(ctx, address, callback)
	return nil
}

// CreateTransaction creates and optionally broadcasts a transaction
func (s *BlockchainService) CreateTransaction(ctx context.Context, chainSymbol string, params TransactionParams) (string, error) {
	provider, err := s.GetProvider(ctx, chainSymbol)
	if err != nil {
		return "", err
	}

	txHash, err := provider.CreateTransaction(ctx, params)
	if err != nil {
		return "", fmt.Errorf("failed to create transaction on chain %s: %w", chainSymbol, err)
	}

	return txHash, nil
}

// GetBalance retrieves the balance for a wallet address (for interface compatibility)
func (s *BlockchainService) GetBalance(
	ctx context.Context,
	chainSymbol string,
	address string,
	currency *models.Currency,
	supportedCurrency *models.SupportedCurrency,
) (float64, error) {
	// Get the appropriate blockchain provider
	provider, err := s.providerFactory.GetProviderForSymbols(currency.Symbol, chainSymbol)
	if err != nil {
		s.logger.Error("Failed to get blockchain provider", "error", err)
		return 0, fmt.Errorf("failed to get provider: %w", err)
	}

	// Validate address
	if !provider.ValidateAddress(address) {
		s.logger.Error("Invalid wallet address", "address", address)
		return 0, fmt.Errorf("invalid wallet address: %s", address)
	}

	// If supportedCurrency contains a token address, add it to the context
	contextWithValues := ctx
	var tokenAddress string
	if supportedCurrency != nil && supportedCurrency.TokenAddress != "" {
		tokenAddress = supportedCurrency.TokenAddress
		// Create a new context with the token address
		contextWithValues = context.WithValue(ctx, constants.TokenAddressKey, tokenAddress)

		s.logger.Info("Added token address to context",
			"currency", currency.Symbol,
			"tokenAddress", tokenAddress)
	}

	if supportedCurrency != nil && supportedCurrency.Decimals > 0 {
		s.logger.Info("Added decimals to context",
			"currency", currency.Symbol,
			"decimals", supportedCurrency.Decimals)
		contextWithValues = context.WithValue(contextWithValues, constants.DecimalsKey, supportedCurrency.Decimals)
	}

	// Fetch the balance
	s.logger.Info("Calling provider.GetBalance",
		"currency", currency.Symbol,
		"address", address,
		"hasTokenAddress", tokenAddress != "")

	balance, err := provider.GetBalance(contextWithValues, address, currency)
	if err != nil {
		s.logger.Error("Failed to fetch balance", "error", err)
		return 0, fmt.Errorf("failed to fetch balance: %w", err)
	}

	s.logger.Info("Retrieved balance successfully",
		"address", address,
		"currency", currency.Symbol,
		"balance", balance)

	return balance, nil
}

// mockProvider is a temporary implementation until we fully integrate with the specific chain providers
type mockProvider struct {
	logger      logger.Logger
	chainSymbol string
}

// GetBalance retrieves a mock balance
func (p *mockProvider) GetBalance(ctx context.Context, address string, currency *models.Currency) (float64, error) {
	// Check if there's a token address in the context
	var hasTokenAddress bool
	var tokenAddress string
	if ctx.Value(constants.TokenAddressKey) != nil {
		if tokenAddrStr, ok := ctx.Value(constants.TokenAddressKey).(string); ok && tokenAddrStr != "" {
			tokenAddress = tokenAddrStr
			hasTokenAddress = true
		}
	}

	p.logger.Info("Getting mock balance",
		"chain", p.chainSymbol,
		"address", address,
		"hasTokenAddress", hasTokenAddress)

	// If token address is provided in context, return token balance
	if hasTokenAddress {
		p.logger.Info("Getting token balance",
			"chain", p.chainSymbol,
			"tokenAddress", tokenAddress)

		// Return different mock values for token balances
		switch p.chainSymbol {
		case "ETH":
			return 100.0, nil
		case "SOL":
			return 250.75, nil
		case "TRX":
			return 500.0, nil
		default:
			return 50.0, nil
		}
	}

	// Return different mock values for native balances based on chain symbol
	switch p.chainSymbol {
	case "BTC":
		return 0.1, nil
	case "ETH":
		return 0.5, nil
	case "SOL":
		return 10.5, nil
	case "TRX":
		return 1000.0, nil
	default:
		return 1.0, nil
	}
}

// ValidateAddress performs a mock address validation
func (p *mockProvider) ValidateAddress(address string) bool {
	// Simple chain-specific validation checks
	switch p.chainSymbol {
	case "BTC":
		return len(address) > 26 && len(address) < 35
	case "ETH", "BSC", "MATIC", "AVAX", "ARB", "OP":
		return len(address) == 42 && address[0:2] == "0x"
	case "SOL":
		return len(address) >= 32 && len(address) <= 44
	case "TRX":
		return len(address) == 34 && address[0] == 'T'
	default:
		return true
	}
}

// GetTokenDetails provides mock token details
func (p *mockProvider) GetTokenDetails(ctx context.Context, tokenAddress string) (*models.TokenDetails, error) {
	p.logger.Info("Getting mock token details", "chain", p.chainSymbol, "address", tokenAddress)

	// Return error for Bitcoin as it doesn't support tokens
	if p.chainSymbol == "BTC" {
		return nil, fmt.Errorf("tokens not supported on Bitcoin")
	}

	// Mock token details based on chain
	return &models.TokenDetails{
		Name:         fmt.Sprintf("Sample %s Token", p.chainSymbol),
		Symbol:       fmt.Sprintf("%sTKN", p.chainSymbol[:2]),
		Decimals:     18,
		TotalSupply:  "1000000000000000000000000",
		TokenAddress: tokenAddress,
		ChainID:      p.chainSymbol,
	}, nil
}

// WatchAddress performs mock address watching
func (p *mockProvider) WatchAddress(ctx context.Context, address string, onUpdate func(update BlockUpdate)) {
	p.logger.Info("Setting up mock watch for address", "chain", p.chainSymbol, "address", address)
	// Mock implementation - would connect to node and set up notifications
}

// CreateTransaction performs a mock transaction creation
func (p *mockProvider) CreateTransaction(ctx context.Context, params TransactionParams) (string, error) {
	p.logger.Info("Creating mock transaction",
		"chain", p.chainSymbol,
		"from", params.FromAddress,
		"to", params.ToAddress,
		"amount", params.Amount)
	return "0xmocktransactionhash", nil
}

// CreateDepositAddress creates a mock deposit address
func (p *mockProvider) CreateDepositAddress(ctx context.Context) (*bitcoin.DepositAddress, error) {
	p.logger.Info("Creating mock deposit address for", "chain", p.chainSymbol)

	var mockAddress, mockDerivationPath string

	// Generate a mock address based on chain
	switch p.chainSymbol {
	case "BTC":
		mockAddress = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
		mockDerivationPath = "m/44'/0'/0'/0/0"
	case "ETH", "BSC", "MATIC", "AVAX", "ARB", "OP":
		mockAddress = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
		mockDerivationPath = "m/44'/60'/0'/0/0"
	case "SOL":
		mockAddress = "DYw8jCTfwHNRJhhmFcbXvVDTqWMEVFBX6ZKUmG5CNSKK"
		mockDerivationPath = "m/44'/501'/0'/0'"
	case "TRX":
		mockAddress = "TMuA6YqfCeX8EhbfYEg5y7S4DqzSJireY9"
		mockDerivationPath = "m/44'/195'/0'/0/0"
	default:
		mockAddress = "generic_address_" + p.chainSymbol
		mockDerivationPath = "m/44'/0'/0'/0/0"
	}

	return &bitcoin.DepositAddress{
		Address:        mockAddress,
		PrivateKey:     "encrypted_private_key_mock",
		DerivationPath: mockDerivationPath,
	}, nil
}

// StartBlockMonitor starts mock block monitoring
func (p *mockProvider) StartBlockMonitor(ctx context.Context) error {
	p.logger.Info("Starting mock block monitor for", "chain", p.chainSymbol)
	return nil
}

// StopBlockMonitor stops mock block monitoring
func (p *mockProvider) StopBlockMonitor(ctx context.Context) error {
	p.logger.Info("Stopping mock block monitor for", "chain", p.chainSymbol)
	return nil
}

// GetChainInfo gets mock chain information
func (p *mockProvider) GetChainInfo(ctx context.Context) (*bitcoin.ChainInfo, error) {
	p.logger.Info("Getting mock chain info for", "chain", p.chainSymbol)

	// Return chain-specific mock data
	var height uint64
	var hash string

	switch p.chainSymbol {
	case "BTC":
		height = 800000
		hash = "000000000000000000015d43b6b8b657924cfecf396e5c1f50854b7ae6956122"
	case "ETH":
		height = 18000000
		hash = "0xb084c10440f05f5a23a55d1d7ebcb1b3892935fb56f23cdc9a7f42c348eed174"
	case "SOL":
		height = 150000000
		hash = "4uQeVj5tqViQh7yWWGStvkEG1Zmhx6uasJtWCJziofM"
	case "TRX":
		height = 50000000
		hash = "000000000000000002a7c4c1e48d76c3f94983b10b5d4a1c5dd4f311356bd2bb"
	default:
		height = 1000000
		hash = "0x" + p.chainSymbol + "000000000000000000000000000000000000"
	}

	return &bitcoin.ChainInfo{
		Height: height,
		Hash:   hash,
		Time:   time.Now(),
		Chain:  p.chainSymbol,
	}, nil
}

// GetBlockDetails gets mock block details
func (p *mockProvider) GetBlockDetails(ctx context.Context, blockHash string) (*bitcoin.BlockDetails, error) {
	p.logger.Info("Getting mock block details for", "chain", p.chainSymbol, "hash", blockHash)

	return &bitcoin.BlockDetails{
		Hash:              blockHash,
		Height:            1000000,
		Time:              time.Now(),
		PreviousBlockHash: "previous_" + blockHash,
		Transactions:      []string{"tx1_" + p.chainSymbol, "tx2_" + p.chainSymbol},
	}, nil
}

// GetTreasuryAddress returns a mock treasury address
func (p *mockProvider) GetTreasuryAddress() (string, error) {
	// Return chain-specific mock treasury address
	switch p.chainSymbol {
	case "BTC":
		return "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh", nil
	case "ETH", "BSC", "MATIC", "AVAX", "ARB", "OP":
		return "0x742d35Cc6634C0532925a3b844Bc454e4438f44e", nil
	case "SOL":
		return "DYw8jCTfwHNRJhhmFcbXvVDTqWMEVFBX6ZKUmG5CNSKK", nil
	case "TRX":
		return "TMuA6YqfCeX8EhbfYEg5y7S4DqzSJireY9", nil
	default:
		return "treasury_" + p.chainSymbol, nil
	}
}
