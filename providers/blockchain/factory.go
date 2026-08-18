package blockchain

import (
	"fmt"
	"strings"
	"sync"

	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/managers/cache"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/managers/queue"
	"bitbucket.org/zapspace/zap-go-server/models"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain/bitcoin"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain/changenow"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain/evm"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain/solana"
	"bitbucket.org/zapspace/zap-go-server/providers/blockchain/tron"
)

// ProviderType represents different blockchain provider types
type ProviderType string

const (
	// Provider types
	BTCProvider       ProviderType = "BTC"
	EVMProvider       ProviderType = "EVM"
	SOLANAProvider    ProviderType = "SOLANA"
	TRONProvider      ProviderType = "TRON"
	CHANGENOWProvider ProviderType = "CHANGENOW"
)

// ProviderFactory creates and manages blockchain providers
type ProviderFactory struct {
	log             logger.Logger
	bitcoinConfig   config.BitcoinConfig
	evmConfig       config.EVMConfig
	solanaConfig    config.SolanaConfig
	tronConfig      config.TronConfig
	changeNowConfig config.ChangeNowConfig
	providers       map[string]bitcoin.ChainProvider
	providersMu     sync.RWMutex
	queueManager    *queue.QueueManager
	cacheManager    *cache.CacheManager
}

// NewProviderFactory creates a new blockchain provider factory
func NewProviderFactory(
	log logger.Logger,
	bitcoinConfig config.BitcoinConfig,
	evmConfig config.EVMConfig,
	solanaConfig config.SolanaConfig,
	tronConfig config.TronConfig,
	changeNowConfig config.ChangeNowConfig,
	queueManager *queue.QueueManager,
	cacheManager *cache.CacheManager,
) *ProviderFactory {
	return &ProviderFactory{
		log:             log,
		bitcoinConfig:   bitcoinConfig,
		evmConfig:       evmConfig,
		solanaConfig:    solanaConfig,
		tronConfig:      tronConfig,
		changeNowConfig: changeNowConfig,
		providers:       make(map[string]bitcoin.ChainProvider),
		queueManager:    queueManager,
		cacheManager:    cacheManager,
	}
}

// GetProviderForCurrency returns the appropriate blockchain provider for a given currency
func (f *ProviderFactory) GetProviderForCurrency(currency *models.Currency, chain *models.Chain) (bitcoin.ChainProvider, error) {
	// Add detailed logging about the chain and currency
	f.log.Info("GetProviderForCurrency called",
		"currencySymbol", currency.Symbol,
		"chainSymbol", chain.Symbol,
		"chainID", chain.ChainID,
		"chainIDType", fmt.Sprintf("%T", chain.ChainID),
		"networkType", chain.NetworkType)

	// Check if we already have this provider instantiated
	providerKey := fmt.Sprintf("%s_%s", currency.Symbol, chain.Symbol)

	fmt.Println(providerKey, "PROVIDER KEY")

	f.providersMu.RLock()
	provider, exists := f.providers[providerKey]
	f.providersMu.RUnlock()

	if exists {
		f.log.Info("Using existing provider", "providerKey", providerKey)
		return provider, nil
	}

	// We need to create a new provider
	f.providersMu.Lock()
	defer f.providersMu.Unlock()

	// Double-check in case another goroutine created it while we were waiting for the lock
	if provider, exists := f.providers[providerKey]; exists {
		f.log.Info("Using existing provider (after lock)", "providerKey", providerKey)
		return provider, nil
	}

	var newProvider bitcoin.ChainProvider
	var err error

	// Log chain details for troubleshooting
	f.log.Info("Creating new provider",
		"providerKey", providerKey,
		"chainName", chain.Name,
		"networkType", chain.NetworkType,
		"rpcURL", chain.RpcURL,
		"chainID", chain.ChainID,
		"chainSymbol", chain.Symbol,
		"currencySymbol", currency.Symbol)

	// Determine provider type based on chain symbol similar to GetProviderForSymbols
	chainSymbol := strings.ToLower(chain.Symbol)

	switch {
	case chainSymbol == "btc" || chain.NetworkType == "BITCOIN":
		f.log.Info("Creating Bitcoin provider")
		newProvider, err = bitcoin.NewProvider(f.log, f.bitcoinConfig)

	case chainSymbol == "eth" || chainSymbol == "bsc" || chainSymbol == "matic" ||
		chainSymbol == "arb" || chainSymbol == "op" || chainSymbol == "avax" ||
		chainSymbol == "ftm" || chainSymbol == "base" || chainSymbol == "gnosis" ||
		chain.NetworkType == "EVM":
		f.log.Info("Creating EVM provider for chain", "symbol", chain.Symbol)
		newProvider, err = evm.NewProvider(f.log, f.evmConfig, chain.Symbol)

	case chainSymbol == "sol" || chain.NetworkType == "SOLANA":
		f.log.Info("Creating Solana provider")
		newProvider, err = solana.NewProvider(f.log, f.solanaConfig)

	case chainSymbol == "trx" || chain.NetworkType == "TRON":
		f.log.Info("Creating Tron provider")
		newProvider, err = tron.NewProvider(f.log, f.tronConfig)

	case chainSymbol == "changenow" || chain.NetworkType == "CHANGENOW":
		f.log.Info("Creating ChangeNow provider")
		newProvider, err = changenow.NewProvider(f.log, f.changeNowConfig, f.queueManager, f.cacheManager)

	default:
		// Fallback to NetworkType if we couldn't determine from symbol
		if chain.NetworkType != "" {
			f.log.Info("Falling back to NetworkType for provider selection", "networkType", chain.NetworkType)
			switch chain.NetworkType {
			case "BITCOIN":
				newProvider, err = bitcoin.NewProvider(f.log, f.bitcoinConfig)
			case "EVM":
				newProvider, err = evm.NewProvider(f.log, f.evmConfig, chain.Symbol)
			case "SOLANA":
				newProvider, err = solana.NewProvider(f.log, f.solanaConfig)
			case "TRON":
				newProvider, err = tron.NewProvider(f.log, f.tronConfig)
			case "CHANGENOW":
				newProvider, err = changenow.NewProvider(f.log, f.changeNowConfig, f.queueManager, f.cacheManager)
			default:
				f.log.Error("Unsupported chain network type", "networkType", chain.NetworkType)
				return nil, fmt.Errorf("unsupported chain network type: %s", chain.NetworkType)
			}
		} else {
			f.log.Error("Could not determine provider type from chain symbol or network type",
				"symbol", chain.Symbol,
				"networkType", chain.NetworkType)
			return nil, fmt.Errorf("unsupported chain symbol: %s", chain.Symbol)
		}
	}

	if err != nil {
		f.log.Error("Failed to create provider",
			"error", err,
			"providerKey", providerKey,
			"chainSymbol", chain.Symbol,
			"chainNetworkType", chain.NetworkType,
			"chainID", chain.ChainID)
		return nil, fmt.Errorf("failed to create provider for %s: %w", providerKey, err)
	}

	// Cache the provider for future use
	f.providers[providerKey] = newProvider
	f.log.Info("Provider created and cached successfully",
		"providerKey", providerKey,
		"chainSymbol", chain.Symbol)

	return newProvider, nil
}

// GetProviderForSymbols returns provider based on currency and chain symbols
func (f *ProviderFactory) GetProviderForSymbols(currencySymbol, chainSymbol string) (bitcoin.ChainProvider, error) {
	// This is a simplified version - in a real implementation, you'd look up the currency and chain
	// from your database or service, then call GetProviderForCurrency

	var chainType string
	switch {
	case chainSymbol == "BTC":
		chainType = "BITCOIN"
	case chainSymbol == "ETH" || chainSymbol == "BSC" || chainSymbol == "MATIC" || chainSymbol == "ARBITRUM":
		chainType = "EVM"
	case chainSymbol == "SOL":
		chainType = "SOLANA"
	case chainSymbol == "TRX":
		chainType = "TRON"
	case chainSymbol == "CHANGENOW":
		chainType = "CHANGENOW"
	default:
		return nil, fmt.Errorf("unsupported chain symbol: %s", chainSymbol)
	}

	// Simplified model objects for provider initialization
	currency := &models.Currency{
		Symbol: currencySymbol,
	}

	chain := &models.Chain{
		Symbol:      chainSymbol,
		NetworkType: chainType,
	}

	return f.GetProviderForCurrency(currency, chain)
}

// GetChangeNowProvider returns the ChangeNow provider directly
func (f *ProviderFactory) GetChangeNowProvider() (*changenow.Provider, error) {
	providerKey := "changenow_provider"

	f.providersMu.RLock()
	provider, exists := f.providers[providerKey]
	f.providersMu.RUnlock()

	if exists {
		if changeNowProvider, ok := provider.(*changenow.Provider); ok {
			return changeNowProvider, nil
		}
	}

	// Create the provider
	f.providersMu.Lock()
	defer f.providersMu.Unlock()

	// Check again after acquiring the lock
	if provider, exists := f.providers[providerKey]; exists {
		if changeNowProvider, ok := provider.(*changenow.Provider); ok {
			return changeNowProvider, nil
		}
	}

	// Create a new provider
	newProvider, err := changenow.NewProvider(f.log, f.changeNowConfig, f.queueManager, f.cacheManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create ChangeNow provider: %w", err)
	}

	// Cache the provider
	f.providers[providerKey] = newProvider

	// Return the provider with proper type
	if changeNowProvider, ok := newProvider.(*changenow.Provider); ok {
		return changeNowProvider, nil
	}

	return nil, fmt.Errorf("failed to cast to ChangeNow provider")
}
