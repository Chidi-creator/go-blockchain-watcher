package bitcoin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"bitbucket.org/zapspace/zap-go-server/config/constants"
	config "bitbucket.org/zapspace/zap-go-server/config/system"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/managers/queue"
	"bitbucket.org/zapspace/zap-go-server/models"
)

// ChainProvider interface to be used until internal/blockchain is built
type ChainProvider interface {
	GetBalance(ctx context.Context, address string, currency *models.Currency) (float64, error)
	ValidateAddress(address string) bool
	GetTokenDetails(ctx context.Context, tokenAddress string) (*models.TokenDetails, error)
	WatchAddress(ctx context.Context, address string, onUpdate func(update BlockUpdate))
	CreateTransaction(ctx context.Context, params TransactionParams) (string, error)
	CreateDepositAddress(ctx context.Context) (*DepositAddress, error)
	StartBlockMonitor(ctx context.Context) error
	StopBlockMonitor(ctx context.Context) error
	GetChainInfo(ctx context.Context) (*ChainInfo, error)
	GetBlockDetails(ctx context.Context, blockHash string) (*BlockDetails, error)
	GetTreasuryAddress() (string, error)
}

// BlockUpdate contains information about a block update
type BlockUpdate struct {
	ChainSymbol  string
	BlockNumber  uint64
	BlockHash    string
	Transactions []TransactionData
}

// TransactionData contains information about a transaction
type TransactionData struct {
	TxHash        string
	FromAddress   string
	ToAddress     string
	Amount        float64
	Timestamp     uint64
	Confirmations uint64
	Status        string // "success", "pending", "failed"
}

// TransactionParams contains parameters for creating a transaction
type TransactionParams struct {
	FromAddress  string
	ToAddress    string
	Amount       float64
	TokenAddress string  // Optional, for token transfers
	GasPrice     float64 // Optional
	GasLimit     uint64  // Optional
	UseTreasury  bool    // Optional, use treasury address instead of user address
}

// DepositAddress represents a cryptocurrency deposit address
type DepositAddress struct {
	Address        string
	PrivateKey     string // Encrypted private key
	DerivationPath string
}

// ChainInfo represents blockchain information
type ChainInfo struct {
	Height uint64
	Hash   string
	Time   time.Time
	Chain  string
}

// BlockDetails contains detailed information about a specific block
type BlockDetails struct {
	Hash              string
	Height            uint64
	Time              time.Time
	PreviousBlockHash string
	Transactions      []string
}

// UTXO represents an unspent transaction output
type UTXO struct {
	TxHash       string
	OutputIdx    uint32
	Value        uint64
	ScriptPubKey string
}

// Provider implements the ChainProvider interface for Bitcoin
type Provider struct {
	logger          logger.Logger
	rpcURL          string
	network         string
	explorerAPI     string
	treasuryAddress string
	privateKeyEnc   string
}

// NewProvider creates a new Bitcoin provider
func NewProvider(logger logger.Logger, cfg config.BitcoinConfig) (ChainProvider, error) {
	// Determine explorer API based on network
	explorerAPI := "https://api.blockcypher.com/v1/btc/main"
	if strings.ToLower(cfg.Network) == "testnet" {
		explorerAPI = "https://api.blockcypher.com/v1/btc/test3"
	}

	// Get treasury address and private key from environment variables
	// In a real implementation, these would be securely stored
	treasuryAddress := os.Getenv("BITCOIN_TREASURY_ADDRESS")
	privateKey := os.Getenv("BITCOIN_PRIVATE_KEY")

	return &Provider{
		logger:          logger,
		rpcURL:          cfg.RPCEndpoint,
		network:         cfg.Network,
		explorerAPI:     explorerAPI,
		treasuryAddress: treasuryAddress,
		privateKeyEnc:   privateKey,
	}, nil
}

// GetBalance retrieves the balance for a Bitcoin address
func (p *Provider) GetBalance(ctx context.Context, address string, currency *models.Currency) (float64, error) {
	p.logger.Info("Getting Bitcoin balance", "address", address)

	url := fmt.Sprintf("%s/addrs/%s/balance", p.explorerAPI, address)
	p.logger.Info("Fetching BTC balance", "url", url)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch balance: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to fetch balance, status code: %d", resp.StatusCode)
	}

	var balanceResp struct {
		FinalBalance int64 `json:"final_balance"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&balanceResp); err != nil {
		return 0, fmt.Errorf("failed to decode balance response: %w", err)
	}

	// Convert from satoshis to BTC
	balanceBtc := float64(balanceResp.FinalBalance) / 1e8
	return balanceBtc, nil
}

// ValidateAddress checks if a Bitcoin address is valid
func (p *Provider) ValidateAddress(address string) bool {
	// Check for empty address
	if len(address) == 0 {
		return false
	}

	// Check length and format based on address type
	if strings.HasPrefix(address, "1") {
		// P2PKH addresses
		return len(address) >= 26 && len(address) <= 34
	} else if strings.HasPrefix(address, "3") {
		// P2SH addresses
		return len(address) >= 26 && len(address) <= 34
	} else if strings.HasPrefix(address, "bc1") {
		// SegWit addresses (bech32)
		if len(address) == 42 {
			// P2WPKH
			return true
		} else if len(address) >= 62 && len(address) <= 64 {
			// P2WSH
			return true
		}
	}

	// For more robust validation, a full bech32/base58check implementation would be needed
	// This is a simplified check
	return len(address) >= 26 && len(address) <= 64
}

// GetTokenDetails is not relevant for Bitcoin as it doesn't have tokens natively
func (p *Provider) GetTokenDetails(ctx context.Context, tokenAddress string) (*models.TokenDetails, error) {
	p.logger.Info("Getting Bitcoin token details", "tokenAddress", tokenAddress)

	// For native BTC
	if strings.ToLower(tokenAddress) == "btc" || strings.ToLower(tokenAddress) == "bitcoin" {
		return &models.TokenDetails{
			Name:         "Bitcoin",
			Symbol:       "BTC",
			Decimals:     8,
			TokenAddress: "bitcoin",
			IconURL:      "https://assets.coingecko.com/coins/images/1/large/bitcoin.png",
			ChainID:      "1", // Bitcoin's chain ID
		}, nil
	}

	// For Omni Layer tokens
	// Check if the tokenAddress is a number (Omni token ID)
	if isNumeric(tokenAddress) {
		// This would be replaced with a real Omni Layer API call
		omniAPI := fmt.Sprintf("https://api.omniexplorer.info/v1/property/%s", tokenAddress)

		req, err := http.NewRequestWithContext(ctx, "GET", omniAPI, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Omni HTTP request: %w", err)
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			p.logger.Warn("Error fetching Omni token details", "error", err)
			// Continue with defaults rather than failing
		} else if resp.StatusCode == http.StatusOK {
			defer resp.Body.Close()

			var omniResp struct {
				Name      string `json:"name"`
				Symbol    string `json:"symbol"`
				Precision int    `json:"precision"`
			}

			if err := json.NewDecoder(resp.Body).Decode(&omniResp); err == nil {
				return &models.TokenDetails{
					Name:         omniResp.Name,
					Symbol:       omniResp.Symbol,
					Decimals:     omniResp.Precision,
					TokenAddress: tokenAddress,
					ChainID:      "1", // Bitcoin's chain ID
				}, nil
			}
		}
	}

	// Default response for unknown token types
	return &models.TokenDetails{
		Name:         "Unknown Bitcoin Asset",
		Symbol:       "UNKNOWN",
		Decimals:     8,
		TokenAddress: tokenAddress,
		ChainID:      "1", // Bitcoin's chain ID
	}, nil
}

// WatchAddress sets up a watch for a Bitcoin address
func (p *Provider) WatchAddress(ctx context.Context, address string, onUpdate func(update BlockUpdate)) {
	p.logger.Info("Setting up watch for Bitcoin address", "address", address)
	// In a real implementation, this would store the address in a database
	// and use a background job to poll for updates or connect to a WebSocket

	// This is a simplified placeholder implementation
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				p.logger.Info("Stopping address watch", "address", address)
				return
			case <-ticker.C:
				p.logger.Debug("Checking for updates to address", "address", address)
				// Here you would check for new transactions
				// and call onUpdate with the block update data
			}
		}
	}()
}

// CreateTransaction creates a Bitcoin transaction
func (p *Provider) CreateTransaction(ctx context.Context, params TransactionParams) (string, error) {
	p.logger.Info("Creating Bitcoin transaction",
		"from", params.FromAddress,
		"to", params.ToAddress,
		"amount", params.Amount)

	// Determine which address to use
	addressToUse := params.FromAddress
	if params.UseTreasury {
		var err error
		addressToUse, err = p.GetTreasuryAddress()
		if err != nil {
			return "", fmt.Errorf("failed to get treasury address: %w", err)
		}
	}

	// 1. Get UTXOs for the address
	utxos, err := p.getUTXOs(ctx, addressToUse)
	if err != nil {
		return "", fmt.Errorf("failed to get UTXOs: %w", err)
	}

	// 2. Calculate required amount in satoshis
	satNeeded := uint64(params.Amount * 1e8)
	fee := uint64(10000) // Fixed fee of 10,000 satoshis

	// 3. Select UTXOs to cover the amount
	selectedUTXOs, totalSats, err := p.selectUTXOs(utxos, satNeeded+fee)
	if err != nil {
		return "", err
	}

	// 4. Build the transaction (this would use bitcoind RPC or a library like btcd)
	// For this implementation, we're just returning a mock transaction ID
	// In a real implementation, you would:
	// - Create a transaction with inputs from the selected UTXOs
	// - Add outputs for the recipient and change
	// - Sign the transaction with the private key
	// - Broadcast the transaction to the network

	// Calculate change
	change := totalSats - satNeeded - fee

	p.logger.Info("Transaction prepared",
		"inputs", len(selectedUTXOs),
		"amount", satNeeded,
		"fee", fee,
		"change", change)

	// In a real implementation, this would return the actual transaction ID
	return "mock_tx_id_" + time.Now().Format(time.RFC3339), nil
}

// CreateDepositAddress creates a new Bitcoin deposit address
func (p *Provider) CreateDepositAddress(ctx context.Context) (*DepositAddress, error) {
	p.logger.Info("Creating new Bitcoin deposit address")

	// In a real implementation, this would:
	// 1. Generate a new key pair using your HD wallet
	// 2. Derive a Bitcoin address from the public key
	// 3. Securely store or encrypt the private key

	// This is a mock implementation
	mockAddress := "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
	mockPrivateKey := "encrypted_private_key_mock"
	mockDerivationPath := "m/44'/0'/0'/0/0"

	return &DepositAddress{
		Address:        mockAddress,
		PrivateKey:     mockPrivateKey,
		DerivationPath: mockDerivationPath,
	}, nil
}

// StartBlockMonitor starts monitoring Bitcoin blocks
func (p *Provider) StartBlockMonitor(ctx context.Context) error {
	p.logger.Info("Starting Bitcoin block monitor")

	// Get current block height to start monitoring from
	chainInfo, err := p.GetChainInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get chain info: %w", err)
	}

	p.logger.Info("Starting block monitor", "height", chainInfo.Height, "chain", "bitcoin")

	// Access the queue manager from context or dependency injection
	queueManager, ok := ctx.Value("queueManager").(*queue.QueueManager)
	if !ok {
		return fmt.Errorf("queue manager not found in context")
	}

	// Create job data
	jobData := map[string]interface{}{
		"lastProcessedBlockHeight": chainInfo.Height,
		"chainSymbol":              "btc",
	}

	// Schedule job with queue manager
	options := map[string]interface{}{
		"repeat": map[string]interface{}{
			"every": 60000, // Check every minute (in milliseconds) - Bitcoin has slower block times
		},
		"jobId":            "btc-block-monitor",
		"removeOnComplete": false,
		"removeOnFail":     false,
	}

	jobId, err := queueManager.AddJob(ctx, constants.QueueBitcoinBlockMonitor, jobData, options)
	if err != nil {
		p.logger.Error("Failed to schedule Bitcoin block monitor job", "error", err, "chain", "bitcoin")
		return fmt.Errorf("failed to schedule Bitcoin block monitor job: %w", err)
	}

	p.logger.Info("Bitcoin block monitor job scheduled", "jobId", jobId)
	return nil
}

// StopBlockMonitor stops the block monitoring process
func (p *Provider) StopBlockMonitor(ctx context.Context) error {
	p.logger.Info("Stopping Bitcoin block monitor")

	// Access the queue manager from context or dependency injection
	queueManager, ok := ctx.Value("queueManager").(*queue.QueueManager)
	if !ok {
		return fmt.Errorf("queue manager not found in context")
	}

	// Get the job status before clearing
	status, err := queueManager.GetQueueStatus(ctx, constants.QueueBitcoinBlockMonitor)
	if err != nil {
		p.logger.Warn("Failed to get queue status", "error", err, "chain", "bitcoin")
		// Continue with clearing attempt even if status check fails
	} else {
		p.logger.Info("Bitcoin block monitor queue status before stopping",
			"chain", "bitcoin",
			"activeJobs", status["active"],
			"waitingJobs", status["waiting"],
			"completedJobs", status["completed"],
			"failedJobs", status["failed"])
	}

	// Clear the queue to stop processing
	err = queueManager.ClearQueue(ctx, constants.QueueBitcoinBlockMonitor)
	if err != nil {
		p.logger.Error("Failed to clear Bitcoin block monitor queue", "error", err, "chain", "bitcoin")
		return fmt.Errorf("failed to clear Bitcoin block monitor queue: %w", err)
	}

	p.logger.Info("Bitcoin block monitor stopped successfully", "chain", "bitcoin")
	return nil
}

// GetChainInfo gets current blockchain information
func (p *Provider) GetChainInfo(ctx context.Context) (*ChainInfo, error) {
	p.logger.Info("Getting Bitcoin chain info")

	req, err := http.NewRequestWithContext(ctx, "GET", p.explorerAPI, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chain info: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch chain info, status code: %d", resp.StatusCode)
	}

	var chainResp struct {
		Height uint64 `json:"height"`
		Hash   string `json:"hash"`
		Time   string `json:"time"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&chainResp); err != nil {
		return nil, fmt.Errorf("failed to decode chain info response: %w", err)
	}

	// Parse time
	timeValue, err := time.Parse(time.RFC3339, chainResp.Time)
	if err != nil {
		timeValue = time.Now() // Fallback to current time if parsing fails
	}

	return &ChainInfo{
		Height: chainResp.Height,
		Hash:   chainResp.Hash,
		Time:   timeValue,
		Chain:  "bitcoin",
	}, nil
}

// GetBlockDetails gets detailed information about a specific block
func (p *Provider) GetBlockDetails(ctx context.Context, blockHash string) (*BlockDetails, error) {
	p.logger.Info("Getting Bitcoin block details", "hash", blockHash)

	url := fmt.Sprintf("%s/blocks/%s", p.explorerAPI, blockHash)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch block details: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch block details, status code: %d", resp.StatusCode)
	}

	var blockResp struct {
		Hash      string   `json:"hash"`
		Height    uint64   `json:"height"`
		Time      string   `json:"time"`
		PrevBlock string   `json:"prev_block"`
		TxIDs     []string `json:"txids"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&blockResp); err != nil {
		return nil, fmt.Errorf("failed to decode block details response: %w", err)
	}

	// Parse time
	timeValue, err := time.Parse(time.RFC3339, blockResp.Time)
	if err != nil {
		timeValue = time.Now() // Fallback to current time if parsing fails
	}

	return &BlockDetails{
		Hash:              blockResp.Hash,
		Height:            blockResp.Height,
		Time:              timeValue,
		PreviousBlockHash: blockResp.PrevBlock,
		Transactions:      blockResp.TxIDs,
	}, nil
}

// GetTreasuryAddress returns the configured treasury address
func (p *Provider) GetTreasuryAddress() (string, error) {
	if p.treasuryAddress == "" {
		return "", fmt.Errorf("treasury address is not configured")
	}
	return p.treasuryAddress, nil
}

// Helper functions

// getUTXOs fetches the unspent transaction outputs for an address
func (p *Provider) getUTXOs(ctx context.Context, address string) ([]UTXO, error) {
	url := fmt.Sprintf("%s/addrs/%s?unspentOnly=true", p.explorerAPI, address)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch UTXOs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch UTXOs, status code: %d", resp.StatusCode)
	}

	var utxoResp struct {
		TxRefs []struct {
			TxHash    string `json:"tx_hash"`
			TxOutputN uint32 `json:"tx_output_n"`
			Value     uint64 `json:"value"`
			Script    string `json:"script"`
		} `json:"txrefs"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&utxoResp); err != nil {
		return nil, fmt.Errorf("failed to decode UTXOs response: %w", err)
	}

	utxos := make([]UTXO, 0, len(utxoResp.TxRefs))
	for _, ref := range utxoResp.TxRefs {
		utxos = append(utxos, UTXO{
			TxHash:       ref.TxHash,
			OutputIdx:    ref.TxOutputN,
			Value:        ref.Value,
			ScriptPubKey: ref.Script,
		})
	}

	return utxos, nil
}

// selectUTXOs selects UTXOs to cover the required amount
func (p *Provider) selectUTXOs(utxos []UTXO, required uint64) ([]UTXO, uint64, error) {
	var selected []UTXO
	var total uint64

	for _, utxo := range utxos {
		selected = append(selected, utxo)
		total += utxo.Value

		if total >= required {
			break
		}
	}

	if total < required {
		return nil, 0, fmt.Errorf("insufficient funds: required %d satoshis, available %d satoshis", required, total)
	}

	return selected, total, nil
}

// isNumeric checks if a string is numeric
func isNumeric(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}
