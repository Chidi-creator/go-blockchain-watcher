package types

// BlockData represents data about a blockchain block
type BlockData struct {
	ChainID      int         `json:"chainId,omitempty"`
	ChainSymbol  string      `json:"chainSymbol"`
	BlockNumber  interface{} `json:"blockNumber"` // Can be uint64 for most chains or string for some
	BlockHash    string      `json:"blockHash,omitempty"`
	Timestamp    interface{} `json:"timestamp,omitempty"` // Can be uint64 or int64
	ParentHash   string      `json:"parentHash,omitempty"`
	Transactions interface{} `json:"transactions,omitempty"` // Array of transactions specific to each chain
}

// EVMBlockMonitorJobData represents job data for EVM block monitoring
type EVMBlockMonitorJobData struct {
	LastProcessedBlockHeight uint64 `json:"lastProcessedBlockHeight"`
	ChainID                  int    `json:"chainId"`
	ChainSymbol              string `json:"chainSymbol"`
}

// BitcoinBlockMonitorJobData represents job data for Bitcoin block monitoring
type BitcoinBlockMonitorJobData struct {
	LastProcessedBlockHeight uint64 `json:"lastProcessedBlockHeight"`
}

// SolanaBlockMonitorJobData represents job data for Solana block monitoring
type SolanaBlockMonitorJobData struct {
	LastProcessedBlockHeight uint64 `json:"lastProcessedBlockHeight"`
}

// TronBlockMonitorJobData represents job data for Tron block monitoring
type TronBlockMonitorJobData struct {
	LastProcessedBlockHeight uint64 `json:"lastProcessedBlockHeight"`
}

// TronWatcherJobData represents job data for Tron transaction watching
type TronWatcherJobData struct {
	ProviderID        string      `json:"providerId"`
	SupportedCurrency interface{} `json:"supportedCurrency"`
	Address           string      `json:"address"`
	Order             interface{} `json:"order,omitempty"`
	ContractAddress   string      `json:"contractAddress,omitempty"`
}

// TransactionData represents basic transaction data
type TransactionData struct {
	TxID          string      `json:"txId,omitempty"`
	Hash          string      `json:"hash,omitempty"`
	FromAddress   string      `json:"fromAddress,omitempty"`
	ToAddress     string      `json:"toAddress,omitempty"`
	Amount        interface{} `json:"amount,omitempty"`
	Timestamp     interface{} `json:"timestamp,omitempty"`
	Confirmations uint64      `json:"confirmations,omitempty"`
	Status        string      `json:"status,omitempty"`
	Data          interface{} `json:"data,omitempty"`
}
