package evm

import (
	"math/big"
	"time"
)

// ChainConfig holds configuration for a specific EVM chain
type ChainConfig struct {
	ChainID        int    `json:"chainId"`
	Name           string `json:"name"`
	Symbol         string `json:"symbol"`
	RpcURL         string `json:"rpcUrl,omitempty"`
	ExplorerURL    string `json:"explorerUrl,omitempty"`
	IconURL        string `json:"iconUrl,omitempty"`
	NativeCurrency struct {
		Name     string `json:"name"`
		Symbol   string `json:"symbol"`
		Decimals int    `json:"decimals"`
	} `json:"nativeCurrency"`
}

// EVMBlock represents an Ethereum block
type EVMBlock struct {
	Hash             string    `json:"hash"`
	ParentHash       string    `json:"parentHash"`
	Number           *big.Int  `json:"number"`
	Timestamp        time.Time `json:"timestamp"`
	Nonce            string    `json:"nonce"`
	Difficulty       *big.Int  `json:"difficulty"`
	GasLimit         *big.Int  `json:"gasLimit"`
	GasUsed          *big.Int  `json:"gasUsed"`
	BaseFeePerGas    *big.Int  `json:"baseFeePerGas,omitempty"`
	Miner            string    `json:"miner"`
	TransactionsRoot string    `json:"transactionsRoot"`
	ReceiptsRoot     string    `json:"receiptsRoot"`
	StateRoot        string    `json:"stateRoot"`
	Size             *big.Int  `json:"size"`
	Transactions     []string  `json:"transactions,omitempty"`
}

// EVMTransaction represents an Ethereum transaction
type EVMTransaction struct {
	Hash             string   `json:"hash"`
	From             string   `json:"from"`
	To               string   `json:"to"`
	Value            *big.Int `json:"value"`
	Gas              *big.Int `json:"gas"`
	GasPrice         *big.Int `json:"gasPrice"`
	Nonce            uint64   `json:"nonce"`
	Input            string   `json:"input"`
	TransactionIndex uint64   `json:"transactionIndex"`
	BlockHash        string   `json:"blockHash"`
	BlockNumber      *big.Int `json:"blockNumber"`
}

// TokenDetails contains details about an ERC20 token
type TokenDetails struct {
	Name         string `json:"name"`
	Symbol       string `json:"symbol"`
	Decimals     int    `json:"decimals"`
	TotalSupply  string `json:"totalSupply"`
	TokenAddress string `json:"tokenAddress"`
	IconURL      string `json:"iconUrl,omitempty"`
	ChainID      string `json:"chainId"`
	ChainName    string `json:"chainName,omitempty"`
}

// EVMBlockMonitorJobData represents job data for EVM block monitoring
type EVMBlockMonitorJobData struct {
	LastProcessedBlockHeight uint64 `json:"lastProcessedBlockHeight"`
	ChainID                  int    `json:"chainId"`
	ChainSymbol              string `json:"chainSymbol"`
}

// Balance represents a balance response
type Balance struct {
	Address  string   `json:"address"`
	Balance  *big.Int `json:"balance"`
	Decimals int      `json:"decimals"`
}

// ERC20Token represents ERC20 token information for contract interactions
type ERC20Token struct {
	Address     string `json:"address"`
	ABI         string `json:"abi,omitempty"`
	Name        string `json:"name,omitempty"`
	Symbol      string `json:"symbol,omitempty"`
	Decimals    int    `json:"decimals,omitempty"`
	TotalSupply string `json:"totalSupply,omitempty"`
}

// TransactionReceipt represents a transaction receipt
type TransactionReceipt struct {
	TransactionHash   string   `json:"transactionHash"`
	TransactionIndex  uint64   `json:"transactionIndex"`
	BlockHash         string   `json:"blockHash"`
	BlockNumber       *big.Int `json:"blockNumber"`
	From              string   `json:"from"`
	To                string   `json:"to"`
	CumulativeGasUsed *big.Int `json:"cumulativeGasUsed"`
	GasUsed           *big.Int `json:"gasUsed"`
	ContractAddress   string   `json:"contractAddress"`
	Status            uint64   `json:"status"`
	Logs              []Log    `json:"logs"`
}

// Log represents a log entry in a transaction receipt
type Log struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      *big.Int `json:"blockNumber"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex uint64   `json:"transactionIndex"`
	BlockHash        string   `json:"blockHash"`
	LogIndex         uint64   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

// EVMNetwork represents an EVM network's configuration
type EVMNetwork struct {
	ChainID  *big.Int `json:"chainId"`
	Name     string   `json:"name"`
	Ensured  bool     `json:"ensured"`
	Features []string `json:"features"`
}

// Wallet represents an EVM wallet with its private key
type Wallet struct {
	Address    string `json:"address"`
	PrivateKey string `json:"privateKey,omitempty"`
}
