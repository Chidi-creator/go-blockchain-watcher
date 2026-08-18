package evm

import "encoding/json"

// AnkrRPCRequest represents a JSON-RPC request to the Ankr API
type AnkrRPCRequest struct {
	JsonRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

// AnkrRPCResponse represents a JSON-RPC response from the Ankr API
type AnkrRPCResponse struct {
	JsonRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *AnkrRPCError   `json:"error,omitempty"`
}

// AnkrRPCError represents a JSON-RPC error
type AnkrRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// AnkrBlock represents an Ethereum block from the Ankr API
type AnkrBlock struct {
	Number           string        `json:"number"`
	Hash             string        `json:"hash"`
	ParentHash       string        `json:"parentHash"`
	Nonce            string        `json:"nonce"`
	Sha3Uncles       string        `json:"sha3Uncles"`
	LogsBloom        string        `json:"logsBloom"`
	TransactionsRoot string        `json:"transactionsRoot"`
	StateRoot        string        `json:"stateRoot"`
	ReceiptsRoot     string        `json:"receiptsRoot"`
	Miner            string        `json:"miner"`
	Difficulty       string        `json:"difficulty"`
	TotalDifficulty  string        `json:"totalDifficulty"`
	ExtraData        string        `json:"extraData"`
	Size             string        `json:"size"`
	GasLimit         string        `json:"gasLimit"`
	GasUsed          string        `json:"gasUsed"`
	Timestamp        string        `json:"timestamp"`
	Transactions     []interface{} `json:"transactions"` // Can be txhashes or full tx objects
	Uncles           []string      `json:"uncles"`
	BaseFeePerGas    string        `json:"baseFeePerGas,omitempty"`
}

// AnkrTransaction represents an Ethereum transaction from the Ankr API
type AnkrTransaction struct {
	BlockHash            string `json:"blockHash"`
	BlockNumber          string `json:"blockNumber"`
	From                 string `json:"from"`
	Gas                  string `json:"gas"`
	GasPrice             string `json:"gasPrice"`
	Hash                 string `json:"hash"`
	Input                string `json:"input"`
	Nonce                string `json:"nonce"`
	To                   string `json:"to"`
	TransactionIndex     string `json:"transactionIndex"`
	Value                string `json:"value"`
	Type                 string `json:"type"`
	ChainID              string `json:"chainId,omitempty"`
	V                    string `json:"v"`
	R                    string `json:"r"`
	S                    string `json:"s"`
	MaxFeePerGas         string `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas string `json:"maxPriorityFeePerGas,omitempty"`
}

// AnkrBalance represents an account balance response
type AnkrBalance struct {
	Balance string `json:"balance"`
}

var SupportedChains = map[string]ChainConfig{
	"eth": {
		RpcURL:       "https://rpc.ankr.com/eth",
		ChainID:      1,
		Name:         "Ethereum",
		Symbol:       "ETH",
		NativeSymbol: "ETH",
		Decimals:     18,
	},
	"matic": {
		RpcURL:       "https://rpc.ankr.com/polygon",
		ChainID:      137,
		Name:         "Polygon",
		Symbol:       "MATIC",
		NativeSymbol: "MATIC",
		Decimals:     18,
	},
	"op": {
		RpcURL:       "https://rpc.ankr.com/optimism",
		ChainID:      10,
		Name:         "Optimism",
		Symbol:       "OP",
		NativeSymbol: "ETH",
		Decimals:     18,
	},
	"bsc": {
		RpcURL:       "https://rpc.ankr.com/bsc",
		ChainID:      56,
		Name:         "BNB Chain",
		Symbol:       "BSC",
		NativeSymbol: "BSC",
		Decimals:     18,
	},
	// "arb": {
	// 	RpcURL:       "https://rpc.ankr.com/arbitrum",
	// 	ChainID:      42161,
	// 	Name:         "Arbitrum One",
	// 	Symbol:       "ARB",
	// 	NativeSymbol: "ETH",
	// 	Decimals:     18,
	// },
}
