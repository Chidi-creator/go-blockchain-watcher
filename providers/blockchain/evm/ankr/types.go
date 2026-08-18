package ankr

import (
	"encoding/json"
)

// AnkrRPCRequest represents a request to the Ankr RPC API
type AnkrRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

// AnkrRPCResponse represents a response from the Ankr RPC API
type AnkrRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// EVMBlock represents a simple block structure
type EVMBlock struct {
	Hash         string   `json:"hash"`
	ParentHash   string   `json:"parentHash"`
	Number       string   `json:"number"`
	Timestamp    string   `json:"timestamp"`
	Transactions []string `json:"transactions,omitempty"`
}
