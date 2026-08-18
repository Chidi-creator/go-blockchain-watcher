package socket

import (
	"encoding/json"
	"fmt"
	"net/http"

	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

// ExampleHandler shows how to use the socket manager in an HTTP handler
func ExampleHandler() http.Handler {
	// Initialize the socket manager with default settings
	socketManager := Initialize()

	// Add an event handler for a specific event
	socketManager.AddEventHandler("wallet_update", func(client *Client, data []byte) {
		// Handle wallet update
		var update struct {
			WalletID string  `json:"wallet_id"`
			Balance  float64 `json:"balance"`
		}
		if err := json.Unmarshal(data, &update); err != nil {
			logger.Get().Error("Failed to parse wallet update", "error", err.Error())
			return
		}

		// Log the wallet update
		logger.Get().Info("Received wallet update", "walletID", update.WalletID, "balance", update.Balance)

		// Send confirmation back to the client
		client.SendEvent("wallet_update_received", map[string]interface{}{
			"wallet_id": update.WalletID,
			"status":    "processed",
		})

		// Send the update to a specific room (all clients interested in this wallet)
		roomID := fmt.Sprintf("wallet-%s", update.WalletID)
		socketManager.EmitToRoom(roomID, "balance_changed", map[string]interface{}{
			"wallet_id": update.WalletID,
			"balance":   update.Balance,
		})
	})

	// Add an event handler for client joining a room
	socketManager.AddEventHandler("join_wallet_room", func(client *Client, data []byte) {
		var join struct {
			WalletID string `json:"wallet_id"`
		}
		if err := json.Unmarshal(data, &join); err != nil {
			logger.Get().Error("Failed to parse join wallet room request", "error", err.Error())
			return
		}

		// Join the client to the wallet room
		roomID := fmt.Sprintf("wallet-%s", join.WalletID)
		client.JoinRoom(roomID)

		// Notify the client they've joined the room
		client.SendEvent("joined_wallet_room", map[string]string{
			"wallet_id": join.WalletID,
			"room_id":   roomID,
		})
	})

	// Create an HTTP mux for handling requests
	mux := http.NewServeMux()

	// WebSocket endpoint
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		socketManager.HandleWebSocket(w, r)
	})

	// Example API endpoint that uses the socket manager to broadcast a message
	mux.HandleFunc("/api/broadcast", func(w http.ResponseWriter, r *http.Request) {
		socketManager.EmitToAll("broadcast", map[string]string{
			"message": "This is a broadcast message",
		})
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"sent"}`))
	})

	return mux
}

// ExampleNodeJSConnection shows how to connect to a Node.js Socket.IO server
func ExampleNodeJSConnection(nodeServerURL string, authToken string, userID string) {
	// Connect to the Node.js server
	nodeConn, err := ConnectToNodeServer(nodeServerURL, authToken, userID)
	if err != nil {
		logger.Get().Error("Failed to connect to Node.js server", "error", err.Error())
		return
	}

	// Add event handlers for Node.js server events
	nodeConn.AddEventHandler("transaction_created", func(data []byte) {
		var transaction struct {
			ID     string  `json:"id"`
			Amount float64 `json:"amount"`
		}
		if err := json.Unmarshal(data, &transaction); err != nil {
			logger.Get().Error("Failed to parse transaction data", "error", err.Error())
			return
		}

		logger.Get().Info("Received transaction from Node.js", "transactionID", transaction.ID, "amount", transaction.Amount)

		// Process the transaction
		// ...

		// Send acknowledgment back to Node.js
		nodeConn.EmitEvent("transaction_received", map[string]interface{}{
			"transaction_id": transaction.ID,
			"status":         "processing",
		})

		// Broadcast to all connected clients
		Get().EmitToAll("new_transaction", map[string]interface{}{
			"transaction_id": transaction.ID,
			"amount":         transaction.Amount,
		})
	})

	// Example of sending a message to the Node.js server
	nodeConn.EmitEvent("server_status", map[string]interface{}{
		"status":  "online",
		"version": "1.0.0",
	})
}
