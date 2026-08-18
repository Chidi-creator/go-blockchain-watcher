package socket

import (
	"net/http"
	"time"

	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

// Get returns the singleton instance of the socket manager
func Get() *SocketManager {
	return GetInstance()
}

// Initialize initializes the socket manager with a default authentication function
func Initialize() *SocketManager {
	config := DefaultSocketConfig()

	// Default authentication function that allows all connections
	authFunc := func(r *http.Request) (string, error) {
		// Extract user ID from query parameters, headers, or JWT token
		// This is a placeholder - replace with your actual authentication logic
		userID := r.URL.Query().Get("userId")
		if userID == "" {
			userID = r.Header.Get("X-User-ID")
		}

		// If no user ID is found, generate a guest ID
		if userID == "" {
			userID = "guest-" + GetRandomID()
		}

		return userID, nil
	}

	logger.Get().Info("Initializing socket manager with default configuration")
	return GetInstance().Initialize(config, authFunc)
}

// InitializeWithAuth initializes the socket manager with a custom authentication function
func InitializeWithAuth(config SocketConfig, authFunc func(r *http.Request) (string, error)) *SocketManager {
	logger.Get().Info("Initializing socket manager with custom authentication")
	return GetInstance().Initialize(config, authFunc)
}

// ConnectToNodeServer creates a new connection to a Node.js socket.io server
func ConnectToNodeServer(serverURL string, authToken string, userID string) (*NodeSocketConnection, error) {
	// Use the socket manager to create and store the connection
	socketManager := GetInstance()
	return socketManager.ConnectToNodeServer(serverURL, authToken, userID)
}

// GetNodeConnection returns the current Node.js socket connection
func GetNodeConnection() *NodeSocketConnection {
	socketManager := GetInstance()
	return socketManager.GetNodeConnection()
}

// GetRandomID generates a random ID for guests
func GetRandomID() string {
	return "user-" + generateRandomString(8)
}

// generateRandomString generates a random string of the specified length
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[getRandomInt(len(charset))]
	}
	return string(b)
}

// getRandomInt returns a random integer between 0 and max-1
func getRandomInt(max int) int {
	return int(time.Now().UnixNano() % int64(max))
}
