package socket

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

// NodeSocketConnection represents a connection to a Node.js socket.io server
type NodeSocketConnection struct {
	URL                string
	Socket             *websocket.Conn
	Connected          bool
	Send               chan []byte
	Done               chan struct{}
	Handlers           map[string][]func(data []byte)
	Reconnecting       bool
	AuthToken          string
	UserID             string
	LastResponse       *http.Response // Store the last HTTP response
	pingIntervalUpdate chan time.Duration
}

// NewNodeSocketConnection creates a new connection to a Node.js socket.io server
func NewNodeSocketConnection(serverURL string, authToken string, userID string) *NodeSocketConnection {
	return &NodeSocketConnection{
		URL:                serverURL,
		Connected:          false,
		Send:               make(chan []byte, 256),
		Done:               make(chan struct{}),
		Handlers:           make(map[string][]func(data []byte)),
		AuthToken:          authToken,
		UserID:             userID,
		pingIntervalUpdate: make(chan time.Duration, 1), // Buffer of 1 to prevent blocking
	}
}

// Connect establishes a connection to the Node.js server
func (nc *NodeSocketConnection) Connect() error {
	// Parse the URL
	baseURL, err := url.Parse(nc.URL)
	if err != nil {
		return err
	}

	// Ensure we have the correct Socket.IO path format
	var socketURL string
	if !strings.Contains(baseURL.Path, "/socket.io") {
		// Append Socket.IO specific path - use EIO=4 for Socket.IO v4
		socketIOPath := "/socket.io/?EIO=4&transport=websocket"
		u, err := url.Parse(baseURL.String() + socketIOPath)
		if err != nil {
			return err
		}
		socketURL = u.String()
	} else {
		// If URL already has socket.io path, just ensure transport parameter is set
		q := baseURL.Query()
		if q.Get("transport") == "" {
			q.Set("transport", "websocket")
		}
		if q.Get("EIO") == "" {
			q.Set("EIO", "4") // Socket.IO v4
		}
		baseURL.RawQuery = q.Encode()
		socketURL = baseURL.String()
	}

	// Create header with authentication - this is crucial for the Go service authentication
	header := http.Header{}

	// These headers are checked by the Node.js server's authenticateSocket method
	if nc.AuthToken != "" {
		header.Add("Authorization", "Bearer "+nc.AuthToken)
	}
	if nc.UserID != "" {
		header.Add("User-ID", nc.UserID)
	}

	// This critical header is what triggers the special handling for Go wallet service
	// Looking at the Node.js auth code, this is what it checks for:
	// const goWalletService = socket.handshake.headers?.["go-wallet-service"] === "true";
	header.Add("go-wallet-service", "true")

	logger.Get().Info("Connecting to Socket.IO server",
		"url", socketURL,
		"headers", fmt.Sprintf("%v", header))

	// Connect to the server with enhanced settings
	dialer := &websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  45 * time.Second,
		EnableCompression: true,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Skip certificate verification for development
		},
	}

	conn, resp, err := dialer.Dial(socketURL, header)
	if err != nil {
		// Enhanced error logging
		if resp != nil {
			logger.Get().Error("Failed to connect to Socket.IO server",
				"error", err.Error(),
				"status", resp.Status,
				"headers", resp.Header)

			// Try to read response body for any error message
			if resp.Body != nil {
				bodyBytes, readErr := io.ReadAll(resp.Body)
				if readErr == nil && len(bodyBytes) > 0 {
					logger.Get().Error("Response body", "body", string(bodyBytes))
				}
				resp.Body.Close()
			}
		} else {
			logger.Get().Error("Failed to connect to Socket.IO server with no response", "error", err.Error())
		}
		return err
	}

	// Store the response for later inspection
	nc.LastResponse = resp
	nc.Socket = conn
	nc.Connected = true

	logger.Get().Info("Successfully connected to WebSocket server", "url", socketURL, "status", resp.Status)

	// Start read and write pumps
	go nc.readPump()
	go nc.writePump()

	// Socket.IO handshake will be handled in the readPump when we receive the initial packet
	// The server will send a handshake packet (type 0) first, and we'll respond appropriately

	return nil
}

// FindWorkingWebSocketPath attempts to connect to the WebSocket path
func (nc *NodeSocketConnection) FindWorkingWebSocketPath() string {
	logger.Get().Info("Testing WebSocket connection")

	// Parse the URL
	u, err := url.Parse(nc.URL)
	if err != nil {
		logger.Get().Error("Failed to parse URL", "error", err)
		return ""
	}

	// Convert wss:// to ws:// since we know the server only supports HTTP
	if u.Scheme == "wss" {
		logger.Get().Info("Server only supports HTTP, converting wss:// to ws:// for connection")
		u.Scheme = "ws"
	}

	// Set up headers for authentication
	header := http.Header{}
	header.Add("Authorization", "Bearer "+nc.AuthToken)
	header.Add("User-ID", nc.UserID)

	// Add special header to identify this client as the Go wallet service
	header.Add("X-Client-Type", "Go-Wallet-Service")

	// Set timeout for connection attempts
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	// Try to connect with the URL
	logger.Get().Debug("Testing connection", "url", u.String())

	conn, resp, err := dialer.Dial(u.String(), header)
	if err != nil {
		logger.Get().Error("Connection test failed",
			"url", u.String(),
			"error", err.Error())

		if resp != nil {
			logger.Get().Debug("Response status",
				"status", resp.Status,
				"statusCode", resp.StatusCode)
		}

		return ""
	}

	// Success!
	logger.Get().Info("WebSocket connection successful", "path", u.Path)
	conn.Close()
	return u.Path
}

// readPump handles incoming messages from the Node.js server
func (nc *NodeSocketConnection) readPump() {
	defer func() {
		nc.Connected = false
		nc.Socket.Close()
		// Trigger reconnect
		go nc.reconnect()
	}()

	// Set read limits to match Node.js server configuration
	readTimeout := 120 * time.Second // Increased from 80s to 120s to be more tolerant of network delays

	nc.Socket.SetReadLimit(1024 * 1024) // 1MB
	nc.Socket.SetReadDeadline(time.Now().Add(readTimeout))
	nc.Socket.SetPongHandler(func(string) error {
		// Update read deadline when we receive a pong
		nc.Socket.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})

	for {
		_, message, err := nc.Socket.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Get().Error("Node.js socket read error", "error", err.Error())
			} else if strings.Contains(err.Error(), "use of closed network connection") {
				logger.Get().Info("Connection already closed")
				break
			} else if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				logger.Get().Info("Socket closed normally")
				break
			} else {
				logger.Get().Info("Socket connection closed", "error", err.Error())
			}
			break
		}

		// Reset read deadline on any message
		nc.Socket.SetReadDeadline(time.Now().Add(readTimeout))

		// Log the raw message for debugging
		msgStr := string(message)
		logger.Get().Debug("Received message", "raw", msgStr)

		// Socket.IO protocol handling - messages start with a packet type
		if len(msgStr) > 0 && msgStr[0] >= '0' && msgStr[0] <= '9' {
			packetType := msgStr[0]
			remainingMsg := ""
			if len(msgStr) > 1 {
				remainingMsg = msgStr[1:]
			}

			switch packetType {
			case '0': // Connect/handshake packet
				logger.Get().Info("Socket.IO handshake received")

				// Extract session ID and ping interval from handshake
				var handshake struct {
					SID          string   `json:"sid"`
					PingInterval int      `json:"pingInterval"`
					PingTimeout  int      `json:"pingTimeout"`
					Upgrades     []string `json:"upgrades"`
				}

				if err := json.Unmarshal([]byte(remainingMsg), &handshake); err == nil {
					// Log detailed handshake information
					logger.Get().Info("Socket.IO handshake details",
						"sid", handshake.SID,
						"pingInterval", handshake.PingInterval,
						"pingTimeout", handshake.PingTimeout,
						"upgrades", handshake.Upgrades)

					// Update ping interval to match server settings
					if handshake.PingInterval > 0 && nc.pingIntervalUpdate != nil {
						// Convert from milliseconds to time.Duration and reduce by 10% to be conservative
						interval := time.Duration(handshake.PingInterval) * time.Millisecond * 9 / 10
						select {
						case nc.pingIntervalUpdate <- interval:
							logger.Get().Info("Set ping interval from handshake",
								"interval", interval.String())
						default:
							logger.Get().Warn("Failed to update ping interval (channel full)")
						}
					}

					// Connect to the default namespace first
					time.Sleep(100 * time.Millisecond) // Brief pause
					err := nc.Socket.WriteMessage(websocket.TextMessage, []byte("40"))
					if err != nil {
						logger.Get().Error("Failed to connect to default namespace", "error", err.Error())
					} else {
						logger.Get().Info("Connected to default namespace")
					}

					// Connect to the /services namespace
					time.Sleep(100 * time.Millisecond) // Brief pause
					err = nc.Socket.WriteMessage(websocket.TextMessage, []byte("40/services"))
					if err != nil {
						logger.Get().Error("Failed to connect to /services namespace", "error", err.Error())
					} else {
						logger.Get().Info("Requested connection to /services namespace")
					}
				} else {
					logger.Get().Error("Failed to parse handshake", "error", err.Error(), "data", remainingMsg)
				}

			case '1': // Disconnect packet
				logger.Get().Info("Socket.IO disconnect received")

			case '2': // Ping packet
				// Respond with a pong (packet type 3)
				logger.Get().Debug("Socket.IO ping received, sending pong")
				err := nc.Socket.WriteMessage(websocket.TextMessage, []byte("3"))
				if err != nil {
					logger.Get().Error("Failed to respond to ping", "error", err.Error())
				}

				// Always reset the read deadline on ping/pong activity
				nc.Socket.SetReadDeadline(time.Now().Add(readTimeout))

			case '3': // Pong packet
				logger.Get().Debug("Socket.IO pong received")
				// Reset read deadline on receiving a pong
				nc.Socket.SetReadDeadline(time.Now().Add(readTimeout))

			case '4': // Message packet
				if len(remainingMsg) > 0 {
					// Handle namespace acknowledgements (e.g., "0/services,{...}")
					if strings.Contains(remainingMsg, "/services") && strings.Contains(remainingMsg, "sid") {
						logger.Get().Info("Received namespace connection acknowledgment", "namespace", "/services")
						// Successfully connected to the namespace
						continue
					}

					// First character after the packet type indicates the namespace
					// "0" means the default namespace

					// Rest of the message is the actual data/payload
					// Try to extract the event and data based on common Socket.IO formats
					payload := remainingMsg
					if strings.HasPrefix(payload, "0") || strings.HasPrefix(payload, "2") {
						// Handle format like "40" (connect to default namespace) or "42[...]" (event in default namespace)
						payload = payload[1:]
					}

					// If it's in the array format (like ["event", {...}])
					if strings.HasPrefix(payload, "[") {
						var eventData []json.RawMessage
						if err := json.Unmarshal([]byte(payload), &eventData); err == nil && len(eventData) > 0 {
							// First element is the event name
							var eventName string
							if err := json.Unmarshal(eventData[0], &eventName); err == nil {
								logger.Get().Debug("Socket.IO event received", "event", eventName)

								// Process event data if available
								if len(eventData) > 1 {
									// Call the appropriate handler for this event
									nc.processMessage(eventName, eventData[1])
								} else {
									// Event with no data
									nc.processMessage(eventName, []byte("{}"))
								}
							}
						} else {
							logger.Get().Error("Failed to parse Socket.IO event", "error", err, "data", payload)
						}
					} else {
						// Try standard format as fallback
						var inboundMsg InboundMessage
						if err := json.Unmarshal([]byte(payload), &inboundMsg); err == nil {
							nc.processMessage(inboundMsg.Event, message)
						} else {
							logger.Get().Error("Failed to parse Socket.IO message", "error", err.Error(), "data", payload)
						}
					}
				}

			default:
				logger.Get().Debug("Unknown Socket.IO packet type", "type", string(packetType), "data", remainingMsg)
			}

			continue
		}

		// Standard WebSocket message format (not Socket.IO)
		var inboundMsg InboundMessage
		if err := json.Unmarshal(message, &inboundMsg); err != nil {
			logger.Get().Error("Failed to parse non-Socket.IO message", "error", err.Error(), "raw", msgStr)
			continue
		}

		// Process the message
		nc.processMessage(inboundMsg.Event, message)
	}
}

// writePump handles outgoing messages to the Node.js server
func (nc *NodeSocketConnection) writePump() {
	// Start with default values that will be updated after handshake
	pingInterval := 25 * time.Second
	pingTimeout := 60 * time.Second // Increased from 20s to 60s for better reliability

	// Create a heartbeat ticker that will be reset after handshake
	ticker := time.NewTicker(pingInterval)
	defer func() {
		ticker.Stop()
		nc.Socket.Close()
	}()

	// This flag helps track if we've received the handshake with ping settings
	handshakeReceived := false

	// Channel to update ping interval
	pingIntervalUpdate := make(chan time.Duration, 1)

	// Listen for ping interval updates from the readPump
	go func() {
		for interval := range pingIntervalUpdate {
			// Only modify if significantly different
			if !handshakeReceived || math.Abs(float64(interval-pingInterval)) > float64(time.Second) {
				ticker.Reset(interval)
				pingInterval = interval
				handshakeReceived = true
				logger.Get().Info("Updated Socket.IO ping interval", "newInterval", interval)
			}
		}
	}()

	// Store the channel for access from readPump
	nc.pingIntervalUpdate = pingIntervalUpdate

	for {
		select {
		case <-nc.Done:
			close(pingIntervalUpdate)
			return

		case message, ok := <-nc.Send:
			nc.Socket.SetWriteDeadline(time.Now().Add(pingTimeout))
			if !ok {
				// Channel closed
				close(pingIntervalUpdate)
				nc.Socket.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// Log outgoing message for debugging
			logger.Get().Debug("Sending message", "message", string(message))

			w, err := nc.Socket.NextWriter(websocket.TextMessage)
			if err != nil {
				logger.Get().Error("Failed to get next writer", "error", err.Error())
				continue
			}

			w.Write(message)

			// Send any queued messages
			n := len(nc.Send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-nc.Send)
			}

			if err := w.Close(); err != nil {
				logger.Get().Error("Failed to close writer", "error", err.Error())
				continue
			}

		case <-ticker.C:
			if !handshakeReceived {
				// Don't send pings until we've received the handshake
				continue
			}

			// Send Socket.IO ping packet (type 2) - this is critical for the connection to stay alive
			nc.Socket.SetWriteDeadline(time.Now().Add(pingTimeout))
			err := nc.Socket.WriteMessage(websocket.TextMessage, []byte("2"))
			if err != nil {
				logger.Get().Error("Failed to write ping message", "error", err.Error())

				if strings.Contains(err.Error(), "broken pipe") ||
					strings.Contains(err.Error(), "use of closed") ||
					strings.Contains(err.Error(), "close sent") {
					// Fatal connection errors - trigger reconnect
					return
				}

				// Non-fatal errors, continue the loop
				continue
			}

			logger.Get().Debug("Sent Socket.IO ping")
		}
	}
}

// reconnect attempts to reconnect to the Node.js server after a delay
func (nc *NodeSocketConnection) reconnect() {
	if nc.Reconnecting {
		return
	}

	nc.Reconnecting = true
	defer func() {
		nc.Reconnecting = false
	}()

	// Wait a bit before reconnecting
	time.Sleep(5 * time.Second)

	// Attempt to reconnect
	for i := 0; i < 5; i++ {
		logger.Get().Info("Attempting to reconnect to Node.js server", "attempt", i+1)
		err := nc.Connect()
		if err == nil {
			logger.Get().Info("Successfully reconnected to Node.js server")
			return
		}

		logger.Get().Error("Failed to reconnect to Node.js server", "error", err.Error())
		time.Sleep(time.Duration(i+1) * 5 * time.Second) // Exponential backoff
	}

	logger.Get().Error("Failed to reconnect to Node.js server after multiple attempts")
}

// AddEventHandler adds a handler for a specific event
func (nc *NodeSocketConnection) AddEventHandler(eventName string, handler func(data []byte)) {
	if _, exists := nc.Handlers[eventName]; !exists {
		nc.Handlers[eventName] = []func(data []byte){}
	}
	nc.Handlers[eventName] = append(nc.Handlers[eventName], handler)
}

// RemoveEventHandler removes a handler for a specific event
func (nc *NodeSocketConnection) RemoveEventHandler(eventName string) {
	delete(nc.Handlers, eventName)
}

// processMessage processes an incoming message from the Node.js server
func (nc *NodeSocketConnection) processMessage(eventName string, data []byte) {
	handlers, exists := nc.Handlers[eventName]
	if !exists {
		logger.Get().Debug("No handler for Node.js event", "event", eventName)
		return
	}

	for _, handler := range handlers {
		handler(data)
	}
}

// EmitEvent sends an event to the Node.js server using Socket.IO protocol
func (nc *NodeSocketConnection) EmitEvent(eventName string, data interface{}) error {
	if !nc.Connected {
		return fmt.Errorf("not connected to Socket.IO server")
	}

	// Create array containing [eventName, data] as per Socket.IO protocol
	eventArray := []interface{}{eventName, data}
	eventJSON, err := json.Marshal(eventArray)
	if err != nil {
		logger.Get().Error("Failed to marshal event data", "error", err.Error())
		return err
	}

	// Create a Socket.IO message packet with the /services namespace
	// Format: 42/services[eventName,data]
	// 4 = message packet, 2 = event with data, /services = namespace
	message := "42/services" + string(eventJSON)

	logger.Get().Info("Emitting event to /services namespace",
		"event", eventName,
		"message", message)

	// Queue the message for sending
	select {
	case nc.Send <- []byte(message):
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("send buffer full or timed out")
	}
}

// Close closes the connection to the Node.js server
func (nc *NodeSocketConnection) Close() {
	if !nc.Connected {
		return
	}

	close(nc.Done)
	nc.Socket.Close()
	nc.Connected = false
}

// GetConnectionInfo returns information about the connection
func (nc *NodeSocketConnection) GetConnectionInfo() map[string]interface{} {
	info := map[string]interface{}{
		"connected":     nc.Connected,
		"url":           nc.URL,
		"userId":        nc.UserID,
		"reconnecting":  nc.Reconnecting,
		"authenticated": false,
	}

	// Add authentication status based on the last response
	if nc.LastResponse != nil {
		info["status_code"] = nc.LastResponse.StatusCode
		info["status"] = nc.LastResponse.Status

		// Check for authentication headers or cookies in the response
		// This will vary based on how your Node.js server indicates successful auth
		if authHeader := nc.LastResponse.Header.Get("X-Auth-Status"); authHeader != "" {
			info["auth_status"] = authHeader
			info["authenticated"] = authHeader == "success"
		}

		// Get other relevant headers
		if serverHeader := nc.LastResponse.Header.Get("Server"); serverHeader != "" {
			info["server"] = serverHeader
		}
	}

	return info
}

// TestSocketIOConnection performs a diagnostic test of the Socket.IO connection
// and logs detailed information about the connection status
func (nc *NodeSocketConnection) TestSocketIOConnection() map[string]interface{} {
	results := map[string]interface{}{
		"connected":       nc.Connected,
		"url":             nc.URL,
		"connection_time": time.Now().Format(time.RFC3339),
		"test_results":    make(map[string]interface{}),
	}

	testResults := map[string]interface{}{}

	// Check if we have a valid URL
	baseURL, err := url.Parse(nc.URL)
	if err != nil {
		testResults["url_valid"] = false
		testResults["url_error"] = err.Error()
	} else {
		testResults["url_valid"] = true
		testResults["url_scheme"] = baseURL.Scheme
		testResults["url_host"] = baseURL.Host
		testResults["url_path"] = baseURL.Path
	}

	// Test connection if not already connected
	if !nc.Connected {
		logger.Get().Info("Testing Socket.IO connection to server", "url", nc.URL)
		path := nc.FindWorkingWebSocketPath()

		if path == "" {
			testResults["connection_test"] = "failed"
		} else {
			testResults["connection_test"] = "success"
			testResults["working_path"] = path
		}
	} else {
		testResults["connection_test"] = "skipped - already connected"
	}

	// Add lastResponse info if available
	if nc.LastResponse != nil {
		responseInfo := map[string]interface{}{
			"status":       nc.LastResponse.Status,
			"status_code":  nc.LastResponse.StatusCode,
			"content_type": nc.LastResponse.Header.Get("Content-Type"),
		}

		// Extract headers that might be relevant
		for _, header := range []string{"Connection", "Upgrade", "Sec-WebSocket-Version", "Sec-WebSocket-Accept"} {
			if value := nc.LastResponse.Header.Get(header); value != "" {
				responseInfo[header] = value
			}
		}

		testResults["last_response"] = responseInfo
	}

	// Log the test results
	logger.Get().Info("Socket.IO connection test results",
		"url", nc.URL,
		"connected", nc.Connected,
		"test_results", testResults)

	results["test_results"] = testResults
	return results
}

// DebugSocketIOConnection provides detailed debugging for Socket.IO connections
func (nc *NodeSocketConnection) DebugSocketIOConnection() {
	if !nc.Connected {
		logger.Get().Warn("Cannot debug Socket.IO connection - not connected")
		return
	}

	// Log current connection state
	logger.Get().Info("Socket.IO connection debug",
		"url", nc.URL,
		"connected", nc.Connected,
		"reconnecting", nc.Reconnecting,
		"connection_info", nc.GetConnectionInfo())

	// Send a ping packet to test connectivity
	err := nc.Socket.WriteMessage(websocket.TextMessage, []byte("2"))
	if err != nil {
		logger.Get().Error("Failed to send Socket.IO ping packet", "error", err.Error())
	} else {
		logger.Get().Info("Sent Socket.IO ping packet")
	}

	// Send a specialized test event
	testData := map[string]interface{}{
		"timestamp": time.Now().Unix(),
		"test_id":   GetRandomID(), // Use existing function from the package
		"client":    "go-wallet-service",
	}

	if err := nc.EmitEvent("socket_io_test", testData); err != nil {
		logger.Get().Error("Failed to send Socket.IO test event", "error", err.Error())
	} else {
		logger.Get().Info("Sent Socket.IO test event")
	}
}

// SendRawSocketIOMessage sends a raw message using the Socket.IO protocol format
// This is useful for debugging or implementing specific protocol features
func (nc *NodeSocketConnection) SendRawSocketIOMessage(packetType string, data string) error {
	if !nc.Connected {
		return fmt.Errorf("not connected")
	}

	message := packetType + data
	logger.Get().Debug("Sending raw Socket.IO message", "message", message)

	return nc.Socket.WriteMessage(websocket.TextMessage, []byte(message))
}
