package socket

import (
	"errors"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

// SocketManager manages WebSocket connections
type SocketManager struct {
	// Singleton instance
	mu                   sync.Mutex
	Config               SocketConfig
	clients              map[string]*Client
	rooms                map[string]*Room
	userRegistry         map[string][]string // userId -> []clientId
	dynamicEventHandlers map[string][]EventHandler
	upgrader             websocket.Upgrader
	authFunc             func(r *http.Request) (string, error)

	// Node.js socket connection
	nodeSocketConn *NodeSocketConnection
}

var (
	instance *SocketManager
	once     sync.Once
)

// GetInstance returns the singleton instance of SocketManager
func GetInstance() *SocketManager {
	once.Do(func() {
		instance = &SocketManager{
			Config:               DefaultSocketConfig(),
			clients:              make(map[string]*Client),
			rooms:                make(map[string]*Room),
			userRegistry:         make(map[string][]string),
			dynamicEventHandlers: make(map[string][]EventHandler),
			upgrader: websocket.Upgrader{
				ReadBufferSize:  1024,
				WriteBufferSize: 1024,
				CheckOrigin: func(r *http.Request) bool {
					return true // Allow all origins for now
				},
			},
		}
	})
	return instance
}

// Initialize sets up the socket manager with the provided configuration
func (sm *SocketManager) Initialize(config SocketConfig, authFunc func(r *http.Request) (string, error)) *SocketManager {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.Config = config
	sm.authFunc = authFunc

	logger.Get().Info("Socket manager initialized")
	return sm
}

// HandleWebSocket upgrades HTTP requests to WebSocket connections
func (sm *SocketManager) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Authenticate the request
	userID, err := sm.authFunc(r)
	if err != nil {
		logger.Get().Error("Authentication failed", "error", err.Error())
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Upgrade the HTTP connection to a WebSocket connection
	conn, err := sm.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Get().Error("Failed to upgrade to WebSocket", "error", err.Error())
		return
	}

	// Create a new client
	client := NewClient(conn, sm, userID)
	sm.Register(client)

	// Start goroutines for handling client messages
	go client.ReadPump()
	go client.WritePump()

	// Emit connected event
	client.SendEvent("connected", map[string]string{
		"message":  "Connected to server",
		"clientId": client.ID,
	})
}

// Register adds a client to the manager
func (sm *SocketManager) Register(client *Client) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Add client to the clients map
	sm.clients[client.ID] = client

	// Add client to user registry
	if client.UserID != "" {
		if _, exists := sm.userRegistry[client.UserID]; !exists {
			sm.userRegistry[client.UserID] = []string{}
		}
		sm.userRegistry[client.UserID] = append(sm.userRegistry[client.UserID], client.ID)
	}

	logger.Get().Info("Client registered", "clientID", client.ID, "userID", client.UserID)
}

// Unregister removes a client from the manager
func (sm *SocketManager) Unregister(client *Client) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if client exists
	if _, ok := sm.clients[client.ID]; !ok {
		return errors.New("client not found")
	}

	// Remove client from all rooms
	for roomID := range client.Rooms {
		if room, exists := sm.rooms[roomID]; exists {
			delete(room.Clients, client.ID)
			// If room is empty, remove it
			if len(room.Clients) == 0 {
				delete(sm.rooms, roomID)
			}
		}
	}

	// Remove client from user registry
	if client.UserID != "" {
		if clientIDs, exists := sm.userRegistry[client.UserID]; exists {
			for i, id := range clientIDs {
				if id == client.ID {
					// Remove this client ID from the slice
					sm.userRegistry[client.UserID] = append(clientIDs[:i], clientIDs[i+1:]...)
					break
				}
			}
			// If no more clients for this user, remove the user entry
			if len(sm.userRegistry[client.UserID]) == 0 {
				delete(sm.userRegistry, client.UserID)
			}
		}
	}

	// Remove client from clients map
	delete(sm.clients, client.ID)

	logger.Get().Info("Client unregistered", "clientID", client.ID, "userID", client.UserID)
	return nil
}

// AddEventHandler adds a dynamic event handler
func (sm *SocketManager) AddEventHandler(eventName string, handler EventHandler) *SocketManager {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.dynamicEventHandlers[eventName]; !exists {
		sm.dynamicEventHandlers[eventName] = []EventHandler{}
	}
	sm.dynamicEventHandlers[eventName] = append(sm.dynamicEventHandlers[eventName], handler)

	logger.Get().Info("Event handler added", "event", eventName)
	return sm
}

// RemoveEventHandler removes a dynamic event handler
func (sm *SocketManager) RemoveEventHandler(eventName string, handler EventHandler) *SocketManager {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	handlers, exists := sm.dynamicEventHandlers[eventName]
	if !exists {
		return sm
	}

	// Find and remove the handler
	for i, h := range handlers {
		if &h == &handler {
			sm.dynamicEventHandlers[eventName] = append(handlers[:i], handlers[i+1:]...)
			break
		}
	}

	// If no more handlers for this event, remove the event entry
	if len(sm.dynamicEventHandlers[eventName]) == 0 {
		delete(sm.dynamicEventHandlers, eventName)
	}

	logger.Get().Info("Event handler removed", "event", eventName)
	return sm
}

// ProcessMessage processes an incoming message
func (sm *SocketManager) ProcessMessage(client *Client, eventName string, message []byte) {
	sm.mu.Lock()
	handlers, exists := sm.dynamicEventHandlers[eventName]
	sm.mu.Unlock()

	if exists {
		for _, handler := range handlers {
			go handler(client, message)
		}
	} else {
		logger.Get().Debug("No handler for event", "event", eventName, "clientID", client.ID)
	}
}

// EmitToUser sends an event to all clients of a user
func (sm *SocketManager) EmitToUser(userID string, eventName string, data interface{}) error {
	sm.mu.Lock()
	clientIDs, exists := sm.userRegistry[userID]
	sm.mu.Unlock()

	if !exists || len(clientIDs) == 0 {
		return errors.New("user not connected")
	}

	var lastErr error
	for _, clientID := range clientIDs {
		sm.mu.Lock()
		client, exists := sm.clients[clientID]
		sm.mu.Unlock()

		if exists {
			if err := client.SendEvent(eventName, data); err != nil {
				lastErr = err
			}
		}
	}

	return lastErr
}

// EmitToRoom sends an event to all clients in a room
func (sm *SocketManager) EmitToRoom(roomID string, eventName string, data interface{}) error {
	sm.mu.Lock()
	room, exists := sm.rooms[roomID]
	sm.mu.Unlock()

	if !exists {
		return errors.New("room not found")
	}

	var lastErr error
	for clientID := range room.Clients {
		sm.mu.Lock()
		client, exists := sm.clients[clientID]
		sm.mu.Unlock()

		if exists {
			if err := client.SendEvent(eventName, data); err != nil {
				lastErr = err
			}
		}
	}

	return lastErr
}

// EmitToAll sends an event to all connected clients
func (sm *SocketManager) EmitToAll(eventName string, data interface{}) {
	sm.mu.Lock()
	clients := make([]*Client, 0, len(sm.clients))
	for _, client := range sm.clients {
		clients = append(clients, client)
	}
	sm.mu.Unlock()

	for _, client := range clients {
		client.SendEvent(eventName, data)
	}
}

// IsUserConnected checks if a user has any connected clients
func (sm *SocketManager) IsUserConnected(userID string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	clientIDs, exists := sm.userRegistry[userID]
	return exists && len(clientIDs) > 0
}

// GetUserClientIDs gets all client IDs for a user
func (sm *SocketManager) GetUserClientIDs(userID string) []string {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if clientIDs, exists := sm.userRegistry[userID]; exists {
		result := make([]string, len(clientIDs))
		copy(result, clientIDs)
		return result
	}
	return []string{}
}

// clientJoinRoom adds a client to a room
func (sm *SocketManager) clientJoinRoom(client *Client, roomID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Create room if it doesn't exist
	if _, exists := sm.rooms[roomID]; !exists {
		sm.rooms[roomID] = &Room{
			ID:      roomID,
			Clients: make(map[string]*Client),
		}
	}

	// Add client to room
	sm.rooms[roomID].Clients[client.ID] = client
	client.Rooms[roomID] = true

	logger.Get().Info("Client joined room", "clientID", client.ID, "roomID", roomID)
}

// clientLeaveRoom removes a client from a room
func (sm *SocketManager) clientLeaveRoom(client *Client, roomID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if room exists
	room, exists := sm.rooms[roomID]
	if !exists {
		return
	}

	// Remove client from room
	delete(room.Clients, client.ID)
	delete(client.Rooms, roomID)

	// If room is empty, remove it
	if len(room.Clients) == 0 {
		delete(sm.rooms, roomID)
	}

	logger.Get().Info("Client left room", "clientID", client.ID, "roomID", roomID)
}

// CreateUserRoom creates a room for a user and adds all their clients to it
func (sm *SocketManager) CreateUserRoom(userID string, roomID string) error {
	sm.mu.Lock()
	clientIDs, exists := sm.userRegistry[userID]
	sm.mu.Unlock()

	if !exists || len(clientIDs) == 0 {
		return errors.New("user not connected")
	}

	for _, clientID := range clientIDs {
		sm.mu.Lock()
		client, exists := sm.clients[clientID]
		sm.mu.Unlock()

		if exists {
			sm.clientJoinRoom(client, roomID)
		}
	}

	return nil
}

// Shutdown gracefully closes all connections
func (sm *SocketManager) Shutdown() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	logger.Get().Info("Socket manager shutting down")

	// Close all client connections
	for _, client := range sm.clients {
		if client.Socket != nil {
			client.Socket.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Server shutting down"))
			client.Socket.Close()
		}
	}

	// Clear all data structures
	sm.clients = make(map[string]*Client)
	sm.rooms = make(map[string]*Room)
	sm.userRegistry = make(map[string][]string)
	sm.dynamicEventHandlers = make(map[string][]EventHandler)

	logger.Get().Info("Socket manager shutdown complete")
}

// ConnectToNodeServer creates a connection to a Node.js server and stores it
func (sm *SocketManager) ConnectToNodeServer(serverURL string, authToken string, userID string) (*NodeSocketConnection, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	conn := NewNodeSocketConnection(serverURL, authToken, userID)
	err := conn.Connect()
	if err != nil {
		logger.Get().Error("Failed to connect to Node.js server", "error", err.Error())
		return nil, err
	}

	// Store the connection
	sm.nodeSocketConn = conn

	return conn, nil
}

// GetNodeConnection returns the current Node.js socket connection
func (sm *SocketManager) GetNodeConnection() *NodeSocketConnection {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.nodeSocketConn
}
