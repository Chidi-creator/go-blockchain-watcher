package socket

import (
	"time"

	"github.com/gorilla/websocket"
)

// EventHandler is a function that handles a socket event
type EventHandler func(client *Client, data []byte)

// Socket connection statuses
const (
	StatusConnecting = "connecting"
	StatusConnected  = "connected"
	StatusClosed     = "closed"
	StatusError      = "error"
)

// Client represents a connected websocket client
type Client struct {
	ID             string
	UserID         string
	Socket         *websocket.Conn
	Send           chan []byte
	Manager        *SocketManager // Forward declaration, defined in manager.go
	LastPong       time.Time
	ConnectionTime time.Time
	Status         string
	Rooms          map[string]bool
}

// Room represents a group of connected clients
type Room struct {
	ID      string
	Clients map[string]*Client
}

// SocketConfig is used to configure the socket manager
type SocketConfig struct {
	PingInterval   time.Duration
	PingTimeout    time.Duration
	WriteTimeout   time.Duration
	ReadTimeout    time.Duration
	MaxMessageSize int64
}

// DefaultSocketConfig returns the default socket configuration
func DefaultSocketConfig() SocketConfig {
	return SocketConfig{
		PingInterval:   25 * time.Second,
		PingTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		ReadTimeout:    60 * time.Second,
		MaxMessageSize: 1024 * 1024, // 1MB
	}
}

// InboundMessage represents a message received from a client
type InboundMessage struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

// OutboundMessage represents a message sent to a client
type OutboundMessage struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

// SocketError represents a socket error
type SocketError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
