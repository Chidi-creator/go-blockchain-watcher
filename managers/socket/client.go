package socket

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
)

// NewClient creates a new WebSocket client
func NewClient(socket *websocket.Conn, manager *SocketManager, userID string) *Client {
	return &Client{
		ID:             uuid.New().String(),
		UserID:         userID,
		Socket:         socket,
		Send:           make(chan []byte, 256),
		Manager:        manager,
		LastPong:       time.Now(),
		ConnectionTime: time.Now(),
		Status:         StatusConnected,
		Rooms:          make(map[string]bool),
	}
}

// ReadPump processes incoming messages from the client
func (c *Client) ReadPump() {
	defer func() {
		c.Manager.Unregister(c)
		c.Socket.Close()
		close(c.Send)
	}()

	c.Socket.SetReadLimit(c.Manager.Config.MaxMessageSize)
	c.Socket.SetReadDeadline(time.Now().Add(c.Manager.Config.ReadTimeout))
	c.Socket.SetPongHandler(func(string) error {
		c.LastPong = time.Now()
		c.Socket.SetReadDeadline(time.Now().Add(c.Manager.Config.ReadTimeout))
		return nil
	})

	for {
		_, message, err := c.Socket.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Get().Error("Socket read error", "error", err.Error(), "clientID", c.ID)
			}
			break
		}

		// Parse the message to determine the event type
		var inboundMsg InboundMessage
		if err := json.Unmarshal(message, &inboundMsg); err != nil {
			logger.Get().Error("Failed to parse socket message", "error", err.Error(), "clientID", c.ID)
			continue
		}

		// Handle the message based on the event type
		c.Manager.ProcessMessage(c, inboundMsg.Event, message)
	}
}

// WritePump sends messages to the client
func (c *Client) WritePump() {
	ticker := time.NewTicker(c.Manager.Config.PingInterval)
	defer func() {
		ticker.Stop()
		c.Socket.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Socket.SetWriteDeadline(time.Now().Add(c.Manager.Config.WriteTimeout))
			if !ok {
				// The channel has been closed
				c.Socket.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			writer, err := c.Socket.NextWriter(websocket.TextMessage)
			if err != nil {
				logger.Get().Error("Failed to get next writer", "error", err.Error(), "clientID", c.ID)
				return
			}

			writer.Write(message)

			// Flush any queued messages
			n := len(c.Send)
			for i := 0; i < n; i++ {
				writer.Write([]byte{'\n'})
				writer.Write(<-c.Send)
			}

			if err := writer.Close(); err != nil {
				logger.Get().Error("Failed to close writer", "error", err.Error(), "clientID", c.ID)
				return
			}
		case <-ticker.C:
			c.Socket.SetWriteDeadline(time.Now().Add(c.Manager.Config.WriteTimeout))
			if err := c.Socket.WriteMessage(websocket.PingMessage, nil); err != nil {
				logger.Get().Error("Failed to write ping message", "error", err.Error(), "clientID", c.ID)
				return
			}

			// Check for stale connections
			if time.Since(c.LastPong) > c.Manager.Config.PingTimeout*2 {
				logger.Get().Warn("Client connection is stale, disconnecting", "clientID", c.ID)
				return
			}
		}
	}
}

// Send a message to the client
func (c *Client) SendEvent(eventName string, data interface{}) error {
	outboundMsg := OutboundMessage{
		Event: eventName,
		Data:  data,
	}

	msgBytes, err := json.Marshal(outboundMsg)
	if err != nil {
		logger.Get().Error("Failed to marshal outbound message", "error", err.Error())
		return err
	}

	select {
	case c.Send <- msgBytes:
		return nil
	default:
		logger.Get().Error("Client send buffer is full", "clientID", c.ID)
		return c.Manager.Unregister(c)
	}
}

// JoinRoom adds the client to a room
func (c *Client) JoinRoom(roomID string) {
	c.Manager.clientJoinRoom(c, roomID)
}

// LeaveRoom removes the client from a room
func (c *Client) LeaveRoom(roomID string) {
	c.Manager.clientLeaveRoom(c, roomID)
}
