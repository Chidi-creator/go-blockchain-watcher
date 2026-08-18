# Socket Manager

The Socket Manager provides real-time bidirectional communication capabilities for the wallet service, using WebSockets.

## Features

- WebSocket server for client connections
- User-based message targeting
- Room-based communication
- Connection to Node.js Socket.IO servers
- Event-driven architecture
- Automatic ping/pong for connection health monitoring
- Connection status tracking

## Setup

### Initialization

Initialize the socket manager in your application's startup code:

```go
// Initialize socket manager with default settings
socket.Initialize()

// Or with custom configuration
config := socket.DefaultSocketConfig()
config.PingInterval = 20 * time.Second
config.MaxMessageSize = 2048

socketManager := socket.GetInstance()
socketManager.Initialize(config, myAuthFunc)
```

### Adding WebSocket endpoint

```go
// In your HTTP router
r.GET("/ws", func(c *gin.Context) {
    socketManager := socket.Get()
    socketManager.HandleWebSocket(c.Writer, c.Request)
})
```

### Environment Variables

Configure the following environment variables:

```
NODE_SERVER_ENABLED=true
NODE_SERVER_URL=ws://your-node-server:3000/socket.io
NODE_SERVER_AUTH_TOKEN=your-auth-token
NODE_SERVER_USER_ID=go-server
```

## Usage Examples

### Connecting to a Node.js server

```go
nodeConn, err := socket.ConnectToNodeServer("ws://node-server:3000/socket.io", "auth-token", "user-123")
if err != nil {
    // Handle error
}
```

### Sending messages to clients

```go
// Send to a specific user (all their connected clients)
socketManager.EmitToUser("user-123", "balance_update", updateData)

// Send to a room
socketManager.EmitToRoom("wallet-456", "transaction_update", txData)

// Broadcast to all connected clients
socketManager.EmitToAll("system_announcement", announcement)
```

### Handling client messages

```go
// Register an event handler
socketManager.AddEventHandler("join_wallet_room", func(client *socket.Client, data []byte) {
    var join struct {
        WalletID string `json:"wallet_id"`
    }
    if err := json.Unmarshal(data, &join); err != nil {
        // Handle error
        return
    }

    // Handle the event
    roomID := fmt.Sprintf("wallet-%s", join.WalletID)
    client.JoinRoom(roomID)
})
```

## Client Example

Here's a simple JavaScript client:

```javascript
const socket = new WebSocket("ws://localhost:8080/ws?userId=user-123");

socket.onopen = function (e) {
  console.log("Connection established");

  // Send a message
  socket.send(
    JSON.stringify({
      event: "join_wallet_room",
      data: {
        wallet_id: "wallet-456",
      },
    })
  );
};

socket.onmessage = function (event) {
  const message = JSON.parse(event.data);
  console.log("Message from server:", message);

  // Handle specific events
  if (message.event === "balance_update") {
    updateBalanceUI(message.data);
  }
};

socket.onclose = function (event) {
  if (event.wasClean) {
    console.log(
      `Connection closed cleanly, code=${event.code} reason=${event.reason}`
    );
  } else {
    console.log("Connection died");
  }
};

socket.onerror = function (error) {
  console.log("WebSocket Error:", error);
};
```

## Architecture

The Socket Manager uses the following components:

1. **SocketManager**: Core singleton component that manages clients, rooms, and events
2. **Client**: Represents a connected WebSocket client
3. **Room**: Represents a group of connected clients for targeted messaging
4. **NodeSocketConnection**: Represents a connection to a Node.js Socket.IO server

## Message Format

### Inbound Messages (from client to server)

```json
{
  "event": "event_name",
  "data": {
    "key1": "value1",
    "key2": "value2"
  }
}
```

### Outbound Messages (from server to client)

```json
{
  "event": "event_name",
  "data": {
    "key1": "value1",
    "key2": "value2"
  }
}
```

## Socket.IO Compatibility

This implementation is compatible with Socket.IO clients, including Postman. To work with Socket.IO:

### Socket.IO Protocol

This implementation supports the Socket.IO v4 protocol with the following packet types:

- `0`: Connect/handshake
- `1`: Disconnect
- `2`: Ping
- `3`: Pong
- `4`: Message
- `40`: Connection packet, namespace is the empty string (default)
- `42`: Event packet, followed by JSON event data

Socket.IO message format examples:

- Handshake: `0{"sid":"tM5Pod3kU2G2j0bFAAAQ","upgrades":[],"pingInterval":25000,"pingTimeout":10000}`
- Ping: `2`
- Pong: `3`
- Event: `42["event_name",{"data":"value"}]`

### Connecting from Postman

1. Use the Postman WebSocket interface and connect to your Socket.IO server using the following URL format:
   ```
   ws://your-server:3000/socket.io/?EIO=4&transport=websocket
   ```
2. Add the following headers if needed:

   - `Authorization: Bearer your-token`
   - `User-ID: your-user-id`

3. For emitting events, use the following message format:

   ```json
   {
     "event": "your_event_name",
     "data": {
       "key1": "value1",
       "key2": "value2"
     }
   }
   ```

   Alternatively, for raw protocol messages, use:

   ```
   42["your_event_name",{"key1":"value1","key2":"value2"}]
   ```

### Troubleshooting Socket.IO Connections

If you experience connection issues:

1. Check server logs for Socket.IO handshake errors
2. Use the `DebugSocketIOConnection()` method to diagnose connection issues:
   ```go
   nodeConn.DebugSocketIOConnection()
   ```
3. Send raw protocol messages for testing:

   ```go
   // Send a ping
   nodeConn.SendRawSocketIOMessage("2", "")

   // Connect to namespace
   nodeConn.SendRawSocketIOMessage("40/admin", "")

   // Send event with namespace
   nodeConn.SendRawSocketIOMessage("42/admin", "[\"status\",{}]")
   ```

### Using the Go Socket.IO Client

The `NodeSocketConnection` in this package provides a Socket.IO client implementation. Example:

```go
// Initialize the connection
socketManager := socket.GetInstance()
nodeConn, err := socketManager.ConnectToNodeServer(
    "ws://your-server:3000",
    "your-auth-token",
    "your-user-id"
)
if err != nil {
    // Handle error
}

// Send an event
nodeConn.EmitEvent("my_event", map[string]interface{}{
    "message": "Hello from Go",
    "timestamp": time.Now().Unix(),
})

// Listen for events
nodeConn.AddEventHandler("server_event", func(data []byte) {
    // Process event data
    fmt.Println("Received event:", string(data))
})
```

See `examples/socket_example/socket_io_usage.go` for a complete example.
