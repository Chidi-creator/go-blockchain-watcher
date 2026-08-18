package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Client wraps the mongo.Client
type Client struct {
	*mongo.Client
}

// NewClient creates a new MongoDB client wrapper
func NewClient(uri, dbName string) (*Client, error) {
	// Use the URI directly from the environment
	connectionURI := uri

	// If URI is empty, fall back to a default
	if connectionURI == "" {
		return nil, fmt.Errorf("MongoDB URI is required")
	}

	// Increase the context timeout for initial connection
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Set client options with better timeout values
	clientOptions := options.Client().
		ApplyURI(connectionURI).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(15 * time.Second).
		SetMaxPoolSize(100).
		SetRetryWrites(true).
		SetRetryReads(true)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create MongoDB client: %w", err)
	}

	// Create a separate context for ping to avoid timeout issues
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer pingCancel()

	// Ping the database to verify connection
	if err = client.Ping(pingCtx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return &Client{Client: client}, nil
}

// Disconnect closes the client connection
func (c *Client) Disconnect(ctx context.Context) error {
	return c.Client.Disconnect(ctx)
}

// GetDatabase returns a database instance
func GetDatabase(client *mongo.Client, dbName string) *mongo.Database {
	return client.Database(dbName)
}

// GetCollection returns a collection from the database
func GetCollection(client *mongo.Client, dbName, collectionName string) *mongo.Collection {
	return client.Database(dbName).Collection(collectionName)
}

// VerifyCollectionExists checks if a collection exists in the database
func VerifyCollectionExists(client *mongo.Client, dbName, collectionName string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collections, err := client.Database(dbName).ListCollectionNames(ctx, map[string]interface{}{
		"name": collectionName,
	})

	if err != nil {
		return false, fmt.Errorf("error checking collections: %w", err)
	}

	for _, collection := range collections {
		if collection == collectionName {
			return true, nil
		}
	}

	return false, nil
}
