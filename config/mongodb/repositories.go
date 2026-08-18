package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"bitbucket.org/zapspace/zap-go-server/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Collection names
const (
	WalletCollection            = "wallets"
	AccountCollection           = "accounts"
	CurrencyCollection          = "currencies"
	ChainCollection             = "chains"
	SupportedCurrencyCollection = "supportedcurrencies"
	OrderCollection             = "orders"
)

// Repository errors
var (
	ErrNotFound     = errors.New("document not found")
	ErrInvalidID    = errors.New("invalid ID")
	ErrDuplicateKey = errors.New("duplicate key error")
)

// WalletRepository defines the interface for wallet operations
type WalletRepository interface {
	GetWalletByID(ctx context.Context, id primitive.ObjectID) (*models.Wallet, error)
	GetWalletsByUserId(ctx context.Context, userId primitive.ObjectID) ([]models.Wallet, error)
	CreateWallet(ctx context.Context, wallet *models.Wallet) (*models.Wallet, error)
	UpdateWallet(ctx context.Context, wallet *models.Wallet) error
	DeleteWallet(ctx context.Context, id primitive.ObjectID) error
}

// AccountRepository defines the interface for account operations
type AccountRepository interface {
	GetAccountByID(ctx context.Context, id primitive.ObjectID) (*models.Account, error)
	GetAccountsByWalletID(ctx context.Context, walletID primitive.ObjectID) ([]models.Account, error)
	GetAccountsByUserID(ctx context.Context, userID primitive.ObjectID) ([]models.Account, error)
	GetAccountByAddress(ctx context.Context, address string) (*models.Account, error)
	CountByAddress(ctx context.Context, address string) (int64, error)
	CheckDuplicateAccount(ctx context.Context, walletAddress string, userID, chainID, currencyID primitive.ObjectID) (bool, error)
	CreateAccount(ctx context.Context, account *models.Account) (*models.Account, error)
	UpdateAccount(ctx context.Context, account *models.Account) error
	DeleteAccount(ctx context.Context, id primitive.ObjectID) error
}

// CurrencyRepository defines the interface for currency operations
type CurrencyRepository interface {
	GetCurrencyByID(ctx context.Context, id primitive.ObjectID) (*models.Currency, error)
	GetCurrencyBySymbol(ctx context.Context, symbol string) (*models.Currency, error)
	GetCurrencies(ctx context.Context) ([]models.Currency, error)
}

// ChainRepository defines the interface for chain operations
type ChainRepository interface {
	GetChainByID(ctx context.Context, id primitive.ObjectID) (*models.Chain, error)
	GetChainBySymbol(ctx context.Context, symbol string) (*models.Chain, error)
	GetChains(ctx context.Context) ([]models.Chain, error)
}

// SupportedCurrencyRepository defines the interface for supported currency operations
type SupportedCurrencyRepository interface {
	GetSupportedCurrencyByID(ctx context.Context, id primitive.ObjectID) (*models.SupportedCurrency, error)
	GetSupportedCurrenciesByChainID(ctx context.Context, chainID primitive.ObjectID) ([]models.SupportedCurrency, error)
	GetSupportedCurrencies(ctx context.Context) ([]models.SupportedCurrency, error)
	GetSupportedCurrencyBySymbol(ctx context.Context, symbol string) (*models.SupportedCurrency, error)
}

// OrderRepository defines the interface for order operations
type OrderRepository interface {
	GetOrderByID(ctx context.Context, id primitive.ObjectID) (*models.Order, error)
	GetOrdersByUserID(ctx context.Context, userID primitive.ObjectID) ([]models.Order, error)
	GetOrderByDepositAddress(ctx context.Context, depositAddress string) (*models.Order, error)
	CreateOrder(ctx context.Context, order *models.Order) (*models.Order, error)
	UpdateOrder(ctx context.Context, order *models.Order) error
	DeleteOrder(ctx context.Context, id primitive.ObjectID) error
}

// walletRepository implements WalletRepository
type walletRepository struct {
	client *mongo.Client
	dbName string
}

// NewWalletRepository creates a new wallet repository
func NewWalletRepository(client *mongo.Client, dbName string) WalletRepository {
	return &walletRepository{
		client: client,
		dbName: dbName,
	}
}

// GetWalletByID retrieves a wallet by its ID
func (r *walletRepository) GetWalletByID(ctx context.Context, id primitive.ObjectID) (*models.Wallet, error) {
	// Create a new context with a longer timeout to avoid cancellation issues
	queryCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collection := r.client.Database(r.dbName).Collection(WalletCollection)

	var wallet models.Wallet
	filter := bson.M{"_id": id}

	// Use the new context instead of the potentially problematic passed context
	err := collection.FindOne(queryCtx, filter).Decode(&wallet)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Printf("Wallet not found with ID: %s\n", id.Hex())
			return nil, ErrNotFound
		}
		fmt.Printf("Database error finding wallet: %v\n", err)
		return nil, err
	}

	return &wallet, nil
}

// GetWalletsByUserId retrieves all wallets for a user
func (r *walletRepository) GetWalletsByUserId(ctx context.Context, userId primitive.ObjectID) ([]models.Wallet, error) {
	collection := r.client.Database(r.dbName).Collection(WalletCollection)

	cursor, err := collection.Find(ctx, bson.M{"userId": userId, "isDeleted": false})
	if err != nil {
		return nil, err
	}

	var wallets []models.Wallet
	if err = cursor.All(ctx, &wallets); err != nil {
		return nil, err
	}

	return wallets, nil
}

// CreateWallet creates a new wallet
func (r *walletRepository) CreateWallet(ctx context.Context, wallet *models.Wallet) (*models.Wallet, error) {
	collection := r.client.Database(r.dbName).Collection(WalletCollection)

	now := time.Now()
	wallet.CreatedAt = now
	wallet.UpdatedAt = now

	if wallet.ID.IsZero() {
		wallet.ID = primitive.NewObjectID()
	}

	_, err := collection.InsertOne(ctx, wallet)
	if err != nil {
		// Check for duplicate key error
		if mongo.IsDuplicateKeyError(err) {
			return nil, ErrDuplicateKey
		}
		return nil, err
	}

	return wallet, nil
}

// UpdateWallet updates an existing wallet
func (r *walletRepository) UpdateWallet(ctx context.Context, wallet *models.Wallet) error {
	collection := r.client.Database(r.dbName).Collection(WalletCollection)

	if wallet.ID.IsZero() {
		return ErrInvalidID
	}

	wallet.UpdatedAt = time.Now()

	filter := bson.M{"_id": wallet.ID}
	update := bson.M{"$set": wallet}

	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	if result.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}

// DeleteWallet soft deletes a wallet
func (r *walletRepository) DeleteWallet(ctx context.Context, id primitive.ObjectID) error {
	collection := r.client.Database(r.dbName).Collection(WalletCollection)

	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"isDeleted": true,
			"deletedAt": now,
			"updatedAt": now,
		},
	}

	result, err := collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return err
	}

	if result.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}

// accountRepository implements AccountRepository
type accountRepository struct {
	client *mongo.Client
	dbName string
}

// NewAccountRepository creates a new account repository
func NewAccountRepository(client *mongo.Client, dbName string) AccountRepository {
	return &accountRepository{
		client: client,
		dbName: dbName,
	}
}

// GetAccountByID retrieves an account by its ID
func (r *accountRepository) GetAccountByID(ctx context.Context, id primitive.ObjectID) (*models.Account, error) {
	collection := r.client.Database(r.dbName).Collection(AccountCollection)

	var account models.Account
	err := collection.FindOne(ctx, bson.M{"_id": id, "deletedAt": nil}).Decode(&account)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &account, nil
}

// GetAccountsByWalletID retrieves all accounts for a wallet
func (r *accountRepository) GetAccountsByWalletID(ctx context.Context, walletID primitive.ObjectID) ([]models.Account, error) {
	collection := r.client.Database(r.dbName).Collection(AccountCollection)

	cursor, err := collection.Find(ctx, bson.M{"walletId": walletID, "deletedAt": nil})
	if err != nil {
		return nil, err
	}

	var accounts []models.Account
	if err = cursor.All(ctx, &accounts); err != nil {
		return nil, err
	}

	return accounts, nil
}

// GetAccountsByUserID retrieves all accounts for a user
func (r *accountRepository) GetAccountsByUserID(ctx context.Context, userID primitive.ObjectID) ([]models.Account, error) {
	collection := r.client.Database(r.dbName).Collection(AccountCollection)

	cursor, err := collection.Find(ctx, bson.M{"userId": userID, "deletedAt": nil})
	if err != nil {
		return nil, err
	}

	var accounts []models.Account
	if err = cursor.All(ctx, &accounts); err != nil {
		return nil, err
	}

	return accounts, nil
}

// GetAccountByAddress retrieves an account by its wallet address
func (r *accountRepository) GetAccountByAddress(ctx context.Context, address string) (*models.Account, error) {
	collection := r.client.Database(r.dbName).Collection(AccountCollection)

	var account models.Account
	err := collection.FindOne(ctx, bson.M{"walletAddress": address, "deletedAt": nil}).Decode(&account)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &account, nil
}

// CountByAddress counts the number of accounts by address
func (r *accountRepository) CountByAddress(ctx context.Context, address string) (int64, error) {
	collection := r.client.Database(r.dbName).Collection(AccountCollection)

	count, err := collection.CountDocuments(ctx, bson.M{"walletAddress": address, "deletedAt": nil})
	if err != nil {
		return 0, err
	}

	return count, nil
}

// CheckDuplicateAccount checks if an account already exists for a given wallet address and user
func (r *accountRepository) CheckDuplicateAccount(ctx context.Context, walletAddress string, userID, chainID, currencyID primitive.ObjectID) (bool, error) {
	collection := r.client.Database(r.dbName).Collection(AccountCollection)

	filter := bson.M{
		"walletAddress": walletAddress,
		"userId":        userID,
		"chainId":       chainID,
		"currencyId":    currencyID,
		"deletedAt":     nil,
	}

	count, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// CreateAccount creates a new account
func (r *accountRepository) CreateAccount(ctx context.Context, account *models.Account) (*models.Account, error) {
	collection := r.client.Database(r.dbName).Collection(AccountCollection)

	now := time.Now()
	account.CreatedAt = now
	account.UpdatedAt = now

	if account.ID.IsZero() {
		account.ID = primitive.NewObjectID()
	}

	fmt.Printf("Creating account with currency: %v, userId: %s, walletId: %s, accountId: %s, address: %s, balance: %v\n",
		account.CurrencyID,
		account.UserID.Hex(),
		account.WalletID.Hex(),
		account.ID.Hex(),
		account.WalletAddress,
		account.Balance)

	_, err := collection.InsertOne(ctx, account)
	if err != nil {
		// Check for duplicate key error
		if mongo.IsDuplicateKeyError(err) {
			return nil, ErrDuplicateKey
		}
		return nil, err
	}

	return account, nil
}

// UpdateAccount updates an existing account
func (r *accountRepository) UpdateAccount(ctx context.Context, account *models.Account) error {
	collection := r.client.Database(r.dbName).Collection(AccountCollection)

	if account.ID.IsZero() {
		return ErrInvalidID
	}

	account.UpdatedAt = time.Now()

	filter := bson.M{"_id": account.ID}
	update := bson.M{"$set": account}

	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	if result.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}

// DeleteAccount soft deletes an account
func (r *accountRepository) DeleteAccount(ctx context.Context, id primitive.ObjectID) error {
	collection := r.client.Database(r.dbName).Collection(AccountCollection)

	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"deletedAt": now,
			"updatedAt": now,
		},
	}

	result, err := collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return err
	}

	if result.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}

// currencyRepository implements CurrencyRepository
type currencyRepository struct {
	client *mongo.Client
	dbName string
}

// NewCurrencyRepository creates a new currency repository
func NewCurrencyRepository(client *mongo.Client, dbName string) CurrencyRepository {
	return &currencyRepository{
		client: client,
		dbName: dbName,
	}
}

// GetCurrencyByID retrieves a currency by its ID
func (r *currencyRepository) GetCurrencyByID(ctx context.Context, id primitive.ObjectID) (*models.Currency, error) {
	collection := r.client.Database(r.dbName).Collection(CurrencyCollection)

	var currency models.Currency
	err := collection.FindOne(ctx, bson.M{"_id": id}).Decode(&currency)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &currency, nil
}

// GetCurrencyBySymbol retrieves a currency by its symbol
func (r *currencyRepository) GetCurrencyBySymbol(ctx context.Context, symbol string) (*models.Currency, error) {
	collection := r.client.Database(r.dbName).Collection(CurrencyCollection)

	var currency models.Currency
	err := collection.FindOne(ctx, bson.M{"symbol": symbol}).Decode(&currency)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &currency, nil
}

// GetCurrencies retrieves all currencies
func (r *currencyRepository) GetCurrencies(ctx context.Context) ([]models.Currency, error) {
	collection := r.client.Database(r.dbName).Collection(CurrencyCollection)

	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	var currencies []models.Currency
	if err = cursor.All(ctx, &currencies); err != nil {
		return nil, err
	}

	return currencies, nil
}

// chainRepository implements ChainRepository
type chainRepository struct {
	client *mongo.Client
	dbName string
}

// NewChainRepository creates a new chain repository
func NewChainRepository(client *mongo.Client, dbName string) ChainRepository {
	return &chainRepository{
		client: client,
		dbName: dbName,
	}
}

// GetChainByID retrieves a chain by its ID
func (r *chainRepository) GetChainByID(ctx context.Context, id primitive.ObjectID) (*models.Chain, error) {
	collection := r.client.Database(r.dbName).Collection(ChainCollection)

	var chain models.Chain
	err := collection.FindOne(ctx, bson.M{"_id": id}).Decode(&chain)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &chain, nil
}

// GetChainBySymbol retrieves a chain by its symbol
func (r *chainRepository) GetChainBySymbol(ctx context.Context, symbol string) (*models.Chain, error) {
	collection := r.client.Database(r.dbName).Collection(ChainCollection)

	var chain models.Chain
	err := collection.FindOne(ctx, bson.M{"symbol": symbol}).Decode(&chain)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &chain, nil
}

// GetChains retrieves all chains
func (r *chainRepository) GetChains(ctx context.Context) ([]models.Chain, error) {
	collection := r.client.Database(r.dbName).Collection(ChainCollection)

	opts := options.Find().SetSort(bson.D{{Key: "name", Value: 1}})
	cursor, err := collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}

	var chains []models.Chain
	if err = cursor.All(ctx, &chains); err != nil {
		return nil, err
	}

	return chains, nil
}

// supportedCurrencyRepository implements SupportedCurrencyRepository
type supportedCurrencyRepository struct {
	client *mongo.Client
	dbName string
}

// NewSupportedCurrencyRepository creates a new supported currency repository
func NewSupportedCurrencyRepository(client *mongo.Client, dbName string) SupportedCurrencyRepository {
	return &supportedCurrencyRepository{
		client: client,
		dbName: dbName,
	}
}

// GetSupportedCurrencyByID retrieves a supported currency by its ID
func (r *supportedCurrencyRepository) GetSupportedCurrencyByID(ctx context.Context, id primitive.ObjectID) (*models.SupportedCurrency, error) {
	collection := r.client.Database(r.dbName).Collection(SupportedCurrencyCollection)

	var supportedCurrency models.SupportedCurrency
	err := collection.FindOne(ctx, bson.M{"_id": id}).Decode(&supportedCurrency)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &supportedCurrency, nil
}

// GetSupportedCurrenciesByChainID retrieves all supported currencies for a chain
func (r *supportedCurrencyRepository) GetSupportedCurrenciesByChainID(ctx context.Context, chainID primitive.ObjectID) ([]models.SupportedCurrency, error) {
	collection := r.client.Database(r.dbName).Collection(SupportedCurrencyCollection)

	cursor, err := collection.Find(ctx, bson.M{"chainId": chainID})
	if err != nil {
		return nil, err
	}

	var supportedCurrencies []models.SupportedCurrency
	if err = cursor.All(ctx, &supportedCurrencies); err != nil {
		return nil, err
	}

	return supportedCurrencies, nil
}

// GetSupportedCurrencies retrieves all supported currencies
func (r *supportedCurrencyRepository) GetSupportedCurrencies(ctx context.Context) ([]models.SupportedCurrency, error) {
	collection := r.client.Database(r.dbName).Collection(SupportedCurrencyCollection)

	opts := options.Find().SetSort(bson.D{{Key: "createdAt", Value: -1}})
	cursor, err := collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}

	var supportedCurrencies []models.SupportedCurrency
	if err = cursor.All(ctx, &supportedCurrencies); err != nil {
		return nil, err
	}

	return supportedCurrencies, nil
}

// GetSupportedCurrencyBySymbol retrieves a supported currency by its symbol
func (r *supportedCurrencyRepository) GetSupportedCurrencyBySymbol(ctx context.Context, symbol string) (*models.SupportedCurrency, error) {
	collection := r.client.Database(r.dbName).Collection(SupportedCurrencyCollection)

	var supportedCurrency models.SupportedCurrency
	err := collection.FindOne(ctx, bson.M{"symbol": symbol}).Decode(&supportedCurrency)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &supportedCurrency, nil
}

// orderRepository implements OrderRepository
type orderRepository struct {
	client *mongo.Client
	dbName string
}

// NewOrderRepository creates a new order repository
func NewOrderRepository(client *mongo.Client, dbName string) OrderRepository {
	return &orderRepository{
		client: client,
		dbName: dbName,
	}
}

// GetOrderByID retrieves an order by its ID
func (r *orderRepository) GetOrderByID(ctx context.Context, id primitive.ObjectID) (*models.Order, error) {
	collection := r.client.Database(r.dbName).Collection(OrderCollection)

	var order models.Order
	err := collection.FindOne(ctx, bson.M{"_id": id, "deletedAt": nil}).Decode(&order)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &order, nil
}

// GetOrdersByUserID retrieves orders by user ID
func (r *orderRepository) GetOrdersByUserID(ctx context.Context, userID primitive.ObjectID) ([]models.Order, error) {
	collection := r.client.Database(r.dbName).Collection(OrderCollection)

	cursor, err := collection.Find(ctx, bson.M{"userId": userID, "deletedAt": nil})
	if err != nil {
		return nil, err
	}

	var orders []models.Order
	if err = cursor.All(ctx, &orders); err != nil {
		return nil, err
	}

	return orders, nil
}

// GetOrderByDepositAddress retrieves an order by deposit address
func (r *orderRepository) GetOrderByDepositAddress(ctx context.Context, depositAddress string) (*models.Order, error) {
	// First, find the account with the deposit address
	accountCollection := r.client.Database(r.dbName).Collection(AccountCollection)
	var account models.Account
	err := accountCollection.FindOne(ctx, bson.M{"walletAddress": depositAddress}).Decode(&account)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	// Then find the order with this account ID in deposit accounts
	collection := r.client.Database(r.dbName).Collection(OrderCollection)
	var order models.Order
	err = collection.FindOne(ctx, bson.M{
		"depositAccountIds": account.ID,
		"deletedAt":         nil,
	}).Decode(&order)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &order, nil
}

// CreateOrder creates a new order
func (r *orderRepository) CreateOrder(ctx context.Context, order *models.Order) (*models.Order, error) {
	collection := r.client.Database(r.dbName).Collection(OrderCollection)

	now := time.Now()
	order.CreatedAt = now
	order.UpdatedAt = now

	if order.ID.IsZero() {
		order.ID = primitive.NewObjectID()
	}

	_, err := collection.InsertOne(ctx, order)
	if err != nil {
		// Check for duplicate key error
		if mongo.IsDuplicateKeyError(err) {
			return nil, ErrDuplicateKey
		}
		return nil, err
	}

	return order, nil
}

// UpdateOrder updates an existing order
func (r *orderRepository) UpdateOrder(ctx context.Context, order *models.Order) error {
	collection := r.client.Database(r.dbName).Collection(OrderCollection)

	if order.ID.IsZero() {
		return ErrInvalidID
	}

	order.UpdatedAt = time.Now()

	filter := bson.M{"_id": order.ID}
	update := bson.M{"$set": order}

	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	if result.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}

// DeleteOrder soft deletes an order
func (r *orderRepository) DeleteOrder(ctx context.Context, id primitive.ObjectID) error {
	collection := r.client.Database(r.dbName).Collection(OrderCollection)

	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"deletedAt": now,
			"updatedAt": now,
		},
	}

	result, err := collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return err
	}

	if result.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}
