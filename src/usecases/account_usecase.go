package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	httpmanager "bitbucket.org/zapspace/zap-go-server/managers/http"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/models"
	"bitbucket.org/zapspace/zap-go-server/src/repositories"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// AccountUseCase handles account related business logic
type AccountUseCase struct {
	accountRepo           repositories.AccountRepository
	walletRepo            repositories.WalletRepository
	currencyRepo          repositories.CurrencyRepository
	supportedCurrencyRepo repositories.SupportedCurrencyRepository
	chainRepo             repositories.ChainRepository
	blockchainService     *services.BlockchainService
	logger                logger.Logger
	requestManager        *httpmanager.RequestManager // For making HTTP requests to Node.js server
}

// NewAccountUseCase creates a new account use case
func NewAccountUseCase(
	accountRepo repositories.AccountRepository,
	walletRepo repositories.WalletRepository,
	currencyRepo repositories.CurrencyRepository,
	supportedCurrencyRepo repositories.SupportedCurrencyRepository,
	chainRepo repositories.ChainRepository,
	blockchainService *services.BlockchainService,
	logger logger.Logger,
	requestManager *httpmanager.RequestManager,
) *AccountUseCase {
	return &AccountUseCase{
		accountRepo:           accountRepo,
		walletRepo:            walletRepo,
		currencyRepo:          currencyRepo,
		supportedCurrencyRepo: supportedCurrencyRepo,
		chainRepo:             chainRepo,
		blockchainService:     blockchainService,
		logger:                logger,
		// nodeAdapter:           nodeAdapter,
		requestManager: requestManager,
	}
}

// CreateAccount creates a new account
func (uc *AccountUseCase) CreateAccount(
	ctx context.Context,
	walletID, userID, currencyID, chainID primitive.ObjectID,
	walletAddress, encryptedPrivateKey string,
) (*types.Account, error) {
	// Validate wallet exists
	wallet, err := uc.walletRepo.GetByID(ctx, walletID)
	if err != nil {
		uc.logger.Error("Failed to get wallet for account creation", "walletId", walletID.Hex(), "error", err)
		return nil, err
	}

	if wallet == nil {
		return nil, ErrWalletNotFound
	}

	// Validate currency exists
	currency, err := uc.currencyRepo.GetByID(ctx, currencyID)
	if err != nil {
		uc.logger.Error("Failed to get currency for account creation", "currencyId", currencyID.Hex(), "error", err)
		return nil, err
	}

	if currency == nil {
		return nil, ErrCurrencyNotFound
	}

	// Validate chain exists if provided
	if !chainID.IsZero() {
		chain, err := uc.chainRepo.GetByID(ctx, chainID)
		if err != nil {
			uc.logger.Error("Failed to get chain for account creation", "chainId", chainID.Hex(), "error", err)
			return nil, err
		}

		if chain == nil {
			return nil, ErrChainNotFound
		}
	}

	// Create new account
	account := types.NewAccount(walletID, userID, currencyID, chainID, walletAddress, encryptedPrivateKey)

	// Save account
	err = uc.accountRepo.Create(ctx, account)
	if err != nil {
		uc.logger.Error("Failed to create account", "error", err)
		return nil, err
	}

	return account, nil
}

func (uc *AccountUseCase) CreateOrFindAccount(ctx context.Context, userId primitive.ObjectID, chainId primitive.ObjectID, walletAddress string) (*types.Account, error) {
	// First try to find existing account
	count, err := uc.accountRepo.CountByAddress(ctx, walletAddress)

	if err != nil {
		uc.logger.Error("Failed to count accounts by address", "error", err)
		return nil, fmt.Errorf("failed to count accounts by address: %w", err)
	}

	if count > 0 {
		uc.logger.Info("Account already exists, returning existing account",
			"walletAddress", walletAddress,
			"userId", userId.Hex(),
			"chainId", chainId.Hex())
		account, err := uc.accountRepo.GetByAddress(ctx, walletAddress)
		if err != nil {
			uc.logger.Error("Failed to get account by address", "error", err)
			return nil, fmt.Errorf("failed to get account: %w", err)
		}
		return account, nil
	}

	newAccount := types.NewAccount(primitive.NilObjectID, userId, primitive.NilObjectID, chainId, walletAddress, "")

	// Account not found, create a new one
	err = uc.accountRepo.Create(ctx, newAccount)
	if err != nil {
		uc.logger.Error("Failed to create account",
			"walletAddress", walletAddress,
			"error", err)
		return nil, fmt.Errorf("failed to create account: %w", err)
	}

	uc.logger.Info("Successfully created new account",
		"accountId", newAccount.ID.Hex(),
		"walletAddress", walletAddress)

	return newAccount, nil
}

// GetAccountByID retrieves an account by its ID
func (uc *AccountUseCase) GetAccountByID(ctx context.Context, accountID primitive.ObjectID) (*types.Account, error) {
	account, err := uc.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		uc.logger.Error("Failed to get account by ID", "accountId", accountID.Hex(), "error", err)
		return nil, err
	}

	if account == nil {
		return nil, ErrAccountNotFound
	}

	return account, nil
}

// GetAccountsByWalletID retrieves all accounts for a wallet
func (uc *AccountUseCase) GetAccountsByWalletID(ctx context.Context, walletID primitive.ObjectID) ([]*types.Account, error) {
	// Validate wallet exists
	wallet, err := uc.walletRepo.GetByID(ctx, walletID)
	if err != nil {
		uc.logger.Error("Failed to get wallet for accounts lookup", "walletId", walletID.Hex(), "error", err)
		return nil, err
	}

	if wallet == nil {
		return nil, ErrWalletNotFound
	}

	accounts, err := uc.accountRepo.GetByWalletID(ctx, walletID)
	if err != nil {
		uc.logger.Error("Failed to get accounts by wallet ID", "walletId", walletID.Hex(), "error", err)
		return nil, err
	}

	return accounts, nil
}

// GetAccountByAddress retrieves an account by its wallet address
func (uc *AccountUseCase) GetAccountByAddress(ctx context.Context, address string) (*types.Account, error) {
	if address == "" {
		return nil, errors.New("wallet address is required")
	}

	account, err := uc.accountRepo.GetByAddress(ctx, address)
	if err != nil {
		uc.logger.Error("Failed to get account by address", "address", address, "error", err)
		return nil, err
	}

	if account == nil {
		return nil, ErrAccountNotFound
	}

	return account, nil
}

// UpdateAccount updates an existing account
func (uc *AccountUseCase) UpdateAccount(ctx context.Context, account *types.Account) error {
	if account.ID.IsZero() {
		return errors.New("account ID is required")
	}

	// Check if account exists
	existingAccount, err := uc.accountRepo.GetByID(ctx, account.ID)
	if err != nil {
		uc.logger.Error("Failed to get account for update", "accountId", account.ID.Hex(), "error", err)
		return err
	}

	if existingAccount == nil {
		return ErrAccountNotFound
	}

	// Update account
	err = uc.accountRepo.Update(ctx, account)
	if err != nil {
		uc.logger.Error("Failed to update account", "accountId", account.ID.Hex(), "error", err)
		return err
	}

	return nil
}

// DeleteAccount soft deletes an account
func (uc *AccountUseCase) DeleteAccount(ctx context.Context, accountID primitive.ObjectID) error {
	account, err := uc.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		uc.logger.Error("Failed to get account for deletion", "accountId", accountID.Hex(), "error", err)
		return err
	}

	if account == nil {
		return ErrAccountNotFound
	}

	account.MarkAsDeleted()

	err = uc.accountRepo.Update(ctx, account)
	if err != nil {
		uc.logger.Error("Failed to mark account as deleted", "accountId", accountID.Hex(), "error", err)
		return err
	}

	return nil
}

// ImportAccounts imports multiple accounts in bulk and automatically adds missing supported currencies
func (uc *AccountUseCase) ImportAccounts(ctx context.Context, accounts []*types.Account) ([]primitive.ObjectID, error) {
	uc.logger.Info("Starting ImportAccounts", "accountCount", len(accounts))

	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			uc.logger.Error("Recovered from panic in ImportAccounts", "error", r)
		}
	}()

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		uc.logger.Error("Context already canceled at start of ImportAccounts", "error", ctx.Err())
		return nil, ctx.Err()
	default:
		uc.logger.Debug("Context is valid at start of ImportAccounts")
	}

	if len(accounts) == 0 {
		uc.logger.Warn("No accounts to import")
		return nil, errors.New("no accounts to import")
	}

	// Ensure we have a valid user ID from the first account
	if len(accounts) > 0 && accounts[0].UserID.IsZero() {
		uc.logger.Error("First account has no user ID")
		return nil, errors.New("first account has no user ID")
	}

	// Check if required repositories are initialized
	if uc.supportedCurrencyRepo == nil {
		uc.logger.Error("supportedCurrencyRepo is nil, cannot import accounts")
		return nil, errors.New("supportedCurrencyRepo is not initialized")
	}

	if uc.blockchainService == nil {
		uc.logger.Error("blockchainService is nil, cannot import accounts")
		return nil, errors.New("blockchainService is not initialized")
	}

	// Store all created account IDs
	createdIDs := make([]primitive.ObjectID, 0)

	// First, process the original accounts
	originalIDs, err := uc.processOriginalAccounts(ctx, accounts)
	if err != nil {
		uc.logger.Error("Error processing original accounts", "error", err)
		return originalIDs, err
	}

	createdIDs = append(createdIDs, originalIDs...)
	uc.logger.Info("Successfully imported original accounts", "count", len(originalIDs))

	// Create a map of existing account chain+currency combinations for efficient lookup
	existingAccountMap := make(map[string]*types.Account)
	for _, account := range accounts {
		if !account.ChainID.IsZero() && !account.CurrencyID.IsZero() {
			key := fmt.Sprintf("%s-%s", account.ChainID.Hex(), account.CurrencyID.Hex())
			existingAccountMap[key] = account
		}
	}

	// Process supported currencies in batches to avoid memory issues
	batchSize := 10 // Smaller batch size to limit memory usage
	processed := 0
	hasMore := true
	totalCreated := 0

	for hasMore {
		// Check if context is done
		select {
		case <-ctx.Done():
			uc.logger.Error("Context canceled during batch processing", "error", ctx.Err(),
				"processed", processed, "createdAccounts", totalCreated)
			return createdIDs, ctx.Err()
		default:
			// Continue processing
		}

		// Get next batch of supported currencies
		supportedCurrencies, err := uc.supportedCurrencyRepo.GetBatch(ctx, batchSize, processed)
		if err != nil {
			uc.logger.Error("Failed to get supported currencies batch", "error", err,
				"batchSize", batchSize, "skip", processed)
			return createdIDs, err
		}

		if len(supportedCurrencies) == 0 {
			hasMore = false
			break
		}

		processed += len(supportedCurrencies)
		uc.logger.Info("Processing batch of supported currencies",
			"batchSize", len(supportedCurrencies),
			"totalProcessed", processed)
		fmt.Println("Processing batch of supported currencies",
			"batchSize", len(supportedCurrencies),
			"totalProcessed", processed)

		// Find currencies that need to be added (not in the existing map)
		unaddedCurrencies := make([]*models.SupportedCurrency, 0)
		for _, currency := range supportedCurrencies {
			if currency == nil || currency.ChainID.IsZero() || currency.CurrencyID.IsZero() {
				continue
			}

			key := fmt.Sprintf("%s-%s", currency.ChainID.Hex(), currency.CurrencyID.Hex())
			if _, exists := existingAccountMap[key]; !exists {
				unaddedCurrencies = append(unaddedCurrencies, currency)
			}
		}

		uc.logger.Info("Unadded currencies in this batch", "count", len(unaddedCurrencies))

		// Process in smaller chunks to reduce memory pressure
		chunkSize := 5
		for chunkStart := 0; chunkStart < len(unaddedCurrencies); chunkStart += chunkSize {
			// Check if context is done
			select {
			case <-ctx.Done():
				uc.logger.Error("Context canceled during chunk processing", "error", ctx.Err(),
					"processedChunks", chunkStart/chunkSize)
				return createdIDs, ctx.Err()
			default:
				// Continue processing
			}

			chunkEnd := chunkStart + chunkSize
			if chunkEnd > len(unaddedCurrencies) {
				chunkEnd = len(unaddedCurrencies)
			}
			chunk := unaddedCurrencies[chunkStart:chunkEnd]

			uc.logger.Info("Processing chunk",
				"chunkNumber", chunkStart/chunkSize+1,
				"totalChunks", (len(unaddedCurrencies)+chunkSize-1)/chunkSize)

			// Process each currency in the current chunk
			for _, currency := range chunk {
				// Find a supported account with the same chain
				var supportedAccount *types.Account
				for _, acc := range accounts {
					if !acc.ChainID.IsZero() && acc.ChainID == currency.ChainID {
						supportedAccount = acc
						break
					}
				}

				if supportedAccount == nil {
					uc.logger.Warn("No matching supported account found",
						"currencyId", currency.CurrencyID.Hex(),
						"chainId", currency.ChainID.Hex())
					continue
				}

				// Get walletAddress from the supported account
				walletAddress := supportedAccount.WalletAddress
				uc.logger.Info("Fetching balance for address", "address", walletAddress)

				// Get currency details
				currencyObj, err := uc.currencyRepo.GetByID(ctx, currency.CurrencyID)
				if err != nil {
					uc.logger.Error("Failed to get currency details",
						"currencyId", currency.CurrencyID.Hex(), "error", err)
					continue
				}

				// Get chain details
				chainObj, err := uc.chainRepo.GetByID(ctx, currency.ChainID)
				if err != nil {
					uc.logger.Error("Failed to get chain details",
						"chainId", currency.ChainID.Hex(), "error", err)
					continue
				}

				// Get supportedCurrency and pass it to FetchAccountBalance
				supportedCurrency, err := uc.supportedCurrencyRepo.GetByID(ctx, currency.ID)
				if err != nil {
					uc.logger.Warn("Failed to get supportedCurrency details",
						"supportedCurrencyId", currency.ID.Hex(),
						"error", err)
					// Continue with nil if not found
				}

				// Initialize balance to 0
				var balance float64 = 0

				// Fetch balance from blockchain only if the account type is not ACCOUNT_CREATED
				if supportedAccount.Type != "ACCOUNT_CREATED" {
					// Fetch balance from blockchain
					fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					balance, err = uc.blockchainService.FetchAccountBalance(
						fetchCtx,
						walletAddress,
						currencyObj,
						chainObj,
						supportedCurrency,
					)
					cancel()

					if err != nil {
						uc.logger.Error("Failed to fetch balance",
							"error", err, "walletAddress", walletAddress)
						// Continue with zero balance
						balance = 0
					} else {
						uc.logger.Info("Successfully fetched balance",
							"walletAddress", walletAddress, "balance", balance)
					}
				} else {
					uc.logger.Info("Skipping balance fetch for ACCOUNT_CREATED",
						"walletAddress", walletAddress)
				}

				// Create a new account
				uc.logger.Info("Creating account",
					"currency", currencyObj.Symbol,
					"userId", supportedAccount.UserID.Hex(),
					"walletId", supportedAccount.WalletID.Hex(),
					"accountId", supportedAccount.ID.Hex(),
					"chain", chainObj.Symbol,
					"address", walletAddress,
					"balance", balance)

				// Create the new account
				newAccount := &types.Account{
					ID:                  primitive.NewObjectID(),
					WalletID:            supportedAccount.WalletID,
					UserID:              supportedAccount.UserID,
					CurrencyID:          currency.CurrencyID,
					ChainID:             currency.ChainID,
					SupportedCurrencyID: currency.ID,
					WalletAddress:       walletAddress,
					EncryptedPrivateKey: supportedAccount.EncryptedPrivateKey,
					Balance:             balance,
					Type:                "ACCOUNT_IMPORTED",
					CreatedAt:           time.Now(),
					UpdatedAt:           time.Now(),
				}

				// Check for duplicate account
				isDuplicate, err := uc.accountRepo.CheckDuplicateAccount(
					ctx,
					newAccount.WalletAddress,
					newAccount.UserID,
					newAccount.ChainID,
					newAccount.CurrencyID,
				)
				if err != nil {
					uc.logger.Error("Failed to check for duplicate account",
						"error", err, "walletAddress", newAccount.WalletAddress)
					continue
				}

				if isDuplicate {
					uc.logger.Warn("Skipping duplicate account",
						"walletAddress", newAccount.WalletAddress,
						"userId", newAccount.UserID.Hex(),
						"chainId", newAccount.ChainID.Hex(),
						"currencyId", newAccount.CurrencyID.Hex())
					continue
				}

				// Save the new account
				startTime := time.Now()
				err = uc.accountRepo.Create(ctx, newAccount)
				if err != nil {
					uc.logger.Error("Failed to create account",
						"error", err, "duration", time.Since(startTime))
					continue
				}

				createdIDs = append(createdIDs, newAccount.ID)
				totalCreated++
				uc.logger.Info("Account created successfully",
					"accountId", newAccount.ID.Hex(),
					"duration", time.Since(startTime))

				if uc.requestManager != nil {
					// Prepare data for notification
					notificationData := map[string]interface{}{
						"event":        "accounts_import_started",
						"accountCount": len(accounts),
						"account": map[string]interface{}{
							"_id":                 newAccount.ID.Hex(),
							"walletId":            newAccount.WalletID.Hex(),
							"userId":              newAccount.UserID.Hex(),
							"currencyId":          newAccount.CurrencyID.Hex(),
							"chainId":             newAccount.ChainID.Hex(),
							"walletAddress":       newAccount.WalletAddress,
							"supportedCurrencyId": newAccount.SupportedCurrencyID.Hex(),
							"balance":             newAccount.Balance,
							"encryptedPrivateKey": newAccount.EncryptedPrivateKey,
							"type":                newAccount.Type,
							"createdAt":           newAccount.CreatedAt,
							"updatedAt":           newAccount.UpdatedAt,
						},
						"timestamp": time.Now().Unix(),
					}

					// Send notification to Node.js server asynchronously
					go func() {
						// Create a new context with timeout for the HTTP request
						reqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()

						// Make the HTTP request
						resp, err := uc.requestManager.Post(reqCtx, "NODE_SERVER", "/wallets/accounts-event", notificationData, nil)
						if err != nil {
							uc.logger.Error("Failed to notify Node.js server about account import", "error", err)
						} else {
							uc.logger.Info("Successfully notified Node.js server about account import", "response", string(resp))
						}
					}()
				} else {
					uc.logger.Warn("RequestManager not initialized, skipping Node.js server notification")
				}

				// Add a small delay between account creations to avoid resource exhaustion
				time.Sleep(50 * time.Millisecond)
			}

			// Add a longer delay between chunks to allow GC to run
			uc.logger.Info("Chunk completed")
			time.Sleep(300 * time.Millisecond)
		}

		// Add an even longer delay between batches for GC
		uc.logger.Info("Batch completed",
			"batchSize", len(supportedCurrencies))
		supportedCurrencies = nil // Help free memory
		time.Sleep(1000 * time.Millisecond)
	}

	uc.logger.Info("Finished processing all batches",
		"totalProcessed", processed,
		"totalCreated", totalCreated,
		"totalAccounts", len(createdIDs))

	return createdIDs, nil
}

// processOriginalAccounts handles the import of the original account list
func (uc *AccountUseCase) processOriginalAccounts(ctx context.Context, accounts []*types.Account) ([]primitive.ObjectID, error) {
	createdIDs := make([]primitive.ObjectID, 0, len(accounts))

	// Process each original account
	for i, account := range accounts {
		// Check if context is done
		select {
		case <-ctx.Done():
			uc.logger.Error("Context canceled during original account processing",
				"error", ctx.Err(), "completedAccounts", i, "totalAccounts", len(accounts))
			return createdIDs, ctx.Err()
		default:
			// Continue processing
		}

		// Safety check
		if account == nil {
			uc.logger.Error("Received nil account", "index", i)
			continue
		}

		uc.logger.Debug("Processing account", "index", i, "walletId", account.WalletID.Hex(), "address", account.WalletAddress)

		// Set creation time if not set
		if account.CreatedAt.IsZero() {
			now := time.Now()
			account.CreatedAt = now
			account.UpdatedAt = now
			uc.logger.Debug("Set creation time for account", "index", i, "time", now)
		}

		// Validate wallet exists
		uc.logger.Debug("Validating wallet", "walletId", account.WalletID.Hex())
		wallet, err := uc.walletRepo.GetByID(ctx, account.WalletID)
		if err != nil {
			uc.logger.Error("Failed to get wallet for account import",
				"walletId", account.WalletID.Hex(), "error", err)
			return createdIDs, err
		}

		if wallet == nil {
			uc.logger.Error("Wallet not found for account import", "walletId", account.WalletID.Hex())
			return createdIDs, ErrWalletNotFound
		}

		// Validate supported currency exists
		if !account.SupportedCurrencyID.IsZero() {
			uc.logger.Debug("Validating currency", "supportedCurrencyId", account.SupportedCurrencyID.Hex())

			// Check if supportedCurrencyRepo is initialized
			if uc.supportedCurrencyRepo == nil {
				uc.logger.Error("supportedCurrencyRepo is nil, cannot validate currency")
				return createdIDs, errors.New("supportedCurrencyRepo is not initialized")
			}

			// Use a separate try-catch for this potentially dangerous operation
			var supported_currency *models.SupportedCurrency
			var err error

			func() {
				defer func() {
					if r := recover(); r != nil {
						uc.logger.Error("Panic during supportedCurrencyRepo.GetByID",
							"supportedCurrencyId", account.SupportedCurrencyID.Hex(),
							"error", r)
						err = fmt.Errorf("panic during currency lookup: %v", r)
					}
				}()
				supported_currency, err = uc.supportedCurrencyRepo.GetByID(ctx, account.SupportedCurrencyID)
			}()

			if err != nil {
				uc.logger.Error("Failed to get currency for account import",
					"supportedCurrencyId", account.SupportedCurrencyID.Hex(),
					"error", err)
				return createdIDs, err
			}

			if supported_currency == nil {
				uc.logger.Error("SupportedCurrency not found for account import",
					"supportedCurrencyId", account.SupportedCurrencyID.Hex())
				return createdIDs, ErrCurrencyNotFound
			}

			// Validate the currency and chain IDs before using them
			if supported_currency.CurrencyID.IsZero() {
				uc.logger.Warn("SupportedCurrency has empty CurrencyID, not setting",
					"supportedCurrencyId", account.SupportedCurrencyID.Hex())
			} else {
				// Set the currencyID from the supported currency
				account.CurrencyID = supported_currency.CurrencyID
				uc.logger.Debug("Set currencyID", "value", account.CurrencyID.Hex())
			}

			if supported_currency.ChainID.IsZero() {
				uc.logger.Warn("SupportedCurrency has empty ChainID, not setting",
					"supportedCurrencyId", account.SupportedCurrencyID.Hex())
			} else {
				// Set the chainID from the supported currency
				account.ChainID = supported_currency.ChainID
				uc.logger.Debug("Set chainID", "value", account.ChainID.Hex())
			}

		} else {
			uc.logger.Warn("No SupportedCurrencyID provided, account will have empty currencyID and chainID",
				"address", account.WalletAddress)
		}

		// Set account type for imports if not already set
		if account.Type == "" {
			account.Type = "ACCOUNT_IMPORTED"
		}

		// Check for duplicate account
		isDuplicate, err := uc.accountRepo.CheckDuplicateAccount(
			ctx,
			account.WalletAddress,
			account.UserID,
			account.ChainID,
			account.CurrencyID,
		)
		if err != nil {
			uc.logger.Error("Failed to check for duplicate account", "error", err, "walletAddress", account.WalletAddress)
			return createdIDs, err
		}

		if isDuplicate {
			uc.logger.Warn("Skipping duplicate account",
				"walletAddress", account.WalletAddress,
				"userId", account.UserID.Hex(),
				"chainId", account.ChainID.Hex(),
				"currencyId", account.CurrencyID.Hex())
			continue
		}

		// Save account
		uc.logger.Debug("Saving account", "index", i, "walletAddress", account.WalletAddress)
		startTime := time.Now()

		// Check if we need to fetch the balance from blockchain
		shouldFetchBalance := account.WalletAddress != "" && !account.SupportedCurrencyID.IsZero() && account.Type != "ACCOUNT_CREATED"

		if shouldFetchBalance {
			// Get supported currency to access chain and currency directly
			supportedCurrency, err := uc.supportedCurrencyRepo.GetByID(ctx, account.SupportedCurrencyID)
			if err != nil {
				uc.logger.Error("Failed to get supported currency for balance fetch",
					"supportedCurrencyId", account.SupportedCurrencyID.Hex(),
					"error", err)
				// Continue with zero balance if supported currency fetch fails
			} else if supportedCurrency != nil {
				// Fetch currency and chain information directly
				currency, err := uc.currencyRepo.GetByID(ctx, supportedCurrency.CurrencyID)
				if err != nil {
					uc.logger.Error("Failed to get currency for balance fetch",
						"currencyId", supportedCurrency.CurrencyID.Hex(),
						"error", err)
					// Continue with zero balance if currency fetch fails
				} else if currency != nil {
					uc.logger.Info("Fetching chain for balance update",
						"supportedCurrencyId", supportedCurrency.ID.Hex(),
						"chainId", supportedCurrency.ChainID.Hex())

					chain, err := uc.chainRepo.GetByID(ctx, supportedCurrency.ChainID)

					// Print more detailed information about the chain
					if chain != nil {
						uc.logger.Info("Chain details retrieved successfully",
							"id", chain.ID.Hex(),
							"name", chain.Name,
							"symbol", chain.Symbol,
							"chainID", chain.ChainID,
							"chainIDType", fmt.Sprintf("%T", chain.ChainID),
							"networkType", chain.NetworkType,
							"rpcURL", chain.RpcURL)

						fmt.Printf("Chain details: ID=%s, Name=%s, Symbol=%s, ChainID=%v (type: %T), NetworkType=%s\n",
							chain.ID.Hex(), chain.Name, chain.Symbol, chain.ChainID, chain.ChainID, chain.NetworkType)
					} else {
						uc.logger.Error("Chain is nil after repository lookup",
							"supportedCurrencyId", supportedCurrency.ID.Hex(),
							"chainId", supportedCurrency.ChainID.Hex())
						fmt.Println("Chain is nil after repository lookup")
					}

					if err != nil {
						uc.logger.Error("Failed to get chain for balance fetch",
							"chainId", supportedCurrency.ChainID.Hex(),
							"error", err)
						// Continue with zero balance if chain fetch fails
					} else if chain != nil {
						// Use blockchain service to fetch the balance
						uc.logger.Info("Fetching balance from blockchain",
							"walletAddress", account.WalletAddress,
							"currency", currency.Symbol,
							"chain", chain.Symbol,
							"chainID", chain.ChainID,
							"supportedCurrencyId", account.SupportedCurrencyID.Hex())

						fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
						balance, err := uc.blockchainService.FetchAccountBalance(
							fetchCtx,
							account.WalletAddress,
							currency,
							chain,
							supportedCurrency, // Pass the supportedCurrency instead of nil
						)
						cancel()

						if err != nil {
							uc.logger.Error("Failed to fetch balance from blockchain",
								"error", err,
								"walletAddress", account.WalletAddress)
							// Continue with zero balance
						} else {
							// Update account with fetched balance
							uc.logger.Info("Successfully fetched balance",
								"walletAddress", account.WalletAddress,
								"balance", balance)
							account.UpdateBalance(balance)
						}
					}
				}
			}
		}

		err = uc.accountRepo.Create(ctx, account)
		if err != nil {
			uc.logger.Error("Failed to create account during import", "error", err, "duration", time.Since(startTime), "index", i)
			return createdIDs, err
		}

		if uc.requestManager != nil {
			// Prepare data for notification
			notificationData := map[string]interface{}{
				"event":        "accounts_import_started",
				"accountCount": len(accounts),
				"account": map[string]interface{}{
					"_id":                 account.ID.Hex(),
					"walletId":            account.WalletID.Hex(),
					"userId":              account.UserID.Hex(),
					"currencyId":          account.CurrencyID.Hex(),
					"chainId":             account.ChainID.Hex(),
					"walletAddress":       account.WalletAddress,
					"supportedCurrencyId": account.SupportedCurrencyID.Hex(),
					"balance":             account.Balance,
					"encryptedPrivateKey": account.EncryptedPrivateKey,
					"type":                account.Type,
					"createdAt":           account.CreatedAt,
					"updatedAt":           account.UpdatedAt,
				},
				"timestamp": time.Now().Unix(),
			}

			// Send notification to Node.js server asynchronously
			go func() {
				// Create a new context with timeout for the HTTP request
				reqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				// Make the HTTP request
				resp, err := uc.requestManager.Post(reqCtx, "NODE_SERVER", "/wallets/accounts-event", notificationData, nil)
				if err != nil {
					uc.logger.Error("Failed to notify Node.js server about account import", "error", err)
				} else {
					uc.logger.Info("Successfully notified Node.js server about account import", "response", string(resp))
				}
			}()
		} else {
			uc.logger.Warn("RequestManager not initialized, skipping Node.js server notification")
		}

		createdIDs = append(createdIDs, account.ID)
		uc.logger.Info("Account imported successfully", "index", i, "accountId", account.ID.Hex(), "duration", time.Since(startTime))
	}

	return createdIDs, nil
}
