package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/types"
	"bitbucket.org/zapspace/zap-go-server/src/usecases"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// WalletHandler handles HTTP requests related to wallets
type WalletHandler struct {
	walletUseCase    *usecases.WalletUseCase
	portfolioUseCase *usecases.PortfolioUseCase
	accountUseCase   *usecases.AccountUseCase
	eventService     *services.EventService
	logger           logger.Logger
}

// NewWalletHandler creates a new wallet handler
func NewWalletHandler(
	walletUseCase *usecases.WalletUseCase,
	portfolioUseCase *usecases.PortfolioUseCase,
	accountUseCase *usecases.AccountUseCase,
	eventService *services.EventService,
	logger logger.Logger,
) *WalletHandler {
	return &WalletHandler{
		walletUseCase:    walletUseCase,
		portfolioUseCase: portfolioUseCase,
		accountUseCase:   accountUseCase,
		eventService:     eventService,
		logger:           logger,
	}
}

// GetUserPortfolio fetches portfolio data for all wallets of a user
func (h *WalletHandler) GetUserPortfolio(c *gin.Context) {
	userId := c.Param("userId")

	// Validate userId
	objectId, err := primitive.ObjectIDFromHex(userId)
	if err != nil {
		h.logger.Error("Invalid user ID format", "userId", userId, "error", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid user ID format",
			"status":  "error",
		})
		return
	}

	// Set timeout context
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	// Fetch wallets for user
	wallets, err := h.walletUseCase.GetWalletsByUserID(ctx, objectId)
	if err != nil {
		h.logger.Error("Failed to fetch user wallets", "userId", userId, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Failed to fetch user wallets",
			"status":  "error",
		})
		return
	}

	if len(wallets) == 0 {
		h.logger.Info("No wallets found for user", "userId", userId)
		c.JSON(http.StatusOK, gin.H{
			"data":    []interface{}{},
			"message": "No wallets found for user",
			"status":  "success",
		})
		return
	}

	h.logger.Info("Found wallets for user", "userId", userId, "count", len(wallets))

	// Process wallets in parallel
	results := make([]map[string]interface{}, 0, len(wallets))
	resultChan := make(chan map[string]interface{})
	errorChan := make(chan error)
	activeRoutines := 0

	// Process each wallet
	for _, wallet := range wallets {
		activeRoutines++
		go func(wallet *types.Wallet) {
			portfolio, err := h.portfolioUseCase.GetWalletPortfolio(ctx, wallet.ID)
			if err != nil {
				h.logger.Error("Failed to get portfolio for wallet", "walletId", wallet.ID.Hex(), "error", err)
				errorChan <- err
				return
			}

			if portfolio == nil {
				h.logger.Info("No portfolio data for wallet", "walletId", wallet.ID.Hex())
				resultChan <- nil
				return
			}

			resultChan <- map[string]interface{}{
				"walletId":        wallet.ID,
				"name":            wallet.Name,
				"identifier":      wallet.HashedSeedPhrase,
				"walletPortfolio": portfolio.Accounts,
				"walletUsdValue":  portfolio.WalletUsdValue,
			}
		}(wallet)
	}

	// Collect results
	for i := 0; i < activeRoutines; i++ {
		select {
		case result := <-resultChan:
			if result != nil {
				results = append(results, result)
			}
		case err := <-errorChan:
			h.logger.Error("Error processing wallet portfolio", "error", err)
		case <-ctx.Done():
			h.logger.Error("Context deadline exceeded when fetching wallet portfolios")
			c.JSON(http.StatusGatewayTimeout, gin.H{
				"message": "Request timeout while processing wallet portfolios",
				"status":  "error",
			})
			return
		}
	}

	h.logger.Info("Completed portfolio processing", "userId", userId, "successfulWallets", len(results))

	c.JSON(http.StatusOK, gin.H{
		"data":    results,
		"message": "User portfolio fetched successfully",
		"status":  "success",
	})
}

// GetWalletPortfolio fetches portfolio data for a specific wallet
func (h *WalletHandler) GetWalletPortfolio(c *gin.Context) {
	walletId := c.Param("walletId")

	// Validate walletId
	objectId, err := primitive.ObjectIDFromHex(walletId)
	if err != nil {
		h.logger.Error("Invalid wallet ID format", "walletId", walletId, "error", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid wallet ID format",
			"status":  "error",
		})
		return
	}

	// Set timeout context
	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()

	// Get wallet portfolio
	portfolio, err := h.portfolioUseCase.GetWalletPortfolio(ctx, objectId)
	if err != nil {
		h.logger.Error("Failed to get wallet portfolio", "walletId", walletId, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Failed to get wallet portfolio",
			"status":  "error",
		})
		return
	}

	if portfolio == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"message": "Wallet portfolio not found",
			"status":  "error",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":    portfolio,
		"message": "Wallet portfolio fetched successfully",
		"status":  "success",
	})
}

// GetAccountPortfolio fetches portfolio data for a specific account
func (h *WalletHandler) GetAccountPortfolio(c *gin.Context) {
	accountId := c.Param("accountId")

	// Validate accountId
	objectId, err := primitive.ObjectIDFromHex(accountId)
	if err != nil {
		h.logger.Error("Invalid account ID format", "accountId", accountId, "error", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid account ID format",
			"status":  "error",
		})
		return
	}

	// Set timeout context
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// Get account portfolio
	portfolio, err := h.portfolioUseCase.GetAccountPortfolio(ctx, objectId)
	if err != nil {
		h.logger.Error("Failed to get account portfolio", "accountId", accountId, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Failed to get account portfolio",
			"status":  "error",
		})
		return
	}

	if portfolio == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"message": "Account portfolio not found",
			"status":  "error",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":    portfolio,
		"message": "Account portfolio fetched successfully",
		"status":  "success",
	})
}

// GetAccountBalance fetches and updates the balance for a specific account
func (h *WalletHandler) GetAccountBalance(c *gin.Context) {
	accountId := c.Param("accountId")

	// Validate accountId
	objectId, err := primitive.ObjectIDFromHex(accountId)
	if err != nil {
		h.logger.Error("Invalid account ID format", "accountId", accountId, "error", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid account ID format",
			"status":  "error",
		})
		return
	}

	// Set timeout context
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// Get account
	account, err := h.accountUseCase.GetAccountByID(ctx, objectId)
	if err != nil {
		h.logger.Error("Failed to fetch account", "accountId", accountId, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Failed to fetch account",
			"status":  "error",
		})
		return
	}

	if account == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"message": "Account not found",
			"status":  "error",
		})
		return
	}

	// Check if account has wallet address
	if !account.HasWalletAddress() {
		h.logger.Error("Account has no wallet address", "accountId", accountId)
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Account has no wallet address",
			"status":  "error",
		})
		return
	}

	// Get balance
	balance, err := h.portfolioUseCase.GetBalance(ctx, objectId)
	if err != nil {
		h.logger.Error("Failed to get account balance", "accountId", accountId, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Failed to get account balance",
			"status":  "error",
		})
		return
	}

	// Get updated account after balance update
	updatedAccount, err := h.accountUseCase.GetAccountByID(ctx, objectId)
	if err != nil {
		h.logger.Error("Failed to get updated account", "accountId", accountId, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Failed to get updated account details",
			"status":  "error",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"account": updatedAccount,
			"balance": balance,
		},
		"message": "Account balance updated successfully",
		"status":  "success",
	})
}

// AccountImportRequest represents the request for importing accounts
type AccountImportRequest struct {
	Accounts    []AccountData `json:"accounts"`
	CallbackURL string        `json:"callbackUrl"`
}

// AccountData represents a wallet account for bulk import
type AccountData struct {
	WalletID            string  `json:"walletId"`
	UserID              string  `json:"userId"`
	SupportedCurrencyID string  `json:"supportedCurrencyId"`
	WalletAddress       string  `json:"walletAddress"`
	EncryptedPrivateKey string  `json:"encryptedPrivateKey,omitempty"`
	Balance             float64 `json:"balance,omitempty"`
	Type                string  `json:"type,omitempty"`
}

// ImportResponse represents the response for account import operation
type ImportResponse struct {
	Status  string   `json:"status"`
	Message string   `json:"message"`
	Data    []string `json:"data,omitempty"`
	Error   string   `json:"error,omitempty"`
}

// HandleAccountsImported processes accounts import from the Node.js server
func (h *WalletHandler) HandleAccountsImported(c *gin.Context) {
	var request AccountImportRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		h.logger.Error("Invalid account import request", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid request format",
			"error":   err.Error(),
		})
		return
	}

	if len(request.Accounts) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "No accounts to import",
		})
		return
	}

	// Set timeout context for the operation
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	// Convert request data to domain types
	accounts, err := h.convertToAccounttypes(request.Accounts)
	if err != nil {
		h.logger.Error("Failed to convert account data", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid account data",
			"error":   err.Error(),
		})
		return
	}

	// Process import in a goroutine to prevent blocking HTTP handler
	go func() {
		importedIDs, err := h.accountUseCase.ImportAccounts(ctx, accounts)

		var status services.EventStatus
		var message string
		var errorMsg string
		var data interface{}

		if err != nil {
			h.logger.Error("Failed to import accounts", "error", err)
			status = services.EventStatusError
			message = "Failed to import accounts"
			errorMsg = err.Error()
		} else {
			// Convert ObjectIDs to strings for response
			idStrings := make([]string, len(importedIDs))
			for i, id := range importedIDs {
				idStrings[i] = id.Hex()
			}

			status = services.EventStatusSuccess
			message = fmt.Sprintf("Successfully imported %d accounts", len(importedIDs))
			data = idStrings
		}

		// Send event callback to Node.js server using our event service
		callbackCtx, callbackCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer callbackCancel()

		h.eventService.SendEvent(
			callbackCtx,
			request.CallbackURL,
			services.EventAccountsImported,
			status,
			message,
			data,
			errorMsg,
		)
	}()

	// Immediately respond that we've received the request
	c.JSON(http.StatusAccepted, gin.H{
		"status":  "accepted",
		"message": "Account import process started",
	})
}

// convertToAccounttypes converts request data to domain types
func (h *WalletHandler) convertToAccounttypes(data []AccountData) ([]*types.Account, error) {
	accounts := make([]*types.Account, 0, len(data))

	for i, item := range data {
		h.logger.Debug("Processing account data", "index", i, "walletAddress", item.WalletAddress)

		walletID, err := primitive.ObjectIDFromHex(item.WalletID)
		if err != nil {
			return nil, fmt.Errorf("invalid wallet ID: %s", item.WalletID)
		}

		userID, err := primitive.ObjectIDFromHex(item.UserID)
		if err != nil {
			return nil, fmt.Errorf("invalid user ID: %s", item.UserID)
		}

		// Optional fields
		var currencyID, chainID, supportedCurrencyID primitive.ObjectID
		if item.SupportedCurrencyID != "" {
			h.logger.Debug("Processing supportedCurrencyID", "value", item.SupportedCurrencyID)

			// Parse the SupportedCurrencyID
			supportedCurrencyID, err = primitive.ObjectIDFromHex(item.SupportedCurrencyID)
			if err != nil {
				return nil, fmt.Errorf("invalid supported currency ID: %s", item.SupportedCurrencyID)
			}

			// Use this value for both currencyID and chainID for now
			// In a real implementation, you should look up the actual values from the database
			currencyID = supportedCurrencyID
			chainID = supportedCurrencyID
		}

		account := types.NewAccount(
			walletID,
			userID,
			currencyID,
			chainID,
			item.WalletAddress,
			item.EncryptedPrivateKey,
		)

		// Set the supported currency ID
		account.SupportedCurrencyID = supportedCurrencyID

		// Set the account type for imports
		account.Type = "ACCOUNT_IMPORTED"

		h.logger.Debug("Created account",
			"walletAddress", account.WalletAddress,
			"type", account.Type,
			"supportedCurrencyId", account.SupportedCurrencyID.Hex(),
			"currencyId", account.CurrencyID.Hex(),
			"chainId", account.ChainID.Hex())

		// Set balance if provided
		if item.Balance > 0 {
			account.UpdateBalance(item.Balance)
		}

		accounts = append(accounts, account)
	}

	return accounts, nil
}

// BalanceUpdateRequest represents a request to update and notify about balance updates
type BalanceUpdateRequest struct {
	AccountID   string  `json:"accountId"`
	NewBalance  float64 `json:"newBalance,omitempty"`
	CallbackURL string  `json:"callbackUrl"`
}

// HandleBalanceUpdate processes a balance update and notifies the Node.js backend
func (h *WalletHandler) HandleBalanceUpdate(c *gin.Context) {
	var request BalanceUpdateRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		h.logger.Error("Invalid balance update request", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid request format",
			"error":   err.Error(),
		})
		return
	}

	// Validate account ID
	accountID, err := primitive.ObjectIDFromHex(request.AccountID)
	if err != nil {
		h.logger.Error("Invalid account ID format", "accountId", request.AccountID, "error", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid account ID format",
			"error":   err.Error(),
		})
		return
	}

	// Accept the request immediately
	c.JSON(http.StatusAccepted, gin.H{
		"status":  "accepted",
		"message": "Balance update process started",
	})

	// Process in a goroutine to prevent blocking HTTP handler
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var account *types.Account
		var balance float64
		var updateErr error

		// If a specific balance was provided, update with that value
		if request.NewBalance > 0 {
			account, err = h.accountUseCase.GetAccountByID(ctx, accountID)
			if err != nil {
				h.logger.Error("Failed to fetch account", "accountId", accountID.Hex(), "error", err)
				updateErr = err
			} else if account == nil {
				h.logger.Error("Account not found", "accountId", accountID.Hex())
				updateErr = fmt.Errorf("account not found")
			} else {
				// Update account with provided balance
				account.UpdateBalance(request.NewBalance)
				updateErr = h.accountUseCase.UpdateAccount(ctx, account)
				balance = request.NewBalance
			}
		} else {
			// Fetch latest balance from blockchain
			balance, updateErr = h.portfolioUseCase.GetBalance(ctx, accountID)
			if updateErr != nil {
				h.logger.Error("Failed to get account balance", "accountId", accountID.Hex(), "error", updateErr)
			}

			// Get updated account after balance update
			account, err = h.accountUseCase.GetAccountByID(ctx, accountID)
			if err != nil {
				h.logger.Error("Failed to get updated account", "accountId", accountID.Hex(), "error", err)
				if updateErr == nil {
					updateErr = err
				}
			}
		}

		// Prepare event data
		var status services.EventStatus
		var message string
		var errorMsg string
		var data interface{}

		if updateErr != nil {
			status = services.EventStatusError
			message = "Failed to update balance"
			errorMsg = updateErr.Error()
		} else {
			status = services.EventStatusSuccess
			message = "Balance updated successfully"
			data = map[string]interface{}{
				"accountId": accountID.Hex(),
				"balance":   balance,
				"account":   account,
			}
		}

		// Send event to Node.js server
		h.eventService.SendEvent(
			ctx,
			request.CallbackURL,
			services.EventBalanceUpdated,
			status,
			message,
			data,
			errorMsg,
		)
	}()
}

// GetEVMChainStatus returns the status of EVM chain workers (for monitoring)
func (h *WalletHandler) GetEVMChainStatus(c *gin.Context) {
	// This would require access to the EVM worker instance
	// For now, return a placeholder response
	// In a real implementation, you'd inject the EVM worker or create a service for this

	c.JSON(http.StatusOK, gin.H{
		"message": "EVM chain status endpoint - would show active/inactive chains",
		"status":  "success",
		"note":    "This endpoint requires EVM worker integration",
	})
}
