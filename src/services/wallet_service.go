package services

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"bitbucket.org/zapspace/zap-go-server/config/mongodb"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// WalletService represents the wallet service
type WalletService struct {
	logger            logger.Logger
	walletRepository  mongodb.WalletRepository
	accountRepository mongodb.AccountRepository
	portfolioService  *PortfolioService
}

// NewWalletService creates a new wallet service instance
func NewWalletService(logger logger.Logger, walletRepo mongodb.WalletRepository, accountRepo mongodb.AccountRepository, portfolioService *PortfolioService) *WalletService {
	return &WalletService{
		logger:            logger,
		walletRepository:  walletRepo,
		accountRepository: accountRepo,
		portfolioService:  portfolioService,
	}
}

// GetUserPortfolio fetches portfolio data for all wallets of a user
func (s *WalletService) GetUserPortfolio(c *gin.Context) {
	userId := c.Param("userId")

	// Validate userId
	objectId, err := primitive.ObjectIDFromHex(userId)
	if err != nil {
		s.logger.Error("Invalid user ID format", "userId", userId, "error", err)
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
	wallets, err := s.walletRepository.GetWalletsByUserId(ctx, objectId)
	if err != nil {
		s.logger.Error("Failed to fetch user wallets", "userId", userId, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Failed to fetch user wallets",
			"status":  "error",
		})
		return
	}

	if len(wallets) == 0 {
		s.logger.Info("No wallets found for user", "userId", userId)
		c.JSON(http.StatusOK, gin.H{
			"data":    []interface{}{},
			"message": "No wallets found for user",
			"status":  "success",
		})
		return
	}

	s.logger.Info("Found wallets for user", "userId", userId, "count", len(wallets))

	// Process wallets in parallel
	results := make([]models.WalletPortfolioResponse, 0, len(wallets))

	// Use a channel to collect results
	resultChan := make(chan *models.WalletPortfolioResponse)
	errorChan := make(chan error)

	// Keep track of active goroutines
	activeRoutines := 0

	// Process each wallet
	for _, wallet := range wallets {
		activeRoutines++
		go func(wallet models.Wallet) {
			portfolio, err := s.portfolioService.GetWalletPortfolio(ctx, wallet.ID)
			if err != nil {
				s.logger.Error("Failed to get portfolio for wallet", "walletId", wallet.ID.Hex(), "error", err)
				errorChan <- err
				return
			}

			if portfolio == nil {
				s.logger.Info("No portfolio data for wallet", "walletId", wallet.ID.Hex())
				resultChan <- nil
				return
			}

			resultChan <- &models.WalletPortfolioResponse{
				WalletID:        wallet.ID,
				Name:            wallet.Name,
				Identifier:      wallet.HashedSeedPhrase,
				WalletPortfolio: portfolio.Accounts,
				WalletUsdValue:  portfolio.WalletUsdValue,
			}
		}(wallet)
	}

	// Collect results
	for i := 0; i < activeRoutines; i++ {
		select {
		case result := <-resultChan:
			if result != nil {
				results = append(results, *result)
			}
		case err := <-errorChan:
			s.logger.Error("Error processing wallet portfolio", "error", err)
		case <-ctx.Done():
			s.logger.Error("Context deadline exceeded when fetching wallet portfolios")
			c.JSON(http.StatusGatewayTimeout, gin.H{
				"message": "Request timeout while processing wallet portfolios",
				"status":  "error",
			})
			return
		}
	}

	s.logger.Info("Completed portfolio processing", "userId", userId, "successfulWallets", len(results))

	c.JSON(http.StatusOK, gin.H{
		"data":    results,
		"message": "User portfolio fetched successfully",
		"status":  "success",
	})
}

// GetWalletPortfolio fetches portfolio data for a specific wallet
func (s *WalletService) GetWalletPortfolio(c *gin.Context) {
	walletId := c.Param("walletId")

	// Validate walletId
	objectId, err := primitive.ObjectIDFromHex(walletId)
	if err != nil {
		s.logger.Error("Invalid wallet ID format", "walletId", walletId, "error", err)
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
	portfolio, err := s.portfolioService.GetWalletPortfolio(ctx, objectId)
	if err != nil {
		s.logger.Error("Failed to get wallet portfolio", "walletId", walletId, "error", err)
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
func (s *WalletService) GetAccountPortfolio(c *gin.Context) {
	accountId := c.Param("accountId")

	// Validate accountId
	objectId, err := primitive.ObjectIDFromHex(accountId)
	if err != nil {
		s.logger.Error("Invalid account ID format", "accountId", accountId, "error", err)
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
	portfolio, err := s.portfolioService.GetAccountPortfolio(ctx, objectId)
	if err != nil {
		s.logger.Error("Failed to get account portfolio", "accountId", accountId, "error", err)
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
func (s *WalletService) GetAccountBalance(c *gin.Context) {
	accountId := c.Param("accountId")

	// Validate accountId
	objectId, err := primitive.ObjectIDFromHex(accountId)
	if err != nil {
		s.logger.Error("Invalid account ID format", "accountId", accountId, "error", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid account ID format",
			"status":  "error",
		})
		return
	}

	// Set timeout context
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// Fetch account
	account, err := s.accountRepository.GetAccountByID(ctx, objectId)
	if err != nil {
		s.logger.Error("Failed to fetch account", "accountId", accountId, "error", err)
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
	if account.WalletAddress == "" {
		s.logger.Error("Account has no wallet address", "accountId", accountId)
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Account has no wallet address",
			"status":  "error",
		})
		return
	}

	// Get balance from blockchain service through portfolio service
	balance, err := s.portfolioService.GetBalance(ctx, account)
	if err != nil {
		s.logger.Error("Failed to get account balance", "accountId", accountId, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Failed to get account balance",
			"status":  "error",
		})
		return
	}

	// Update account with new balance
	account.UpdateBalance(balance)
	err = s.accountRepository.UpdateAccount(ctx, account)
	if err != nil {
		s.logger.Error("Failed to update account balance", "accountId", accountId, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Failed to update account balance",
			"status":  "error",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"account": account,
			"balance": balance,
		},
		"message": "Account balance updated successfully",
		"status":  "success",
	})
}

// GetAccount retrieves an account by ID
func (s *WalletService) GetAccount(ctx context.Context, accountID primitive.ObjectID) (*models.Account, error) {
	s.logger.Info("Getting account", "accountId", accountID.Hex())
	return s.accountRepository.GetAccountByID(ctx, accountID)
}

// GetWalletsByUserID retrieves all wallets for a user
func (s *WalletService) GetWalletsByUserID(ctx context.Context, userID primitive.ObjectID) ([]models.Wallet, error) {
	s.logger.Info("Getting wallets for user", "userId", userID.Hex())
	return s.walletRepository.GetWalletsByUserId(ctx, userID)
}

// WalletExists checks if a wallet exists
func (s *WalletService) WalletExists(ctx context.Context, walletID primitive.ObjectID) (bool, error) {
	s.logger.Info("Checking if wallet exists", "walletId", walletID.Hex())
	wallet, err := s.walletRepository.GetWalletByID(ctx, walletID)
	if err != nil {
		if err == mongodb.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return wallet != nil, nil
}
