package handlers

import (
	"context"
	"fmt"
	"time"

	"bitbucket.org/zapspace/zap-go-server/interfaces/http/handlers"
	"bitbucket.org/zapspace/zap-go-server/managers/logger"
	"bitbucket.org/zapspace/zap-go-server/models"
	pb "bitbucket.org/zapspace/zap-go-server/protoBuilds/wallet"
	"bitbucket.org/zapspace/zap-go-server/src/repositories"
	"bitbucket.org/zapspace/zap-go-server/src/services"
	"bitbucket.org/zapspace/zap-go-server/src/types"
	"bitbucket.org/zapspace/zap-go-server/src/usecases"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GrpcImportHandler implements the AccountImportService gRPC service
type GrpcImportHandler struct {
	accountUseCase        *usecases.AccountUseCase
	eventService          *services.EventService
	supportedCurrencyRepo repositories.SupportedCurrencyRepository
	logger                logger.Logger

	// Embed the UnimplementedAccountImportServiceServer to satisfy interface
	pb.UnimplementedAccountImportServiceServer
}

// mustEmbedUnimplementedAccountImportServiceServer satisfies the protobuf interface
func (h *GrpcImportHandler) mustEmbedUnimplementedAccountImportServiceServer() {}

// NewGrpcImportHandler creates a new gRPC import handler
func NewGrpcImportHandler(
	accountUseCase *usecases.AccountUseCase,
	eventService *services.EventService,
	supportedCurrencyRepo repositories.SupportedCurrencyRepository,
	logger logger.Logger,
) *GrpcImportHandler {
	return &GrpcImportHandler{
		accountUseCase:        accountUseCase,
		eventService:          eventService,
		supportedCurrencyRepo: supportedCurrencyRepo,
		logger:                logger,
	}
}

// Custom account data type for JSON unmarshaling
type ClientAccountData struct {
	WalletID            string  `json:"wallet_id"`
	UserID              string  `json:"user_id"`
	SupportedCurrencyID string  `json:"supported_currency_id"`
	WalletAddress       string  `json:"wallet_address"`
	EncryptedPrivateKey string  `json:"encrypted_private_key"`
	Balance             float64 `json:"balance"`
}

// GrpcAccountData represents account data for gRPC requests - used to avoid direct proto dependencies
type GrpcAccountData struct {
	WalletId            string
	UserId              string
	SupportedCurrencyId string
	ChainId             string
	WalletAddress       string
	EncryptedPrivateKey string
	Balance             float64
}

// ImportRequest contains import account request data - used to avoid direct proto dependencies
type ImportRequest struct {
	Accounts    []*GrpcAccountData
	CallbackUrl string
}

// ImportResponse contains import operation result - used to avoid direct proto dependencies
type ImportResponse struct {
	Status             string
	Message            string
	ImportedAccountIds []string
	Error              string
}

// ImportAccounts handles the gRPC request to import accounts
// This implements the AccountImportServiceServer interface
func (h *GrpcImportHandler) ImportAccounts(ctx context.Context, req *pb.ImportAccountsRequest) (*pb.ImportAccountsResponse, error) {
	h.logger.Info("gRPC ImportAccounts called", "accountsCount", len(req.Accounts))

	// Check if the request context is already canceled
	select {
	case <-ctx.Done():
		h.logger.Error("Request context already canceled", "error", ctx.Err())
		return &pb.ImportAccountsResponse{
			Status:  "error",
			Message: "Request context already canceled",
			Error:   ctx.Err().Error(),
		}, status.Error(codes.Aborted, "Request context already canceled")
	default:
		h.logger.Debug("Request context is valid")
	}

	if len(req.Accounts) == 0 {
		return &pb.ImportAccountsResponse{
			Status:  "error",
			Message: "No accounts to import",
		}, status.Error(codes.InvalidArgument, "No accounts to import")
	}

	// Convert proto accounts to HTTP handler's AccountData format
	httpAccounts := make([]handlers.AccountData, 0, len(req.Accounts))
	for i, account := range req.Accounts {
		// Log all available fields to debug
		h.logger.Debug("Processing import account",
			"index", i,
			"walletId", account.WalletId,
			"userId", account.UserId,
			"supportedCurrencyId", account.SupportedCurrencyId,
			"walletAddress", account.WalletAddress)

		httpAccount := handlers.AccountData{
			WalletID:            account.WalletId,
			UserID:              account.UserId,
			SupportedCurrencyID: account.SupportedCurrencyId,
			WalletAddress:       account.WalletAddress,
			EncryptedPrivateKey: account.EncryptedPrivateKey,
			Balance:             account.Balance,
		}

		// Check for Type field in the request and set it in the AccountData
		if req.Type != "" {
			h.logger.Debug("Setting account type from request", "type", req.Type)
			httpAccount.Type = req.Type
		}

		httpAccounts = append(httpAccounts, httpAccount)
	}

	// Convert request data to domain types
	accounts, err := h.convertToAccounttypes(httpAccounts)
	if err != nil {
		h.logger.Error("Failed to convert account data", "error", err)
		return &pb.ImportAccountsResponse{
			Status:  "error",
			Message: "Invalid account data",
			Error:   err.Error(),
		}, status.Error(codes.InvalidArgument, err.Error())
	}

	// Use a background context for import processing
	processCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	// Don't defer cancel here as it cancels when the function returns, not when the goroutine completes

	// Create a response channel to get the result
	respChan := make(chan *pb.ImportAccountsResponse, 1)

	// Process the import asynchronously
	go func() {
		// Make sure to call cancel when the goroutine is done
		defer cancel()

		h.logger.Info("Starting background import process", "accountsCount", len(accounts))

		// Call the account use case to import the accounts
		importedIDs, err := h.accountUseCase.ImportAccounts(processCtx, accounts)

		var eventStatus services.EventStatus
		var message string
		var errorMsg string
		var idStrings []string

		if err != nil {
			h.logger.Error("Failed to import accounts", "error", err)
			eventStatus = services.EventStatusError
			message = "Failed to import accounts"
			errorMsg = err.Error()
		} else {
			// Convert ObjectIDs to strings for response
			idStrings = make([]string, len(importedIDs))
			for i, id := range importedIDs {
				idStrings[i] = id.Hex()
			}

			eventStatus = services.EventStatusSuccess
			message = fmt.Sprintf("Successfully imported %d accounts", len(importedIDs))
		}

		// Create the response
		resp := &pb.ImportAccountsResponse{
			Status:             string(eventStatus),
			Message:            message,
			ImportedAccountIds: idStrings,
			Error:              errorMsg,
		}

		// Send event callback if provided
		if req.CallbackUrl != "" {
			callbackCtx, callbackCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer callbackCancel()

			h.eventService.SendEvent(
				callbackCtx,
				req.CallbackUrl,
				services.EventAccountsImported,
				eventStatus,
				message,
				idStrings,
				errorMsg,
			)
		}

		// Send response back through the channel
		respChan <- resp
	}()

	// Return initial response while processing continues in background
	return &pb.ImportAccountsResponse{
		Status:  "processing",
		Message: fmt.Sprintf("Processing %d accounts in background", len(req.Accounts)),
	}, nil
}

// The original ImportAccounts method signature is kept for backwards compatibility
// but will be deprecated when all clients are updated
func (h *GrpcImportHandler) ImportAccountsOld(ctx context.Context, req interface{}) (interface{}, error) {
	// Convert the generic request to our internal type
	importReq, ok := req.(*ImportRequest)
	if !ok {
		h.logger.Error("Invalid request type", "type", fmt.Sprintf("%T", req))
		return &ImportResponse{
			Status:  "error",
			Message: "Invalid request type",
			Error:   "Expected ImportAccountsRequest",
		}, status.Error(codes.InvalidArgument, "Invalid request type")
	}

	// Convert to the new protobuf request format
	pbReq := &pb.ImportAccountsRequest{
		CallbackUrl: importReq.CallbackUrl,
		Accounts:    make([]*pb.AccountData, 0, len(importReq.Accounts)),
	}

	for _, acc := range importReq.Accounts {
		pbReq.Accounts = append(pbReq.Accounts, &pb.AccountData{
			WalletId:            acc.WalletId,
			UserId:              acc.UserId,
			SupportedCurrencyId: acc.SupportedCurrencyId,
			ChainId:             acc.ChainId,
			WalletAddress:       acc.WalletAddress,
			EncryptedPrivateKey: acc.EncryptedPrivateKey,
			Balance:             acc.Balance,
		})
	}

	// Call the new implementation
	resp, err := h.ImportAccounts(ctx, pbReq)
	if err != nil {
		return &ImportResponse{
			Status:  "error",
			Message: "Import failed",
			Error:   err.Error(),
		}, err
	}

	// Convert back to old response format
	return &ImportResponse{
		Status:             resp.Status,
		Message:            resp.Message,
		ImportedAccountIds: resp.ImportedAccountIds,
		Error:              resp.Error,
	}, nil
}

// convertToAccounttypes converts request data to domain types
func (h *GrpcImportHandler) convertToAccounttypes(data []handlers.AccountData) ([]*types.Account, error) {
	accounts := make([]*types.Account, 0, len(data))

	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			h.logger.Error("Recovered from panic in convertToAccounttypes", "error", r)
		}
	}()

	for i, item := range data {
		h.logger.Debug("Processing account item", "index", i, "walletAddress", item.WalletAddress)

		// Extra validation for required fields
		if item.WalletID == "" {
			return nil, fmt.Errorf("wallet ID is required for account at index %d", i)
		}

		if item.UserID == "" {
			return nil, fmt.Errorf("user ID is required for account at index %d", i)
		}

		walletID, err := primitive.ObjectIDFromHex(item.WalletID)
		if err != nil {
			h.logger.Error("Invalid wallet ID format", "walletId", item.WalletID, "error", err)
			return nil, fmt.Errorf("invalid wallet ID format at index %d: %s", i, item.WalletID)
		}

		userID, err := primitive.ObjectIDFromHex(item.UserID)
		if err != nil {
			h.logger.Error("Invalid user ID format", "userId", item.UserID, "error", err)
			return nil, fmt.Errorf("invalid user ID format at index %d: %s", i, item.UserID)
		}

		// Optional fields
		var currencyID, chainID, supportedCurrencyID primitive.ObjectID
		if item.SupportedCurrencyID != "" {
			h.logger.Debug("Processing supported currency ID", "supportedCurrencyId", item.SupportedCurrencyID)
			supportedCurrencyID, err = primitive.ObjectIDFromHex(item.SupportedCurrencyID)
			if err != nil {
				h.logger.Error("Invalid supported currency ID format", "supportedCurrencyId", item.SupportedCurrencyID, "error", err)
				return nil, fmt.Errorf("invalid supported currency ID format at index %d: %s", i, item.SupportedCurrencyID)
			}

			// Get supported currency details
			h.logger.Debug("Fetching supported currency details", "supportedCurrencyId", supportedCurrencyID.Hex())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Get the supported currency from the account use case through a separate API call
			// Handle potential errors without crashing
			supportedCurrency, err := h.getSupportedCurrencyDetails(ctx, supportedCurrencyID)
			if err != nil {
				h.logger.Error("Failed to get supported currency details - continuing with just the ID",
					"supportedCurrencyId", supportedCurrencyID.Hex(),
					"error", err)
				// Continue with just the supported currency ID
			} else if supportedCurrency != nil {
				// Verify the IDs before setting them
				if !supportedCurrency.CurrencyID.IsZero() {
					currencyID = supportedCurrency.CurrencyID
					h.logger.Debug("Setting currencyID", "value", currencyID.Hex())
				} else {
					h.logger.Warn("SupportedCurrency has empty CurrencyID", "supportedCurrencyId", supportedCurrencyID.Hex())
				}

				if !supportedCurrency.ChainID.IsZero() {
					chainID = supportedCurrency.ChainID
					h.logger.Debug("Setting chainID", "value", chainID.Hex())
				} else {
					h.logger.Warn("SupportedCurrency has empty ChainID", "supportedCurrencyId", supportedCurrencyID.Hex())
				}

				h.logger.Debug("Retrieved currency and chain IDs",
					"supportedCurrencyId", supportedCurrencyID.Hex(),
					"currencyId", currencyID.Hex(),
					"chainId", chainID.Hex())
			}
		}

		// Use the provided type or default to ACCOUNT_IMPORTED
		accountType := item.Type
		if accountType == "" {
			accountType = "ACCOUNT_IMPORTED"
		}

		h.logger.Debug("Creating account domain model",
			"walletId", walletID.Hex(),
			"userId", userID.Hex(),
			"walletAddress", item.WalletAddress,
			"type", accountType)

		// Create the account domain model with all the IDs we've processed
		account := &types.Account{
			WalletID:            walletID,
			UserID:              userID,
			CurrencyID:          currencyID,
			ChainID:             chainID,
			SupportedCurrencyID: supportedCurrencyID,
			WalletAddress:       item.WalletAddress,
			EncryptedPrivateKey: item.EncryptedPrivateKey,
			Balance:             item.Balance,
			Type:                accountType,
		}

		accounts = append(accounts, account)
	}

	h.logger.Debug("Finished converting to account domain models", "count", len(accounts))
	return accounts, nil
}

// getSupportedCurrencyDetails gets the details for a supported currency
func (h *GrpcImportHandler) getSupportedCurrencyDetails(ctx context.Context, supportedCurrencyID primitive.ObjectID) (*models.SupportedCurrency, error) {
	// Add a timeout to the context to prevent hanging
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Get the supported currency
	supportedCurrency, err := h.supportedCurrencyRepo.GetByID(ctx, supportedCurrencyID)
	if err != nil {
		return nil, fmt.Errorf("failed to get supported currency: %w", err)
	}

	// Make sure we have a valid supported currency
	if supportedCurrency == nil {
		return nil, fmt.Errorf("supported currency not found for ID: %s", supportedCurrencyID.Hex())
	}

	return supportedCurrency, nil
}
