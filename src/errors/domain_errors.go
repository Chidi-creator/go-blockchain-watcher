package errors

import (
	"errors"
)

// Domain error definitions
var (
	// Common errors
	ErrNotFound      = errors.New("entity not found")
	ErrInvalidID     = errors.New("invalid ID")
	ErrDuplicateKey  = errors.New("duplicate key")
	ErrContextCancel = errors.New("context cancelled")

	// Entity specific errors
	ErrWalletNotFound    = errors.New("wallet not found")
	ErrAccountNotFound   = errors.New("account not found")
	ErrOrderNotFound     = errors.New("order not found")
	ErrCurrencyNotFound  = errors.New("currency not found")
	ErrChainNotFound     = errors.New("chain not found")
	ErrPortfolioNotFound = errors.New("portfolio not found")

	// Other specific errors
	ErrInvalidUserID   = errors.New("invalid user ID")
	ErrNoWalletAddress = errors.New("account has no wallet address")
	ErrContextDeadline = errors.New("context deadline exceeded")
)
