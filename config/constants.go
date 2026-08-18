package config

// Cache keys
const (
	// USER_PORTFOLIO_CACHE_KEY is the cache key prefix for user portfolio data
	USER_PORTFOLIO_CACHE_KEY    = "user_portfolio"
	WALLET_PORTFOLIO_CACHE_KEY  = "wallet_portfolio"
	ACCOUNT_PORTFOLIO_CACHE_KEY = "account_portfolio"
)

// getEnvOrDefault gets an environment variable value or returns the default if not set
// func getEnvOrDefault(key, defaultValue string) string {
// 	value := os.Getenv(key)
// 	if value == "" {
// 		return defaultValue
// 	}
// 	return value
// }
