package config

// ChangeNowConfig holds configuration for the ChangeNow provider
type ChangeNowConfig struct {
	// APIKey is the ChangeNow API key
	APIKey string `json:"apiKey" yaml:"apiKey"`

	// APIURL is the base URL for the ChangeNow API
	APIURL string `json:"apiUrl" yaml:"apiUrl"`

	// OrderMinAmount is the minimum order amount in USD
	OrderMinAmount float64 `json:"orderMinAmount" yaml:"orderMinAmount"`

	// OrderMaxAmount is the maximum order amount in USD
	OrderMaxAmount float64 `json:"orderMaxAmount" yaml:"orderMaxAmount"`
}
