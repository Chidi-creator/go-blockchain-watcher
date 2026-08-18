package constants

// Tron token addresses
const (
	// TronUSDT is the address of USDT on Tron network
	TronUSDT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"

	// Other common Tron tokens
	TronUSDC = "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8"
	TronBTC  = "TN3W4H6rK2ce4vX9YnFQHwKENnHjoxb3m9"
	TronETH  = "THb4CqiFdwNHsWsQCs4JhzwjMWys4aqCbF"
)

// Token decimals for common tokens
const (
	// Tron token decimals
	TronUSDTDecimals = 6
	TronUSDCDecimals = 6
	TronBTCDecimals  = 8
	TronETHDecimals  = 18

	// Default token decimals by chain
	DefaultTronTokenDecimals   = 6
	DefaultEVMTokenDecimals    = 18
	DefaultSolanaTokenDecimals = 9

	// Native token decimals
	SolDecimals = 9  // SOL uses 9 decimals - 1 SOL = 1,000,000,000 lamports
	TrxDecimals = 6  // TRX uses 6 decimals - 1 TRX = 1,000,000 sun
	EthDecimals = 18 // ETH uses 18 decimals - 1 ETH = 10^18 wei
)

// Token names and symbols
const (
	// USDT token details
	USDTName   = "Tether USD"
	USDTSymbol = "USDT"

	// USDC token details
	USDCName   = "USD Coin"
	USDCSymbol = "USDC"
)
