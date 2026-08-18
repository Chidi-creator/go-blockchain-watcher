package changenow

const (
	// ProviderName is the name of the provider
	ProviderName = "changenow"

	// ChangeNowAPIBase is the base URL for the ChangeNow API
	ChangeNowAPIBase = "https://api.changenow.io"

	// ChangeNowAPIVersion is the API version
	ChangeNowAPIVersion = "v2"

	// ChangeNowStatusCompleted is the status for a completed transaction
	ChangeNowStatusCompleted = "finished"

	// ChangeNowStatusRefunded is the status for a refunded transaction
	ChangeNowStatusRefunded = "refunded"

	// ChangeNowStatusFailed is the status for a failed transaction
	ChangeNowStatusFailed = "failed"

	// ChangeNowStatusNew is the status for a new transaction
	ChangeNowStatusNew = "new"

	// ChangeNowStatusWaitingForInput is the status for a transaction waiting for input
	ChangeNowStatusWaitingForInput = "waiting"

	// ChangeNowStatusConfirming is the status for a confirming transaction
	ChangeNowStatusConfirming = "confirming"

	// ChangeNowStatusExchanging is the status for an exchanging transaction
	ChangeNowStatusExchanging = "exchanging"

	// ChangeNowStatusSending is the status for a sending transaction
	ChangeNowStatusSending = "sending"

	// ChangeNowTypeFixed is the type for a fixed rate exchange
	ChangeNowTypeFixed = "fixed"

	// ChangeNowTypeFloat is the type for a floating rate exchange
	ChangeNowTypeFloat = "floating"

	// ChangeNowFlowStandard is the flow for a standard exchange
	ChangeNowFlowStandard = "standard"

	// ChangeNowFlowDirect is the flow for a direct exchange
	ChangeNowFlowDirect = "direct"

	// ChangeNowURLAvailableCurrencies is the endpoint for available currencies
	ChangeNowURLAvailableCurrencies = "/currencies"

	// ChangeNowURLExchangeRates is the endpoint for exchange rates
	ChangeNowURLExchangeRates = "/exchange/fixed-rate"

	// ChangeNowURLRange is the endpoint for min/max ranges
	ChangeNowURLRange = "/exchange/fixed-rate/range"

	// ChangeNowURLCreateExchange is the endpoint for creating an exchange
	ChangeNowURLCreateExchange = "/exchange/fixed-rate"

	// ChangeNowURLTransactionStatus is the endpoint for transaction status
	ChangeNowURLTransactionStatus = "/exchange/fixed-rate/"

	// TransactionWatcher is the name of the transaction watcher job
	TransactionWatcher = "changenow-transaction-watcher"

	// TransactionWatcherInterval is the interval for the transaction watcher job in seconds
	TransactionWatcherInterval = 60

	// DefaultJobOptions contains default options for queue jobs
	DefaultJobOptions = "default"
)
