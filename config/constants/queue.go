package constants

// Queue names for blockchain monitoring
const (
	// Queue names for blockchain monitoring
	QueueBitcoinBlockMonitor = "BitcoinBlockQueue"
	QueueEVMBlockMonitor     = "EVMBlockQueue"
	QueueSolanaBlockMonitor  = "SolanaBlockMonitor"
	QueueTronBlockMonitor    = "TronBlockMonitor"
	QueueChangeNowWatcher    = "ChangeNowWatcherQueue"
	// Cache keys for last processed block heights
	BitcoinBlockIdentifier = "bitcoin:last-block-height"
	EVMBlockIdentifier     = "evm:last-block-height"
	SolanaBlockIdentifier  = "solana:last-block-height"
	TronBlockIdentifier    = "tron:last-block-height"
)

// QueueNames maps queue constant names to their string values for dynamic lookup
var QueueNames = map[string]string{
	"QueueBitcoinBlockMonitor": QueueBitcoinBlockMonitor,
	"QueueEVMBlockMonitor":     QueueEVMBlockMonitor,
	"QueueSolanaBlockMonitor":  QueueSolanaBlockMonitor,
	"QueueTronBlockMonitor":    QueueTronBlockMonitor,
	"QueueChangeNowWatcher":    QueueChangeNowWatcher,
	// Solana transaction processor is added dynamically in run_solana_worker
	// but defined here for consistency
	"QueueSolanaTransactionProcessor": "solana:transaction:processor",
}
