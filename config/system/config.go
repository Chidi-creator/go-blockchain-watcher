package config

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all configuration for the application
type Config struct {
	LogLevel    string           `yaml:"logLevel" env:"LOG_LEVEL" env-default:"info"`
	Environment string           `yaml:"environment" env:"ENVIRONMENT" env-default:"development"`
	Server      ServerConfig     `yaml:"server"`
	GrpcServer  GrpcServerConfig `yaml:"grpcServer"`
	MongoDB     MongoDBConfig    `yaml:"mongodb"`
	Redis       RedisConfig      `yaml:"redis"`
	ChangeNow   ChangeNowConfig  `yaml:"changeNow"`
	// Chains holds chain-specific configuration for providers
	Chains ChainsConfig `yaml:"chains"`
	// Blockchain holds settings for blockchain workers
	Blockchain BlockchainConfig `yaml:"blockchain"`
	// NodeServer holds configuration for connecting to a Node.js WebSocket server
	NodeServer           *NodeServerConfig `yaml:"nodeServer"`
	EvmConcurrency       int               `yaml:"evmConcurrency"`
	ChangeNowConcurrency int               `yaml:"changeNowConcurrency"`
	SolConcurrency       int               `yaml:"solConcurrency"`
	BTCConcurrency       int               `yaml:"btcConcurrency"`
	TRXConcurrency       int               `yaml:"trxConcurrency"`
	EVMScheduledTime     int               `yaml:"evmScheduledTime"`
	SolScheduledTime     int               `yaml:"solScheduledTime"`
	BTCScheduledTime     int               `yaml:"btcScheduledTime"`
	TRXScheduledTime     int               `yaml:"trxScheduledTime"`
}

// ServerConfig represents HTTP server configuration
type ServerConfig struct {
	Port         string
	Timeout      time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// GrpcServerConfig represents gRPC server configuration
type GrpcServerConfig struct {
	Port        int
	EnableTLS   bool
	TLSCertFile string
	TLSKeyFile  string
}

// MongoDBConfig represents MongoDB configuration
type MongoDBConfig struct {
	URI      string
	Database string
}

// RedisConfig represents Redis configuration
type RedisConfig struct {
	Host            string
	Port            string
	Password        string
	Username        string
	DB              int
	PoolSize        int           // Maximum number of connections in the pool
	MinIdleConns    int           // Minimum number of idle connections
	DialTimeout     time.Duration // Timeout for establishing new connections
	ReadTimeout     time.Duration // Timeout for socket reads
	WriteTimeout    time.Duration // Timeout for socket writes
	PoolTimeout     time.Duration // Timeout for getting connection from pool
	ConnMaxLifetime time.Duration // Maximum lifetime of connections in the pool
}

// NodeServerConfig represents configuration for connecting to a Node.js WebSocket server
type NodeServerConfig struct {
	URL       string
	AuthToken string
	UserID    string
	Enabled   bool
}

// ChainsConfig holds blockchain provider configuration
type ChainsConfig struct {
	Bitcoin BitcoinConfig `yaml:"bitcoin"`
	EVM     EVMConfig     `yaml:"evm"`
	Solana  SolanaConfig  `yaml:"solana"`
	Tron    TronConfig    `yaml:"tron"`
}

// BitcoinConfig holds Bitcoin specific configuration
type BitcoinConfig struct {
	RPCEndpoint string `yaml:"rpcEndpoint" env:"BITCOIN_RPC_ENDPOINT" env-default:"https://blockchain.info"`
	Network     string `yaml:"network" env:"BITCOIN_NETWORK" env-default:"mainnet"`
}

// EVMConfig holds Ethereum and EVM compatible chains configuration
type EVMConfig struct {
	Endpoints  map[string]string `yaml:"endpoints" env:"EVM_ENDPOINTS" env-default:"1:https://ethereum.publicnode.com"` // comma-separated key:url
	AnkrAPIKey string            `yaml:"ankrApiKey" env:"ANKR_API_KEY" env-default:""`
}

// SolanaConfig holds Solana specific configuration
type SolanaConfig struct {
	RPCEndpoint     string `yaml:"rpcEndpoint" env:"SOLANA_RPC_ENDPOINT" env-default:"https://rpc.ankr.com/solana"`
	WebSocketURL    string `yaml:"websocketUrl" env:"SOLANA_WS_ENDPOINT" env-default:"wss://rpc.ankr.com/solana"`
	CommitmentLevel string `yaml:"commitmentLevel" env:"SOLANA_COMMITMENT" env-default:"confirmed"`
	AnkrAPIKey      string `yaml:"ankrApiKey" env:"SOLANA_ANKR_API_KEY" env-default:""`
}

// TronConfig holds Tron specific configuration
type TronConfig struct {
	ApiKey       string `yaml:"apiKey" env:"TRON_API_KEY" env-default:""`
	HTTPEndpoint string `yaml:"httpEndpoint" env:"TRON_HTTP_ENDPOINT" env-default:"https://api.trongrid.io"`
	FullNodeURL  string `yaml:"fullNodeUrl" env:"TRON_FULL_NODE_URL" env-default:"https://api.trongrid.io"`
}

// BlockchainConfig holds configuration for blockchain workers
type BlockchainConfig struct {
	SolanaRpcURL  string `yaml:"solanaRpcURL" env:"SOLANA_RPC_URL" env-default:"https://rpc.ankr.com/solana"`
	TronAPIURL    string `yaml:"tronApiURL" env:"TRON_API_URL" env-default:"https://api.trongrid.io"`
	TronAPIKey    string `yaml:"tronApiKey" env:"TRON_API_KEY" env-default:""`
	SolanaAnkrKey string `yaml:"solanaAnkrKey" env:"SOLANA_ANKR_API_KEY" env-default:""`
	EnableBitcoin bool   `yaml:"enableBitcoin" env:"ENABLE_BITCOIN" env-default:"true"`
	EnableEVM     bool   `yaml:"enableEVM" env:"ENABLE_EVM" env-default:"true"`
	EnableSolana  bool   `yaml:"enableSolana" env:"ENABLE_SOLANA" env-default:"true"`
	EnableTron    bool   `yaml:"enableTron" env:"ENABLE_TRON" env-default:"true"`
}

type SolanaEndpointConfig struct {
	RPCURL string
	WSURL  string
}

// Load loads configuration from environment variables or .env file
func Load() (*Config, error) {
	// Load .env file if it exists
	err := godotenv.Load()
	if err != nil {
		log.Printf("Warning: .env file not loaded: %v\n", err)
	}

	endpoints, err := getSolanaEndpoints()
	if err != nil {
		fmt.Printf("Error getting Solana endpoints: %v\n", err)
		return nil, err
	}

	// Check if Node.js server is enabled
	nodeServerEnabled := getBoolEnv("NODE_SERVER_ENABLED", false)

	var nodeServerConfig *NodeServerConfig

	if nodeServerEnabled {
		nodeServerConfig = &NodeServerConfig{
			URL:       getEnvOrThrowError("NODE_SERVER_URL"),
			AuthToken: getEnvOrThrowError("NODE_SERVER_AUTH_TOKEN"),
			UserID:    getEnvOrThrowError("NODE_SERVER_USER_ID"),
			Enabled:   true,
		}
	}

	cfg := &Config{
		Server: ServerConfig{
			Port:         getEnvOrThrowError("PORT"),
			Timeout:      getDurationEnv("SERVER_TIMEOUT", 30*time.Second),
			ReadTimeout:  getDurationEnv("SERVER_READ_TIMEOUT", 15*time.Second),
			WriteTimeout: getDurationEnv("SERVER_WRITE_TIMEOUT", 15*time.Second),
		},
		GrpcServer: GrpcServerConfig{
			Port:        getIntEnv("GRPC_PORT", 50051),
			EnableTLS:   getBoolEnv("GRPC_TLS_ENABLED", false),
			TLSCertFile: getEnv("GRPC_TLS_CERT_FILE", ""),
			TLSKeyFile:  getEnv("GRPC_TLS_KEY_FILE", ""),
		},
		MongoDB: MongoDBConfig{
			URI:      getEnvOrThrowError("MONGODB_URI"),
			Database: getEnvOrThrowError("MONGODB_DATABASE"),
		},
		LogLevel: getEnv("LOG_LEVEL", "info"),
		Redis: RedisConfig{
			Host:            getEnvOrThrowError("REDIS_HOST"),
			Port:            getEnvOrThrowError("REDIS_PORT"),
			Password:        getEnvOrThrowError("REDIS_PASSWORD"),
			Username:        getEnvOrThrowError("REDIS_USERNAME"),
			DB:              getIntEnv("REDIS_DB", 0),
			PoolSize:        getIntEnv("REDIS_POOL_SIZE", 1000),
			MinIdleConns:    getIntEnv("REDIS_MIN_IDLE_CONNS", 10),
			DialTimeout:     getDurationEnv("REDIS_DIAL_TIMEOUT", 5*time.Second),
			ReadTimeout:     getDurationEnv("REDIS_READ_TIMEOUT", 3*time.Second),
			WriteTimeout:    getDurationEnv("REDIS_WRITE_TIMEOUT", 3*time.Second),
			PoolTimeout:     getDurationEnv("REDIS_POOL_TIMEOUT", 4*time.Second),
			ConnMaxLifetime: getDurationEnv("REDIS_CONN_MAX_LIFETIME", 30*time.Minute),
		},
		Chains: ChainsConfig{
			Bitcoin: BitcoinConfig{
				RPCEndpoint: getEnv("BITCOIN_RPC_ENDPOINT", "https://blockchain.info"),
				Network:     getEnv("BITCOIN_NETWORK", "mainnet"),
			},
			EVM: EVMConfig{
				Endpoints:  parseEVMEndpoints(getEnv("EVM_ENDPOINTS", "1:https://ethereum.publicnode.com")),
				AnkrAPIKey: getEnv("ANKR_API_KEY", ""),
			},
			Solana: SolanaConfig{
				RPCEndpoint:     getEnv("SOLANA_RPC_ENDPOINT", endpoints.RPCURL),
				WebSocketURL:    getEnv("SOLANA_WS_ENDPOINT", endpoints.WSURL),
				CommitmentLevel: getEnv("SOLANA_COMMITMENT", "confirmed"),
				AnkrAPIKey:      getEnv("SOLANA_ANKR_API_KEY", ""),
			},
			Tron: TronConfig{
				ApiKey:       getEnv("TRON_API_KEY", ""),
				HTTPEndpoint: getEnvOrThrowError("TRON_HTTP_ENDPOINT"),
				FullNodeURL:  getEnvOrThrowError("TRON_FULL_NODE_URL"),
			},
		},
		Blockchain: BlockchainConfig{
			SolanaRpcURL:  getEnv("SOLANA_RPC_URL", endpoints.RPCURL),
			TronAPIURL:    getEnvOrThrowError("TRON_API_URL"),
			TronAPIKey:    getEnvOrThrowError("TRON_API_KEY"),
			SolanaAnkrKey: getEnvOrThrowError("SOLANA_ANKR_API_KEY"),
			EnableBitcoin: getBoolEnv("ENABLE_BITCOIN", true),
			EnableEVM:     getBoolEnv("ENABLE_EVM", true),
			EnableSolana:  getBoolEnv("ENABLE_SOLANA", true),
			EnableTron:    getBoolEnv("ENABLE_TRON", true),
		},
		NodeServer:           nodeServerConfig,
		EvmConcurrency:       getIntEnv("EVM_CONCURRENCY", 2),
		ChangeNowConcurrency: getIntEnv("CHANGE_NOW_CONCURRENCY", 1),
		SolConcurrency:       getIntEnv("SOL_CONCURRENCY", 2),
		BTCConcurrency:       getIntEnv("BTC_CONCURRENCY", 1),
		TRXConcurrency:       getIntEnv("TRX_CONCURRENCY", 1),
		EVMScheduledTime:     getIntEnv("EVM_SCHEDULED_TIME", 3),
		SolScheduledTime:     getIntEnv("SOL_SCHEDULED_TIME", 2),
		BTCScheduledTime:     getIntEnv("BTC_SCHEDULED_TIME", 4),
		TRXScheduledTime:     getIntEnv("TRX_SCHEDULED_TIME", 2),
	}

	return cfg, nil
}

// Helper functions to get environment variables with default values
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getSolanaEndpoints() (*SolanaEndpointConfig, error) {
	// Get all configuration from environment
	baseURL := os.Getenv("SOLANA_ANKR_BASE_URL") // e.g. "rpc.ankr.com/solana"
	apiKey := os.Getenv("SOLANA_ANKR_API_KEY")

	if baseURL == "" {
		return nil, fmt.Errorf("SOLANA_ANKR_BASE_URL environment variable is required")
	}

	// Ensure baseURL doesn't include protocol
	baseURL = strings.TrimPrefix(baseURL, "https://")
	baseURL = strings.TrimPrefix(baseURL, "http://")
	baseURL = strings.TrimPrefix(baseURL, "wss://")
	baseURL = strings.TrimPrefix(baseURL, "ws://")

	// Construct URLs
	rpcURL, err := buildEndpointURL("https", baseURL, apiKey)
	if err != nil {
		return nil, fmt.Errorf("failed to build RPC URL: %w", err)
	}

	wsURL, err := buildEndpointURL("wss", baseURL, apiKey)
	if err != nil {
		return nil, fmt.Errorf("failed to build WebSocket URL: %w", err)
	}

	return &SolanaEndpointConfig{
		RPCURL: rpcURL,
		WSURL:  wsURL,
	}, nil
}

func buildEndpointURL(scheme, baseURL, apiKey string) (string, error) {
	u, err := url.Parse(fmt.Sprintf("%s://%s", scheme, baseURL))
	if err != nil {
		return "", fmt.Errorf("invalid base URL: %w", err)
	}

	if apiKey != "" {
		u.Path = strings.TrimSuffix(u.Path, "/") + "/" + apiKey
	}

	return u.String(), nil
}

func getEnvOrThrowError(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is not set", key)
	}
	return value
}

func getIntEnv(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}

// Helper function to get boolean environment variable with fallback
func getBoolEnv(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	result, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return result
}

func getDurationEnv(key string, defaultValue time.Duration) time.Duration {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := time.ParseDuration(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}

func parseEVMEndpoints(input string) map[string]string {
	endpoints := make(map[string]string)

	if input == "" {
		return endpoints
	}

	pairs := strings.Split(input, ",")
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) == 2 {
			endpoints[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	return endpoints
}
