# Block Analysis Debugging Script

This script helps you analyze any block on the BSC (Binance Smart Chain) to find transactions and token transfers involving specific addresses.

## Prerequisites

1. Set up your environment variables in `.env` file in the project root:

   ```bash
   MONGODB_URI="your_mongodb_connection_string"
   MONGODB_DATABASE="your_database_name"
   ANKR_API_KEY="your_ankr_api_key_here"

   # Optional Redis settings (defaults provided)
   REDIS_HOST="localhost"
   REDIS_PORT="6379"
   REDIS_PASSWORD=""
   REDIS_DB="0"
   ```

2. The script will automatically find your `.env` file in the project root.

## Usage

### Basic Block Analysis

```bash
cd scripts
go run debug_block_simple.go <block_number>
```

Example:

```bash
go run debug_block_simple.go 52534867
```

### Search for Specific Addresses

```bash
go run debug_block_simple.go <block_number> <target_address1> [target_address2] ...
```

Example (your specific case):

```bash
go run debug_block_simple.go 52534867 0x24f150521C1B0DB290EE611B2E051FAA8E821024
```

### Multiple Address Search

```bash
go run debug_block_simple.go 52534867 0x24f150521C1B0DB290EE611B2E051FAA8E821024 0xAnotherAddress
```

## What the Script Does

1. **Fetches Block Data**: Gets all transactions in the specified block
2. **Analyzes Native Transactions**: Shows BNB transfers
3. **Analyzes Token Transfers**: Shows ERC-20 token transfers (like USDT, BUSD, etc.)
4. **Case-Insensitive Matching**: Finds your target addresses regardless of case
5. **Database Matching**: Shows which addresses match deposit accounts in your system
6. **Detailed Output**: Provides JSON output plus human-readable summary

## Output Sections

### 🔍 Detailed Block Analysis

- Complete JSON data of all transactions and transfers

### 📊 Summary

- Total transaction counts
- Native vs token transfer breakdown
- Number of matched deposit accounts

### 🎯 Found Target Transactions

- Shows transactions involving your specified addresses
- Includes transaction hash, from/to addresses, value, and type

### 💰 Deposit Accounts Found

- Lists any addresses in the block that match deposit accounts in your database

## Address Case Sensitivity Fix

The script ensures proper case-insensitive address matching by:

1. **Normalizing all addresses to lowercase** before comparison
2. **Using regex fallback** for database queries if exact lowercase matching fails
3. **Logging the normalization process** for debugging

This fixes issues where:

- Target addresses might be provided in mixed case
- Database addresses might be stored in different cases
- Blockchain addresses naturally have mixed case

## Example Output

```
🎯 Looking for specific addresses: [0x24f150521C1B0DB290EE611B2E051FAA8E821024]
✅ Loaded .env from: ../..env
✅ Connected to MongoDB
✅ Connected to Redis
🔍 Analyzing block 52534867 on BSC chain...

================================================================================
🔍 DETAILED BLOCK ANALYSIS FOR BLOCK 52534867
================================================================================
{
  "blockNumber": "52534867",
  "totalTxCount": 45,
  "nativeTxCount": 30,
  "tokenTxCount": 15,
  "foundTargets": [
    {
      "hash": "0x...",
      "from": "0x...",
      "to": "0x24f150521c1b0db290ee611b2e051faa8e821024",
      "type": "token",
      "tokenAddress": "0x55d398326f99059ff775485246999027b3197955"
    }
  ]
}

--------------------------------------------------------------------------------
📊 SUMMARY:
Total transactions: 45
Native transactions: 30
Token transfers: 15
Target addresses found: 1

🎯 FOUND TARGET TRANSACTIONS:
  ✅ Hash: 0x...
     From: 0x...
     To: 0x24f150521c1b0db290ee611b2e051faa8e821024
     Value: 0x0f4240 (1.000000)
     Type: token
     Token Address: 0x55d398326f99059ff775485246999027b3197955

--------------------------------------------------------------------------------
✅ Analysis complete!
```

## Troubleshooting

### "No target transactions found"

- The transaction might be in a different block
- Try checking blocks around the target block number
- Verify the address is correct

### "Required environment variable X is not set"

- Make sure your `.env` file is in the project root (not in the scripts directory)
- Check that all required variables are set: `MONGODB_URI`, `MONGODB_DATABASE`, `ANKR_API_KEY`

### "Failed to connect to MongoDB/Redis"

- Check your database configuration in `.env`
- Ensure services are running
- Redis connection failure is non-fatal - script will continue without Redis

## Benefits Over Complex Version

This simplified script:

- ✅ **No complex system config** - just needs 3 environment variables
- ✅ **Auto-finds .env file** in project root
- ✅ **Optional Redis** - continues without Redis if connection fails
- ✅ **Clear error messages** - tells you exactly what's missing
- ✅ **Same functionality** - all the analysis features you need

## Enhanced Processor Code

The processor code has also been enhanced with better case-insensitive address matching:

1. **Primary Strategy**: Fast lowercase exact matching
2. **Fallback Strategy**: Case-insensitive regex matching for stored addresses with mixed case
3. **Logging**: Debug information when fallback matching is used

This ensures that your deposit detection system will work regardless of how addresses are stored in the database or provided by users.
