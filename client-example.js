// Example gRPC client for ImportAccounts
// Note: You'll need to install appropriate gRPC packages in your project

// First, load the proto definitions
// const protoLoader = require('@grpc/proto-loader');
// const grpc = require('@grpc/grpc-js');
// const packageDefinition = protoLoader.loadSync('proto/wallet/import.proto');
// const wallet = grpc.loadPackageDefinition(packageDefinition).wallet;

// Sample account data
const accounts = [
  {
    walletId: "680678f8c60921292e370583",
    userId: "68015798e772870c161410d8",
    supportedCurrencyId: "65f39b18b6b4ac28a1e3e3e9",
    walletAddress: "0x2887682C636F666e1DC75dB9202D85A6E76ed5D2",
    hashedPrivateKey: "encrypted-key-1",
  },
];

// THIS IS THE CORRECTED FORMAT:
// The accounts array should contain properly formatted objects
const accountsData = accounts.map((account) => ({
  wallet_id: account.walletId,
  user_id: account.userId,
  currency_id: account.supportedCurrencyId, // Make sure field name matches proto definition
  wallet_address: account.walletAddress,
  encrypted_private_key: account.hashedPrivateKey,
  balance: 0,
}));

// IMPORTANT: The request object must be structured properly
const request = {
  accounts: accountsData, // This must be an array of account objects
  callback_url: "http://localhost:8080/callback",
};

console.log("CORRECT gRPC request format:");
console.log(JSON.stringify(request, null, 2));

// Example of correct client usage:
/*
const client = new wallet.AccountImportService('localhost:50051', grpc.credentials.createInsecure());

client.ImportAccounts(request, (error, response) => {
  if (error) {
    console.error('Error:', error);
    return;
  }
  console.log('Import response:', response);
});
*/
