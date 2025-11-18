import dotenv from "dotenv";
import { ethers } from "ethers";
import fs from "fs";
import express from "express";
import cors from "cors";
import path from "path";

dotenv.config();

const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;
const WS = process.env.WEBSOCKET_URL || "wss://monad-testnet.g.alchemy.com/v2/Y_NVhe3hqReREbcOEa0_1Dw5_sPAjmfa";

const __dirname = path.resolve();

// Load ABI with error handling
let abi;
try {
  abi = JSON.parse(fs.readFileSync(path.join(__dirname, "stakingABI.json"), "utf8"));
  console.log("✅ ABI loaded successfully");
} catch (error) {
  console.error("❌ Failed to load ABI:", error.message);
  process.exit(1);
}

// Validate environment variables
if (!CONTRACT_ADDRESS) {
  console.error("❌ CONTRACT_ADDRESS not found in .env file!");
  process.exit(1);
}

// Global variables
let provider = null;
let contract = null;
let reconnectTimeout = null;
let isReconnecting = false;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;

// Staking statistics - Tracks ALL activities
let stakingStats = {
  totalStakeTransactions: 0,     // Har stake transaction count
  totalUnstakeTransactions: 0,   // Har unstake transaction count
  currentStakedCount: 0,         // Abhi kitne NFTs staked hain
  lastUpdated: new Date().toISOString()
};

let recentEvents = [];
const MAX_RECENT_EVENTS = 50;

// Track currently staked NFTs (for real-time count)
const activeStakes = new Map(); // key: "tokenAddress-tokenId", value: user address

const createEventLog = (type, user, erc721Token, tokenId, event) => {
  const sanitizedType = type || "UNKNOWN";
  const sanitizedUser = user || "0x0000000000000000000000000000000000000000";
  const sanitizedErc721Token = erc721Token || "0x0000000000000000000000000000000000000000";
  const sanitizedTokenId = tokenId?.toString() || "0";
  
  const txHash = event?.transactionHash || 
    `${sanitizedType}-${sanitizedErc721Token}-${sanitizedTokenId}-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;

  return {
    type: sanitizedType,
    user: sanitizedUser,
    erc721Token: sanitizedErc721Token,
    tokenId: sanitizedTokenId,
    txHash: txHash,
    timestamp: new Date().toISOString(),
    blockNumber: event?.blockNumber || 0,
    uniqueId: `${txHash}-${Date.now()}`
  };
};

// Initialize provider with proper error handling
const initializeProvider = async () => {
  try {
    if (provider) {
      console.log("🔄 Cleaning up old provider...");
      try {
        provider.removeAllListeners();
        await provider.destroy();
      } catch (e) {
        console.log("Provider cleanup warning:", e.message);
      }
    }

    console.log("🔌 Connecting to WebSocket...");
    console.log(`🔗 URL: ${WS.substring(0, 50)}...`);
    
    provider = new ethers.WebSocketProvider(WS);
    
    // Wait for connection with timeout
    const timeout = new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Connection timeout')), 10000)
    );
    
    const connection = provider.getNetwork();
    const network = await Promise.race([connection, timeout]);
    
    console.log("✅ WebSocket connected!");
    console.log("✅ Network:", network.name, "Chain ID:", network.chainId.toString());
    
    contract = new ethers.Contract(CONTRACT_ADDRESS, abi, provider);
    console.log("✅ Contract instance created:", CONTRACT_ADDRESS);

    // Setup WebSocket event handlers
    provider.websocket.on('error', (error) => {
      console.error('❌ WebSocket error:', error.message);
      scheduleReconnect();
    });

    provider.websocket.on('close', (code, reason) => {
      console.log(`⚠️ WebSocket closed (code: ${code}, reason: ${reason || 'No reason'})`);
      scheduleReconnect();
    });

    provider.websocket.on('open', () => {
      console.log('✅ WebSocket connection opened');
      reconnectAttempts = 0; // Reset on successful connection
    });

    return true;
  } catch (error) {
    console.error("❌ Error initializing provider:", error.message);
    scheduleReconnect();
    return false;
  }
};

// Improved reconnection logic with exponential backoff
const scheduleReconnect = () => {
  if (isReconnecting) {
    console.log("⏳ Reconnection already scheduled, skipping...");
    return;
  }
  
  if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
    console.error(`❌ Max reconnection attempts (${MAX_RECONNECT_ATTEMPTS}) reached. Please check your connection.`);
    return;
  }
  
  isReconnecting = true;
  reconnectAttempts++;
  
  // Exponential backoff: 5s, 10s, 20s, 40s, etc. (max 60s)
  const delay = Math.min(5000 * Math.pow(2, reconnectAttempts - 1), 60000);
  
  console.log(`🔄 Scheduling reconnection attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS} in ${delay/1000}s...`);
  
  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
  }
  
  reconnectTimeout = setTimeout(async () => {
    if (provider) {
      try {
        if (provider.websocket && provider.websocket.readyState === 1) {
          console.log("✅ WebSocket already connected, skipping reconnect");
          isReconnecting = false;
          reconnectAttempts = 0;
          return;
        }
      } catch (e) {
        // Continue with reconnection
      }
    }
    
    console.log(`🔄 Attempting reconnection (${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})...`);
    isReconnecting = false;
    await initialize();
  }, delay);
};

// Initialize stats from historical events
const initializeStats = async () => {
  try {
    console.log("📊 Initializing staking statistics from blockchain...");
    
    if (!contract) {
      console.error("❌ Contract not initialized!");
      return;
    }

    const currentBlock = await provider.getBlockNumber();
    console.log(`📦 Current block: ${currentBlock}`);
    
    // Query last 2000 blocks (adjust based on your RPC limits)
    const BLOCK_RANGE = 2000;
    const fromBlock = Math.max(0, currentBlock - BLOCK_RANGE);
    
    console.log(`🔍 Querying events from block ${fromBlock} to ${currentBlock}...`);
    
    let pastStakeEvents = [];
    let pastWithdrawEvents = [];
    
    // Retry logic for querying events
    const maxRetries = 3;
    let lastError = null;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        console.log(`📡 Query attempt ${attempt}/${maxRetries}...`);
        
        const stakeFilter = contract.filters.Staked();
        pastStakeEvents = await contract.queryFilter(stakeFilter, fromBlock, currentBlock);
        
        const withdrawFilter = contract.filters.Withdrawn();
        pastWithdrawEvents = await contract.queryFilter(withdrawFilter, fromBlock, currentBlock);
        
        console.log(`✅ Query successful!`);
        break; // Success, exit retry loop
        
      } catch (error) {
        lastError = error;
        console.error(`❌ Query attempt ${attempt} failed:`, error.message);
        
        if (attempt === maxRetries) {
          console.error("❌ All query attempts failed, starting with empty history");
          console.error("   Error details:", lastError.message);
          pastStakeEvents = [];
          pastWithdrawEvents = [];
        } else {
          const waitTime = 2000 * attempt;
          console.log(`⏳ Waiting ${waitTime/1000}s before retry...`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
        }
      }
    }
    
    console.log(`✅ Found ${pastStakeEvents.length} stake events`);
    console.log(`✅ Found ${pastWithdrawEvents.length} withdraw events`);
    
    // Clear existing data
    activeStakes.clear();
    recentEvents = [];
    
    // Count ALL transactions (every activity counts)
    stakingStats.totalStakeTransactions = pastStakeEvents.length;
    stakingStats.totalUnstakeTransactions = pastWithdrawEvents.length;
    
    // Process events chronologically to build current state
    const allEvents = [
      ...pastStakeEvents.map(e => ({
        type: 'STAKE',
        user: e.args.user,
        token: e.args.erc721Token,
        tokenId: e.args.tokenId.toString(),
        blockNumber: e.blockNumber,
        transactionIndex: e.transactionIndex,
        event: e
      })),
      ...pastWithdrawEvents.map(e => ({
        type: 'WITHDRAW',
        user: e.args.user,
        token: e.args.erc721Token,
        tokenId: e.args.tokenId.toString(),
        blockNumber: e.blockNumber,
        transactionIndex: e.transactionIndex,
        event: e
      }))
    ].sort((a, b) => {
      // Sort by block number, then by transaction index
      if (a.blockNumber !== b.blockNumber) {
        return a.blockNumber - b.blockNumber;
      }
      return a.transactionIndex - b.transactionIndex;
    });
    
    console.log(`🔄 Processing ${allEvents.length} total events...`);
    
    // Build current state and recent events
    for (const evt of allEvents) {
      const nftKey = `${evt.token}-${evt.tokenId}`;
      
      if (evt.type === 'STAKE') {
        // Add to active stakes
        activeStakes.set(nftKey, evt.user);
      } else if (evt.type === 'WITHDRAW') {
        // Remove from active stakes
        activeStakes.delete(nftKey);
      }
      
      // Add ALL events to recent events list
      const log = createEventLog(evt.type, evt.user, evt.token, evt.tokenId, evt.event);
      recentEvents.push(log);
    }
    
    // Keep only last MAX_RECENT_EVENTS (most recent first)
    recentEvents = recentEvents.slice(-MAX_RECENT_EVENTS).reverse();
    
    // Update current staked count
    stakingStats.currentStakedCount = activeStakes.size;
    stakingStats.lastUpdated = new Date().toISOString();
    
    console.log("✅ ========== STATS INITIALIZED ==========");
    console.log(`📊 Total Stake Transactions: ${stakingStats.totalStakeTransactions}`);
    console.log(`📊 Total Unstake Transactions: ${stakingStats.totalUnstakeTransactions}`);
    console.log(`📊 Currently Staked NFTs: ${stakingStats.currentStakedCount}`);
    console.log(`📋 Recent Events Loaded: ${recentEvents.length}`);
    console.log("========================================");
    
  } catch (error) {
    console.error("❌ Fatal error initializing stats:", error);
    // Set safe defaults
    activeStakes.clear();
    stakingStats.totalStakeTransactions = 0;
    stakingStats.totalUnstakeTransactions = 0;
    stakingStats.currentStakedCount = 0;
  }
};

// Setup event listeners for real-time updates
const setupEventHandlers = () => {
  try {
    if (!contract) {
      console.error("❌ Cannot setup handlers: Contract not initialized!");
      return;
    }

    // Remove any existing listeners to prevent duplicates
    try {
      contract.removeAllListeners();
      console.log("🧹 Cleaned up old event listeners");
    } catch (e) {
      console.log("⚠️ Listener cleanup warning:", e.message);
    }
    
    console.log("🎧 Setting up real-time event listeners...");

    // Listen for Stake events
    contract.on("Staked", (...args) => {
      try {
        const event = args[args.length - 1];
        const [user, erc721Token, tokenId] = args;
        
        console.log("\n🔥 ========== NEW STAKE EVENT ==========");
        console.log("👤 User:", user);
        console.log("🎨 Token Contract:", erc721Token);
        console.log("🆔 Token ID:", tokenId.toString());
        console.log("📝 Tx Hash:", event.transactionHash);
        console.log("📦 Block:", event.blockNumber);
        
        const nftKey = `${erc721Token}-${tokenId}`;
        
        // Always increment total transactions (har activity count hogi)
        stakingStats.totalStakeTransactions++;
        console.log(`📈 Total Stakes: ${stakingStats.totalStakeTransactions}`);
        
        // Update current staked count
        const wasStaked = activeStakes.has(nftKey);
        activeStakes.set(nftKey, user);
        
        if (!wasStaked) {
          stakingStats.currentStakedCount++;
          console.log(`✅ New stake added. Current staked: ${stakingStats.currentStakedCount}`);
        } else {
          console.log(`🔄 Re-stake detected. Current staked: ${stakingStats.currentStakedCount}`);
        }
        
        // Update timestamp
        stakingStats.lastUpdated = new Date().toISOString();
        
        // Add to recent events (ALL activities)
        const log = createEventLog("STAKE", user, erc721Token, tokenId, event);
        recentEvents.unshift(log);
        if (recentEvents.length > MAX_RECENT_EVENTS) {
          recentEvents.pop();
        }
        
        console.log("======================================\n");
        
      } catch (error) {
        console.error("❌ Error processing Staked event:", error);
      }
    });

    // Listen for Withdraw events
    contract.on("Withdrawn", (...args) => {
      try {
        const event = args[args.length - 1];
        const [user, erc721Token, tokenId] = args;
        
        console.log("\n🔥 ========== NEW WITHDRAW EVENT ==========");
        console.log("👤 User:", user);
        console.log("🎨 Token Contract:", erc721Token);
        console.log("🆔 Token ID:", tokenId.toString());
        console.log("📝 Tx Hash:", event.transactionHash);
        console.log("📦 Block:", event.blockNumber);
        
        const nftKey = `${erc721Token}-${tokenId}`;
        
        // Always increment total transactions
        stakingStats.totalUnstakeTransactions++;
        console.log(`📈 Total Unstakes: ${stakingStats.totalUnstakeTransactions}`);
        
        // Update current staked count
        const wasStaked = activeStakes.has(nftKey);
        
        if (wasStaked) {
          activeStakes.delete(nftKey);
          stakingStats.currentStakedCount = Math.max(0, stakingStats.currentStakedCount - 1);
          console.log(`✅ Stake removed. Current staked: ${stakingStats.currentStakedCount}`);
        } else {
          console.log(`⚠️ Unstake for non-staked NFT. Current staked: ${stakingStats.currentStakedCount}`);
        }
        
        // Update timestamp
        stakingStats.lastUpdated = new Date().toISOString();
        
        // Add to recent events (ALL activities)
        const log = createEventLog("WITHDRAW", user, erc721Token, tokenId, event);
        recentEvents.unshift(log);
        if (recentEvents.length > MAX_RECENT_EVENTS) {
          recentEvents.pop();
        }
        
        console.log("=========================================\n");
        
      } catch (error) {
        console.error("❌ Error processing Withdrawn event:", error);
      }
    });

    console.log("✅ Event handlers setup complete!");
    console.log("👂 Now listening for live events on blockchain...\n");
    
  } catch (error) {
    console.error("❌ Error setting up event handlers:", error);
  }
};

// Main initialization
const initialize = async () => {
  console.log("\n🚀 ========== INITIALIZING SYSTEM ==========");
  
  const providerReady = await initializeProvider();
  if (!providerReady) {
    console.error("❌ Provider initialization failed, will retry...");
    return;
  }
  
  await initializeStats();
  setupEventHandlers();
  
  console.log("🎉 ========== SYSTEM READY ==========\n");
};

// ==================== EXPRESS APP ====================

const app = express();

// CORS Configuration
const corsOptions = {
  origin: function (origin, callback) {
    if (!origin) {
      return callback(null, true);
    }
    
    const allowedOrigins = [
      'http://localhost:3000',
      'http://localhost:3001',
      'http://localhost:5173', // Vite default
      'http://127.0.0.1:3000',
      'https://rebelsnft.xyz',
      'https://www.rebelsnft.xyz'
    ];
    
    const isAllowed = allowedOrigins.some(allowed => origin.startsWith(allowed));
    
    if (isAllowed) {
      callback(null, true);
    } else {
      console.log('⚠️ Origin not in whitelist:', origin);
      callback(null, true); // Allow anyway for development
    }
  },
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'Accept'],
  credentials: true,
  maxAge: 86400
};

app.use(cors(corsOptions));
app.options('*', cors(corsOptions));

// Additional CORS headers middleware
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin) {
    res.setHeader('Access-Control-Allow-Origin', origin);
  }
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Max-Age', '86400');
    return res.status(200).end();
  }
  
  next();
});

app.use(express.json());

// Request logging middleware
app.use((req, res, next) => {
  console.log(`📨 ${req.method} ${req.url} - ${new Date().toISOString()}`);
  next();
});

// ==================== API ENDPOINTS ====================

// Root endpoint
app.get("/", (req, res) => {
  res.json({ 
    message: "RebelsNFT Staking API",
    version: "1.0.0",
    status: "running",
    endpoints: {
      health: "/api/health",
      stats: "/api/staking-stats",
      events: "/api/recent-events"
    },
    contract: CONTRACT_ADDRESS,
    websocketStatus: provider && provider.websocket 
      ? (provider.websocket.readyState === 1 ? "connected" : "disconnected") 
      : "not initialized",
    timestamp: new Date().toISOString()
  });
});

// Health check
app.get(["/health", "/api/health"], (req, res) => {
  const isConnected = provider && provider.websocket && provider.websocket.readyState === 1;
  
  res.json({ 
    status: "OK",
    service: "RebelsNFT Staking API",
    websocket: isConnected ? "connected" : "disconnected",
    stats: {
      totalStakes: stakingStats.totalStakeTransactions,
      totalUnstakes: stakingStats.totalUnstakeTransactions,
      currentStaked: stakingStats.currentStakedCount,
      recentEventsCount: recentEvents.length,
      activeStakesCount: activeStakes.size
    },
    contract: CONTRACT_ADDRESS,
    lastUpdated: stakingStats.lastUpdated,
    timestamp: new Date().toISOString()
  });
});

// Get staking statistics
app.get(["/staking-stats", "/api/staking-stats"], (req, res) => {
  try {
    res.json({
      success: true,
      data: {
        totalStakeTransactions: stakingStats.totalStakeTransactions,
        totalUnstakeTransactions: stakingStats.totalUnstakeTransactions,
        currentStakedCount: stakingStats.currentStakedCount,
        lastUpdated: stakingStats.lastUpdated
      }
    });
  } catch (error) {
    console.error("❌ Error in staking-stats endpoint:", error);
    res.status(500).json({ 
      success: false,
      error: "Internal server error"
    });
  }
});

// Get recent events
app.get(["/recent-events", "/api/recent-events"], (req, res) => {
  try {
    res.json({
      success: true,
      data: {
        events: recentEvents,
        total: recentEvents.length,
        maxEvents: MAX_RECENT_EVENTS
      }
    });
  } catch (error) {
    console.error("❌ Error in recent-events endpoint:", error);
    res.status(500).json({ 
      success: false,
      error: "Internal server error"
    });
  }
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: "Endpoint not found",
    requestedUrl: req.url,
    method: req.method,
    availableEndpoints: [
      "GET /",
      "GET /health",
      "GET /api/health",
      "GET /staking-stats",
      "GET /api/staking-stats",
      "GET /recent-events",
      "GET /api/recent-events"
    ]
  });
});

// Global error handler
app.use((err, req, res, next) => {
  console.error('❌ Server Error:', err);
  res.status(500).json({
    success: false,
    error: 'Internal Server Error',
    message: process.env.NODE_ENV === 'production' ? 'An error occurred' : err.message,
    timestamp: new Date().toISOString()
  });
});

// ==================== SERVER STARTUP ====================

// Check if running under Passenger (cPanel) or standalone
const isPassenger = process.env.PASSENGER || process.env.NODE_ENV === 'production';

if (isPassenger) {
  // Running under Passenger (cPanel)
  console.log('\n🚀 ========== STARTING IN PRODUCTION MODE ==========');
  console.log('📍 Running under Passenger');
  console.log(`📍 Contract: ${CONTRACT_ADDRESS}`);
  console.log(`🌐 Base URL: https://rebelsnft.xyz/staking/api`);
  console.log('====================================================\n');
  
  initialize();
  export default app;
  
} else {
  // Local development mode
  const PORT = process.env.PORT || 3000;
  
  console.log('\n🚀 ========== STARTING IN DEVELOPMENT MODE ==========');
  console.log(`📍 Contract: ${CONTRACT_ADDRESS}`);
  console.log(`🌐 Local URL: http://localhost:${PORT}`);
  console.log('====================================================\n');
  
  const server = app.listen(PORT, () => {
    console.log(`✅ Server listening on port ${PORT}\n`);
    initialize();
  });
  
  // Graceful shutdown
  const shutdown = async (signal) => {
    console.log(`\n⚠️ ${signal} received, shutting down gracefully...`);
    
    if (reconnectTimeout) {
      clearTimeout(reconnectTimeout);
    }
    
    if (contract) {
      try {
        contract.removeAllListeners();
      } catch (e) {
        console.log("Cleanup warning:", e.message);
      }
    }
    
    if (provider) {
      try {
        await provider.destroy();
        console.log("✅ Provider destroyed");
      } catch (e) {
        console.log("Provider cleanup warning:", e.message);
      }
    }
    
    server.close(() => {
      console.log("✅ Server closed");
      process.exit(0);
    });
    
    // Force exit after 10 seconds
    setTimeout(() => {
      console.error("⚠️ Forced shutdown after timeout");
      process.exit(1);
    }, 10000);
  };
  
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}
