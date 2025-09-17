/**
 * Data Collection Service for Black Swan AI
 *
 * A high-frequency financial data collection service designed to monitor and collect
 * real-time market data for Black Swan.
 *
 * Author: Muhammad Bilal Motiwala
 * Project: Black Swan
 *
 * This service collects data from multiple financial APIs and stores it in Redis
 * for real-time access, with Firestore snapshots for backup + recovery.
 *
 * Data Sources:
 * - Cryptocurrencies (Bitcoin, Ethereum, Solana) via CoinGecko API
 * - S&P 500 via Alpha Vantage and Yahoo Finance APIs
 * - Fear & Greed Index via Alternative.me and VIX-derived calculations
 * - Currency exchange rates via Alpha Vantage and ExchangeRate-API
 * - Bull Market Peak Indicators via CoinGlass API
 * - Daily OHLCV data via CoinGlass API
 *
 * Collection Intervals:
 * - Cryptocurrencies: Every 60 seconds
 * - S&P 500: Every 60 seconds (24/7)
 * - Fear & Greed: Every 60 seconds
 * - Currency: Every 60 seconds
 * - Bull Market Peak: Every 15 minutes
 * - Daily OHLCV: Once daily at 02:10 UTC
 * - Snapshots: Every 30 minutes
 */

// Load environment variables from .env file
require("dotenv").config();

// Core dependencies for Express server and middleware
const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const compression = require("compression");

// Firebase Admin SDK for Firestore database operations
const admin = require("firebase-admin");

// HTTP client for API requests
const axios = require("axios");

// Redis client for high-performance caching
const Redis = require("ioredis");

// Cron job scheduler for periodic tasks
const cron = require("node-cron");

// Node.js built-in modules
const { EventEmitter } = require("events");
const crypto = require("crypto");

/**
 * Firebase Admin SDK Initialization
 *
 * Initializes Firebase Admin SDK using the service account key for Firestore access.
 * This allows the service to read/write data to Firestore for backup snapshots.
 * You will need to create a service account key and place it in the root directory of the project.
 */
const serviceAccount = require("./serviceAccountKey.json");
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

// Get Firestore database instance for backup operations
const db = admin.firestore();

/**
 * Redis Client Configuration
 *
 * Configures Redis client with production-ready settings for high-performance
 * data caching and real-time data storage. Includes connection retry logic,
 * timeout handling, and TLS support for secure connections.
 */
const redisClient = new Redis({
  // Connection details from environment variables
  host: process.env.REDIS_HOST_URL,
  port: process.env.REDIS_PORT,
  password: process.env.REDIS_PASSWORD,

  // TLS configuration for secure connections
  tls: {
    rejectUnauthorized: false, // Allow self-signed certificates in development
  },

  // Connection management
  enableReadyCheck: true,
  lazyConnect: true, // Connect only when first command is issued
  keepAlive: 30000, // Keep connection alive for 30 seconds

  // Retry strategy with exponential backoff
  retryStrategy: (times) => {
    const delay = Math.min(times * 50, 1000); // Max 1 second delay
    console.warn(`Redis connection attempt ${times}, retrying in ${delay}ms`);
    return delay;
  },

  // Error handling and reconnection
  reconnectOnError: (err) => {
    console.error("Redis connection error:", err);
    return true; // Always attempt to reconnect
  },

  // Timeout configurations
  maxRetriesPerRequest: 5,
  connectTimeout: 10000, // 10 seconds to establish connection
  commandTimeout: 10000, // 10 seconds for command execution
  maxLoadingTimeout: 5000, // 5 seconds for loading timeout

  // Performance optimizations
  enableAutoPipelining: true, // Automatically pipeline commands for better performance
  db: 0, // Use database 0 (default)
});

/**
 * Redis Connection Event Handlers
 *
 * Sets up event listeners for Redis connection status monitoring.
 * Provides real-time feedback on connection health and issues.
 */
redisClient.on("error", (err) => console.error("Redis Client Error:", err));
redisClient.on("connect", () => console.log("✅ Connected to Redis"));
redisClient.on("ready", () => console.log("✅ Redis client ready"));
redisClient.on("close", () => console.log("⚠️  Redis connection closed"));
redisClient.on("reconnecting", () => console.log("🔄 Redis reconnecting..."));

/**
 * External API Configuration
 *
 * Configuration object containing all external API endpoints, rate limits,
 * and timeout settings. Each API has specific rate limits that must be
 * respected to avoid being blocked or throttled.
 */
const APIS = {
  // CoinGecko API - Cryptocurrency price data
  COINGECKO: {
    BASE_URL: "https://api.coingecko.com/api/v3",
    RATE_LIMIT: 10, // 10 requests per minute for free tier
    TIMEOUT: 10000, // 10 second timeout
  },

  // Alpha Vantage API - Stock and currency data
  ALPHA_VANTAGE: {
    BASE_URL: "https://www.alphavantage.co/query",
    API_KEY: process.env.ALPHA_VANTAGE_API_KEY, // From environment variables
    RATE_LIMIT: 5, // 5 requests per minute for free tier
    TIMEOUT: 15000, // 15 second timeout (can be slow)
  },

  // Alternative.me API - Fear & Greed Index
  ALTERNATIVE_ME: {
    BASE_URL: "https://api.alternative.me",
    RATE_LIMIT: 60, // 60 requests per minute
    TIMEOUT: 10000, // 10 second timeout
  },

  // ExchangeRate-API - Currency exchange rates
  EXCHANGERATE_API: {
    BASE_URL: "https://api.exchangerate-api.com/v4",
    RATE_LIMIT: 1500, // 1500 requests per month (free tier)
    TIMEOUT: 10000, // 10 second timeout
  },

  // Yahoo Finance API - Stock market data (unofficial)
  YAHOO_FINANCE: {
    BASE_URL: "https://query1.finance.yahoo.com/v8/finance/chart",
    RATE_LIMIT: 2000, // Conservative limit for unofficial API
    TIMEOUT: 10000, // 10 second timeout
  },

  // CoinGlass API - Bull market indicators and OHLCV data
  COINGLASS: {
    BASE_URL: "https://open-api-v4.coinglass.com",
    API_KEY: process.env.COINGLASS_API_KEY, // From environment variables
    RATE_LIMIT: 60, // Conservative limit; we fetch every 15 minutes
    TIMEOUT: 15000, // 15 second timeout
  },
};

/**
 * Data Retention Configuration
 *
 * Defines how long data should be kept in Redis cache for each data type.
 * All values are in minutes to maintain consistency across different
 * collection intervals. Data older than these limits is automatically
 * removed to prevent memory bloat.
 */
const DATA_RETENTION = {
  BITCOIN: 7 * 24 * 60, // 7 days of minute data (10,080 minutes)
  ETHEREUM: 7 * 24 * 60, // 7 days of minute data (10,080 minutes)
  SOLANA: 7 * 24 * 60, // 7 days of minute data (10,080 minutes)
  SP500: 7 * 24 * 60, // 7 days of minute data (10,080 minutes)
  FEAR_GREED: 7 * 24 * 60, // 7 days of minute data (10,080 minutes)
  CURRENCY: 7 * 24 * 60, // 7 days of minute data (10,080 minutes)
  BULL_PEAK_INDICATOR: 7 * 24 * 4, // 7 days of 15-min data points (672 data points)
};

/**
 * Log Streamer Class
 *
 * A real-time log streaming service that captures all console output and streams
 * it to connected browser clients via Server-Sent Events (SSE). This allows
 * real-time monitoring of the data collection service from web interfaces.
 *
 * Features:
 * - Captures all console.log, console.error, console.warn, console.info, console.debug
 * - Maintains a rolling buffer of the last 1000 log entries
 * - Streams logs to multiple connected clients simultaneously
 * - Handles client disconnections gracefully
 * - Provides both real-time streaming and historical log access
 */
class LogStreamer extends EventEmitter {
  constructor() {
    super();

    // Client management
    this.clients = new Set(); // Set of connected SSE clients
    this.logBuffer = []; // Rolling buffer of log entries
    this.maxBufferSize = 1000; // Maximum number of log entries to keep in memory

    // Store original console methods for restoration
    this.originalConsole = {
      log: console.log,
      error: console.error,
      warn: console.warn,
      info: console.info,
      debug: console.debug,
    };

    // Initialize console capture
    this.setupConsoleCapture();
  }

  /**
   * Setup Console Capture
   *
   * Intercepts all console methods (log, error, warn, info, debug) and wraps them
   * to capture output for streaming to connected clients. The original console
   * methods are preserved and called first, then the output is captured and
   * processed for streaming.
   */
  setupConsoleCapture() {
    const self = this;

    /**
     * Helper function to create wrapped console method
     *
     * @param {string} methodName - Name of the console method (log, error, etc.)
     * @param {Function} originalMethod - Original console method to preserve
     * @returns {Function} Wrapped console method that captures and streams output
     */
    const wrapConsoleMethod = (methodName, originalMethod) => {
      return function (...args) {
        // Call original method first to maintain normal console output
        originalMethod.apply(console, args);

        // Create structured log entry for streaming
        const logEntry = {
          timestamp: new Date().toISOString(), // ISO timestamp for consistency
          level: methodName.toUpperCase(), // Log level (LOG, ERROR, WARN, etc.)
          message: args
            .map(
              (arg) =>
                typeof arg === "object"
                  ? JSON.stringify(arg, null, 2) // Pretty-print objects
                  : String(arg) // Convert other types to string
            )
            .join(" "), // Join multiple arguments with spaces
          raw_args: args, // Store original arguments for debugging
        };

        // Add to rolling buffer
        self.addToBuffer(logEntry);

        // Emit to connected clients via EventEmitter
        self.emit("log", logEntry);
      };
    };

    // Override all console methods with wrapped versions
    console.log = wrapConsoleMethod("log", this.originalConsole.log);
    console.error = wrapConsoleMethod("error", this.originalConsole.error);
    console.warn = wrapConsoleMethod("warn", this.originalConsole.warn);
    console.info = wrapConsoleMethod("info", this.originalConsole.info);
    console.debug = wrapConsoleMethod("debug", this.originalConsole.debug);
  }

  /**
   * Add Log Entry to Buffer
   *
   * Adds a new log entry to the rolling buffer and maintains the buffer size
   * by removing old entries when the maximum size is exceeded.
   *
   * @param {Object} logEntry - Log entry object with timestamp, level, message, etc.
   */
  addToBuffer(logEntry) {
    this.logBuffer.push(logEntry);

    // Trim buffer if it exceeds max size (keep only the most recent entries)
    if (this.logBuffer.length > this.maxBufferSize) {
      this.logBuffer = this.logBuffer.slice(-this.maxBufferSize);
    }
  }

  /**
   * Add Client to Log Stream
   *
   * Establishes a new Server-Sent Events (SSE) connection for a client to receive
   * real-time log streams. Sets up proper headers, sends a welcome message,
   * and streams the recent log buffer to the new client.
   *
   * @param {Object} response - Express response object for SSE connection
   * @returns {Object} Client object with connection details
   */
  addClient(response) {
    const clientId = Date.now() + Math.random();

    // Create client object to track connection
    const client = {
      id: clientId,
      response: response,
      connected: true,
    };

    this.clients.add(client);

    // Setup SSE headers with proper CORS for EventSource compatibility
    response.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers":
        "Accept, Cache-Control, Content-Type, X-Requested-With",
      "Access-Control-Expose-Headers": "Content-Type",
      "X-Accel-Buffering": "no", // Disable nginx buffering for real-time streaming
    });

    // Send welcome message to new client
    this.sendToClient(client, {
      timestamp: new Date().toISOString(),
      level: "INFO",
      message: "🚀 Connected to Data Collection Service log stream",
    });

    // Send recent log buffer to catch up new client
    this.logBuffer.forEach((logEntry) => {
      this.sendToClient(client, logEntry);
    });

    // Handle client disconnect cleanup
    response.on("close", () => {
      client.connected = false;
      this.clients.delete(client);
      console.log(
        `📱 [LOG_STREAM] Client ${clientId} disconnected (${this.clients.size} remaining)`
      );
    });

    console.log(
      `📱 [LOG_STREAM] Client ${clientId} connected (${this.clients.size} total)`
    );

    return client;
  }

  sendToClient(client, logEntry) {
    if (!client.connected) return;

    try {
      const data = JSON.stringify(logEntry);
      client.response.write(`data: ${data}\n\n`);
    } catch (error) {
      // Client probably disconnected
      client.connected = false;
      this.clients.delete(client);
    }
  }

  broadcastLog(logEntry) {
    // Remove disconnected clients
    const disconnectedClients = Array.from(this.clients).filter(
      (client) => !client.connected
    );
    disconnectedClients.forEach((client) => this.clients.delete(client));

    // Send to all connected clients
    this.clients.forEach((client) => {
      this.sendToClient(client, logEntry);
    });
  }

  getStats() {
    return {
      connectedClients: this.clients.size,
      bufferSize: this.logBuffer.length,
      maxBufferSize: this.maxBufferSize,
    };
  }

  restoreOriginalConsole() {
    console.log = this.originalConsole.log;
    console.error = this.originalConsole.error;
    console.warn = this.originalConsole.warn;
    console.info = this.originalConsole.info;
    console.debug = this.originalConsole.debug;
  }
}

/**
 * Public Log Streamer Class
 * Streams clean, sanitized information logs for public consumption
 */
class PublicLogStreamer extends EventEmitter {
  constructor() {
    super();
    this.clients = new Set();
    this.logBuffer = [];
    this.maxBufferSize = 1000; // Keep last 1000 log entries
  }

  emitPublicLog(message, type = "Info") {
    const logEntry = {
      timestamp: new Date().toISOString(),
      type: type,
      message: message,
    };

    // Add to buffer
    this.addToBuffer(logEntry);

    // Emit to connected clients
    this.emit("publicLog", logEntry);
    this.broadcastLog(logEntry);
  }

  addToBuffer(logEntry) {
    this.logBuffer.push(logEntry);

    // Trim buffer if it exceeds max size
    if (this.logBuffer.length > this.maxBufferSize) {
      this.logBuffer = this.logBuffer.slice(-this.maxBufferSize);
    }
  }

  addClient(response) {
    const clientId = Date.now() + Math.random();

    const client = {
      id: clientId,
      response: response,
      connected: true,
    };

    this.clients.add(client);

    // Setup SSE headers
    response.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Cache-Control",
    });

    // Send welcome message
    this.sendToClient(client, {
      timestamp: new Date().toISOString(),
      type: "Info",
      message: "Connected to Data Collection Public Stream",
    });

    // Send recent log buffer
    this.logBuffer.forEach((logEntry) => {
      this.sendToClient(client, logEntry);
    });

    // Handle client disconnect
    response.on("close", () => {
      client.connected = false;
      this.clients.delete(client);
    });

    return client;
  }

  sendToClient(client, logEntry) {
    if (!client.connected) return;

    try {
      const data = JSON.stringify(logEntry);
      client.response.write(`data: ${data}\n\n`);
    } catch (error) {
      // Client probably disconnected
      client.connected = false;
      this.clients.delete(client);
    }
  }

  broadcastLog(logEntry) {
    // Remove disconnected clients
    const disconnectedClients = Array.from(this.clients).filter(
      (client) => !client.connected
    );
    disconnectedClients.forEach((client) => this.clients.delete(client));

    // Send to all connected clients
    this.clients.forEach((client) => {
      this.sendToClient(client, logEntry);
    });
  }

  getStats() {
    return {
      connectedClients: this.clients.size,
      bufferSize: this.logBuffer.length,
      maxBufferSize: this.maxBufferSize,
    };
  }
}

// Rate limiting classes for different APIs
class APIRateLimiter {
  constructor(apiName, requestsPerMinute) {
    this.apiName = apiName;
    this.requests = [];
    this.maxRequests = requestsPerMinute;
    this.windowMs = 60000; // 1 minute
  }

  async waitForRateLimit() {
    const now = Date.now();
    this.requests = this.requests.filter((time) => now - time < this.windowMs);

    if (this.requests.length >= this.maxRequests) {
      const oldestRequest = Math.min(...this.requests);
      const waitTime = this.windowMs - (now - oldestRequest) + 100;
      console.log(
        `${this.apiName} rate limit reached. Waiting ${waitTime}ms...`
      );
      await new Promise((resolve) => setTimeout(resolve, waitTime));
      return this.waitForRateLimit();
    }

    this.requests.push(now);
    return true;
  }
}

// Initialize rate limiters
const rateLimiters = {
  coingecko: new APIRateLimiter("CoinGecko", APIS.COINGECKO.RATE_LIMIT),
  alphaVantage: new APIRateLimiter(
    "Alpha Vantage",
    APIS.ALPHA_VANTAGE.RATE_LIMIT
  ),
  alternativeMe: new APIRateLimiter(
    "Alternative.me",
    APIS.ALTERNATIVE_ME.RATE_LIMIT
  ),
  exchangeRateApi: new APIRateLimiter(
    "ExchangeRate-API",
    APIS.EXCHANGERATE_API.RATE_LIMIT
  ),
  yahooFinance: new APIRateLimiter(
    "Yahoo Finance",
    APIS.YAHOO_FINANCE.RATE_LIMIT
  ),
  coinglass: new APIRateLimiter("CoinGlass", APIS.COINGLASS.RATE_LIMIT),
};

// Data collection classes
class CryptocurrencyDataCollector {
  constructor(publicLogStreamer = null) {
    this.isRunning = false;
    this.collectionInterval = 60000; // 1 minute
    this.lastProcessedTimestamp = null;
    this.publicLogStreamer = publicLogStreamer;
    this.cryptocurrencies = {
      bitcoin: { symbol: "BTC", displayName: "Bitcoin" },
      ethereum: { symbol: "ETH", displayName: "Ethereum" },
      solana: { symbol: "SOL", displayName: "Solana" },
    };
  }

  async start() {
    if (this.isRunning) return;
    this.isRunning = true;

    console.log(
      "🚀 Starting cryptocurrency data collection (Bitcoin, Ethereum, Solana - minute-by-minute intervals)..."
    );

    while (this.isRunning) {
      try {
        await this.collectCurrentCryptocurrencyData();
        await new Promise((resolve) =>
          setTimeout(resolve, this.collectionInterval)
        );
      } catch (error) {
        console.error(
          "❌ [CRYPTO] Error in cryptocurrency data collection:",
          error
        );
        await new Promise((resolve) =>
          setTimeout(resolve, this.collectionInterval)
        );
      }
    }
  }

  async collectCurrentCryptocurrencyData() {
    try {
      await rateLimiters.coingecko.waitForRateLimit();

      // Get current prices for all cryptocurrencies in a single request
      const currentPriceResponse = await axios.get(
        `${APIS.COINGECKO.BASE_URL}/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd`,
        { timeout: APIS.COINGECKO.TIMEOUT }
      );

      const cryptoData = currentPriceResponse.data;

      if (!cryptoData || Object.keys(cryptoData).length === 0) {
        console.warn(
          "⚠️ [CRYPTO] No cryptocurrency price data received from CoinGecko"
        );
        return;
      }

      // Create current minute data point
      const currentTimestamp = Date.now();
      const currentMinute = Math.floor(currentTimestamp / 60000) * 60000; // Round down to minute boundary

      // Only store if this is a new minute we haven't processed
      if (
        !this.lastProcessedTimestamp ||
        currentMinute > this.lastProcessedTimestamp
      ) {
        // Process each cryptocurrency
        const processedCryptos = [];

        for (const [cryptoId, cryptoInfo] of Object.entries(
          this.cryptocurrencies
        )) {
          if (cryptoData[cryptoId] && cryptoData[cryptoId].usd) {
            const currentData = {
              timestamp: currentMinute,
              price: cryptoData[cryptoId].usd,
              collected_at: new Date().toISOString(),
              source: "real_time",
            };

            await this.storeCryptocurrencyData(cryptoId, currentData);
            processedCryptos.push({
              name: cryptoInfo.displayName,
              symbol: cryptoInfo.symbol,
              price: currentData.price,
            });
          }
        }

        this.lastProcessedTimestamp = currentMinute;

        if (processedCryptos.length > 0) {
          const pricesSummary = processedCryptos
            .map((crypto) => `${crypto.name}: $${crypto.price.toFixed(2)}`)
            .join(", ");

          console.log(
            `✅ [CRYPTO] New minute data collected: ${pricesSummary} at ${new Date(
              currentMinute
            ).toISOString()}`
          );

          // Emit public log
          if (this.publicLogStreamer) {
            this.publicLogStreamer.emitPublicLog(
              "Latest Cryptocurrency Prices Synced"
            );
          }
        }
      } else {
        console.log(
          `⏭️ [CRYPTO] Minute ${new Date(
            currentMinute
          ).toISOString()} already processed, waiting...`
        );
      }
    } catch (error) {
      console.error(
        "❌ [CRYPTO] Failed to collect current cryptocurrency data:",
        error.message
      );
    }
  }

  async storeCryptocurrencyData(cryptoId, newDataPoint) {
    const key = `blackswan:${cryptoId}:minute_data`;

    try {
      // Get existing data
      const existingDataString = await redisClient.get(key);
      let dataArray = existingDataString ? JSON.parse(existingDataString) : [];

      // Check if this timestamp already exists
      const existingIndex = dataArray.findIndex(
        (item) => item.timestamp === newDataPoint.timestamp
      );

      if (existingIndex >= 0) {
        // Update existing data point (real-time data overwrites existing)
        dataArray[existingIndex] = newDataPoint;
        console.log(
          `🔄 [${cryptoId.toUpperCase()}] Updated existing minute data for ${new Date(
            newDataPoint.timestamp
          ).toISOString()}`
        );
      } else {
        // Add new data point
        dataArray.push(newDataPoint);

        // Sort by timestamp to maintain order
        dataArray.sort((a, b) => a.timestamp - b.timestamp);

        // Keep only last 7 days (7 * 24 * 60 = 10080 minutes)
        const retentionKey = cryptoId.toUpperCase();
        if (dataArray.length > DATA_RETENTION[retentionKey]) {
          dataArray = dataArray.slice(-DATA_RETENTION[retentionKey]);
        }
      }

      // Store back to Redis
      await redisClient.set(key, JSON.stringify(dataArray));
      await redisClient.set(
        `blackswan:${cryptoId}:last_update`,
        new Date().toISOString()
      );
      await redisClient.set(
        `blackswan:${cryptoId}:data_count`,
        dataArray.length.toString()
      );
    } catch (error) {
      console.error(
        `❌ [${cryptoId.toUpperCase()}] Error storing data:`,
        error
      );
    }
  }

  stop() {
    this.isRunning = false;
    console.log("🛑 Stopping cryptocurrency data collection");
  }
}

class SP500DataCollector {
  constructor(publicLogStreamer = null) {
    this.isRunning = false;
    this.collectionInterval = 60000; // 1 minute
    this.lastProcessedTimestamp = null;
    this.publicLogStreamer = publicLogStreamer;
  }

  async start() {
    if (this.isRunning) return;
    this.isRunning = true;

    console.log(
      "🚀 Starting S&P 500 data collection (1-minute intervals, 24/7)..."
    );

    while (this.isRunning) {
      try {
        await this.collectCurrentSP500Data();
        await new Promise((resolve) =>
          setTimeout(resolve, this.collectionInterval)
        );
      } catch (error) {
        console.error("❌ [SP500] Error in S&P 500 data collection:", error);
        await new Promise((resolve) =>
          setTimeout(resolve, this.collectionInterval)
        );
      }
    }
  }

  isMarketHours() {
    try {
      // Get current time in Eastern Time (UTC-5 or UTC-4 depending on DST)
      const now = new Date();

      // Convert to Eastern Time using toLocaleString
      const etDate = new Date(
        now.toLocaleString("en-US", { timeZone: "America/New_York" })
      );

      const day = etDate.getDay(); // 0 = Sunday, 6 = Saturday
      const hour = etDate.getHours();
      const minute = etDate.getMinutes();

      // Check if it's a weekday (Monday = 1, Friday = 5)
      if (day < 1 || day > 5) {
        return false;
      }

      // Check if it's during market hours (9:30 AM - 4:00 PM ET)
      const marketStart = 9 * 60 + 30; // 9:30 AM in minutes
      const marketEnd = 16 * 60; // 4:00 PM in minutes
      const currentTime = hour * 60 + minute;

      return currentTime >= marketStart && currentTime < marketEnd;
    } catch (error) {
      console.error("❌ [SP500] Error checking market hours:", error);
      // Default to market closed if there's an error
      return false;
    }
  }

  async collectCurrentSP500Data() {
    try {
      const currentTimestamp = Date.now();
      const currentMinute = Math.floor(currentTimestamp / 60000) * 60000; // Round down to minute boundary

      // Only collect if this is a new minute we haven't processed
      if (
        this.lastProcessedTimestamp &&
        currentMinute <= this.lastProcessedTimestamp
      ) {
        return; // Skip if we already processed this minute
      }

      // Try Alpha Vantage first (if API key available)
      let sp500Data = null;
      if (APIS.ALPHA_VANTAGE.API_KEY) {
        sp500Data = await this.fetchCurrentFromAlphaVantage();
      }

      // Fallback to Yahoo Finance
      if (!sp500Data) {
        sp500Data = await this.fetchCurrentFromYahooFinance();
      }

      if (sp500Data) {
        // Align to minute boundary
        sp500Data.timestamp = currentMinute;
        sp500Data.collected_at = new Date().toISOString();
        sp500Data.market_hours = this.isMarketHours(); // Add market hours info for context

        await this.storeSP500Data(sp500Data);
        this.lastProcessedTimestamp = currentMinute;

        const marketStatus = sp500Data.market_hours ? "OPEN" : "CLOSED";
        console.log(
          `✅ [SP500] New minute data collected: ${sp500Data.price.toFixed(
            2
          )} from ${sp500Data.source} at ${new Date(
            currentMinute
          ).toISOString()} (Market: ${marketStatus})`
        );

        // Emit public log
        if (this.publicLogStreamer) {
          this.publicLogStreamer.emitPublicLog("Latest S&P 500 Price Synced");
        }
      }
    } catch (error) {
      console.error(
        "❌ [SP500] Failed to collect current S&P 500 data:",
        error.message
      );
    }
  }

  async fetchCurrentFromAlphaVantage() {
    try {
      await rateLimiters.alphaVantage.waitForRateLimit();

      // Get current quote data
      const response = await axios.get(APIS.ALPHA_VANTAGE.BASE_URL, {
        params: {
          function: "GLOBAL_QUOTE",
          symbol: "SPY", // S&P 500 ETF as proxy
          apikey: APIS.ALPHA_VANTAGE.API_KEY,
        },
        timeout: APIS.ALPHA_VANTAGE.TIMEOUT,
      });

      const quote = response.data["Global Quote"];
      if (!quote) return null;

      return {
        price: parseFloat(quote["05. price"]),
        open: parseFloat(quote["02. open"]),
        high: parseFloat(quote["03. high"]),
        low: parseFloat(quote["04. low"]),
        volume: parseInt(quote["06. volume"]),
        change: parseFloat(quote["09. change"]),
        change_percent: quote["10. change percent"],
        source: "alpha_vantage",
      };
    } catch (error) {
      console.error("❌ [SP500] Alpha Vantage error:", error.message);
      return null;
    }
  }

  async fetchCurrentFromYahooFinance() {
    try {
      await rateLimiters.yahooFinance.waitForRateLimit();

      const response = await axios.get(`${APIS.YAHOO_FINANCE.BASE_URL}/^GSPC`, {
        params: {
          interval: "1m",
          range: "1d",
        },
        timeout: APIS.YAHOO_FINANCE.TIMEOUT,
      });

      const result = response.data.chart?.result?.[0];
      if (!result) return null;

      const quote = result.indicators?.quote?.[0];
      const timestamps = result.timestamp;
      const meta = result.meta;

      if (!quote || !timestamps || timestamps.length === 0) return null;

      const latestIndex = timestamps.length - 1;

      return {
        price: quote.close[latestIndex] || meta.regularMarketPrice,
        open: quote.open[latestIndex] || meta.regularMarketOpen,
        high: quote.high[latestIndex] || meta.regularMarketDayHigh,
        low: quote.low[latestIndex] || meta.regularMarketDayLow,
        volume: quote.volume[latestIndex] || meta.regularMarketVolume,
        change: meta.regularMarketPrice - meta.previousClose,
        change_percent:
          (
            ((meta.regularMarketPrice - meta.previousClose) /
              meta.previousClose) *
            100
          ).toFixed(2) + "%",
        source: "yahoo_finance",
      };
    } catch (error) {
      console.error("❌ [SP500] Yahoo Finance error:", error.message);
      return null;
    }
  }

  async storeSP500Data(newDataPoint) {
    const key = "blackswan:sp500:minute_data";

    try {
      const existingDataString = await redisClient.get(key);
      let dataArray = existingDataString ? JSON.parse(existingDataString) : [];

      // Check if this timestamp already exists
      const existingIndex = dataArray.findIndex(
        (item) => item.timestamp === newDataPoint.timestamp
      );

      if (existingIndex >= 0) {
        // Update existing data point
        dataArray[existingIndex] = newDataPoint;
        console.log(
          `🔄 [SP500] Updated existing minute data for ${new Date(
            newDataPoint.timestamp
          ).toISOString()}`
        );
      } else {
        // Add new data point
        dataArray.push(newDataPoint);

        // Sort by timestamp to maintain order
        dataArray.sort((a, b) => a.timestamp - b.timestamp);

        // Keep only last 7 days
        if (dataArray.length > DATA_RETENTION.SP500) {
          dataArray = dataArray.slice(-DATA_RETENTION.SP500);
        }
      }

      await redisClient.set(key, JSON.stringify(dataArray));
      await redisClient.set(
        "blackswan:sp500:last_update",
        new Date().toISOString()
      );
      await redisClient.set(
        "blackswan:sp500:data_count",
        dataArray.length.toString()
      );
    } catch (error) {
      console.error("❌ [SP500] Error storing data:", error);
    }
  }

  stop() {
    this.isRunning = false;
    console.log("🛑 Stopping S&P 500 data collection");
  }
}

class FearGreedDataCollector {
  constructor(publicLogStreamer = null) {
    this.isRunning = false;
    this.collectionInterval = 60000; // 1 minute (was 1 hour)
    this.lastProcessedTimestamp = null;
    this.publicLogStreamer = publicLogStreamer;
  }

  async start() {
    if (this.isRunning) return;
    this.isRunning = true;

    console.log(
      "🚀 Starting Fear & Greed Index data collection (60-second intervals)..."
    );

    // Collect immediately then start interval
    await this.collectCurrentFearGreedData();

    while (this.isRunning) {
      try {
        await new Promise((resolve) =>
          setTimeout(resolve, this.collectionInterval)
        );
        await this.collectCurrentFearGreedData();
      } catch (error) {
        console.error(
          "❌ [FEAR_GREED] Error in Fear & Greed data collection:",
          error
        );
        await new Promise((resolve) =>
          setTimeout(resolve, this.collectionInterval)
        );
      }
    }
  }

  async collectCurrentFearGreedData() {
    try {
      const currentTimestamp = Date.now();
      const currentMinute = Math.floor(currentTimestamp / 60000) * 60000; // Round down to minute boundary

      // Only collect if this is a new minute we haven't processed
      if (
        this.lastProcessedTimestamp &&
        currentMinute <= this.lastProcessedTimestamp
      ) {
        console.log(
          `⏭️ [FEAR_GREED] Minute ${new Date(
            currentMinute
          ).toISOString()} already processed, waiting...`
        );
        return;
      }

      // Collect both crypto and traditional market fear & greed
      const [cryptoFG, traditionalFG] = await Promise.all([
        this.fetchCurrentCryptoFearGreed(),
        this.fetchCurrentTraditionalFearGreed(),
      ]);

      const data = {
        timestamp: currentMinute,
        crypto_fear_greed: cryptoFG,
        traditional_fear_greed: traditionalFG,
        collected_at: new Date().toISOString(),
      };

      await this.storeFearGreedData(data);
      this.lastProcessedTimestamp = currentMinute;

      console.log(
        `✅ [FEAR_GREED] New minute data collected - Crypto: ${
          cryptoFG?.value || "N/A"
        }, Traditional: ${traditionalFG?.value || "N/A"} at ${new Date(
          currentMinute
        ).toISOString()}`
      );

      // Emit public log
      if (this.publicLogStreamer) {
        this.publicLogStreamer.emitPublicLog(
          "Latest Crypto Market Fear & Greed Index Synced"
        );
      }
    } catch (error) {
      console.error(
        "❌ [FEAR_GREED] Failed to collect current Fear & Greed data:",
        error.message
      );
    }
  }

  async fetchCurrentCryptoFearGreed() {
    try {
      await rateLimiters.alternativeMe.waitForRateLimit();

      const response = await axios.get(`${APIS.ALTERNATIVE_ME.BASE_URL}/fng`, {
        timeout: APIS.ALTERNATIVE_ME.TIMEOUT,
      });

      const data = response.data?.data?.[0];
      if (!data) return null;

      return {
        value: parseInt(data.value),
        classification: data.value_classification,
        source: "alternative_me",
      };
    } catch (error) {
      console.error(
        "❌ [FEAR_GREED] Crypto Fear & Greed error:",
        error.message
      );
      return null;
    }
  }

  async fetchCurrentTraditionalFearGreed() {
    try {
      if (!APIS.ALPHA_VANTAGE.API_KEY) return null;

      await rateLimiters.alphaVantage.waitForRateLimit();

      // Get current VIX value
      const response = await axios.get(APIS.ALPHA_VANTAGE.BASE_URL, {
        params: {
          function: "GLOBAL_QUOTE",
          symbol: "VIX",
          apikey: APIS.ALPHA_VANTAGE.API_KEY,
        },
        timeout: APIS.ALPHA_VANTAGE.TIMEOUT,
      });

      const quote = response.data["Global Quote"];
      if (!quote) return null;

      const vixValue = parseFloat(quote["05. price"]);

      // Convert VIX to 0-100 fear/greed scale (inverted: high VIX = high fear = low value)
      // VIX typically ranges from 10-80, with 20+ being fear territory
      let fearGreedValue;
      if (vixValue <= 12) fearGreedValue = 90; // Extreme greed
      else if (vixValue <= 17) fearGreedValue = 75; // Greed
      else if (vixValue <= 25) fearGreedValue = 50; // Neutral
      else if (vixValue <= 35) fearGreedValue = 25; // Fear
      else fearGreedValue = 10; // Extreme fear

      return {
        value: fearGreedValue,
        vix_value: vixValue,
        classification: this.getClassification(fearGreedValue),
        source: "vix_derived",
      };
    } catch (error) {
      console.error(
        "❌ [FEAR_GREED] Traditional Fear & Greed error:",
        error.message
      );
      return null;
    }
  }

  getClassification(value) {
    if (value <= 20) return "Extreme Fear";
    if (value <= 40) return "Fear";
    if (value <= 60) return "Neutral";
    if (value <= 80) return "Greed";
    return "Extreme Greed";
  }

  async storeFearGreedData(newDataPoint) {
    const key = "blackswan:fear_greed:minute_data";

    try {
      const existingDataString = await redisClient.get(key);
      let dataArray = existingDataString ? JSON.parse(existingDataString) : [];

      // Check if this timestamp already exists
      const existingIndex = dataArray.findIndex(
        (item) => item.timestamp === newDataPoint.timestamp
      );

      if (existingIndex >= 0) {
        // Update existing data point
        dataArray[existingIndex] = newDataPoint;
        console.log(
          `🔄 [FEAR_GREED] Updated existing minute data for ${new Date(
            newDataPoint.timestamp
          ).toISOString()}`
        );
      } else {
        // Add new data point
        dataArray.push(newDataPoint);

        // Sort by timestamp to maintain order
        dataArray.sort((a, b) => a.timestamp - b.timestamp);

        // Keep only last 7 days (7 * 24 * 60 = 10080 minutes)
        if (dataArray.length > DATA_RETENTION.FEAR_GREED) {
          dataArray = dataArray.slice(-DATA_RETENTION.FEAR_GREED);
        }
      }

      await redisClient.set(key, JSON.stringify(dataArray));
      await redisClient.set(
        "blackswan:fear_greed:last_update",
        new Date().toISOString()
      );
      await redisClient.set(
        "blackswan:fear_greed:data_count",
        dataArray.length.toString()
      );
    } catch (error) {
      console.error("❌ [FEAR_GREED] Error storing data:", error);
    }
  }

  stop() {
    this.isRunning = false;
    console.log("🛑 Stopping Fear & Greed data collection");
  }
}

class CurrencyDataCollector {
  constructor(publicLogStreamer = null) {
    this.isRunning = false;
    this.collectionInterval = 60000; // 1 minute (was 15 minutes)
    this.currencyPairs = ["GBP", "EUR", "JPY", "CHF"];
    this.lastProcessedTimestamp = null;
    this.publicLogStreamer = publicLogStreamer;
  }

  async start() {
    if (this.isRunning) return;
    this.isRunning = true;

    console.log(
      "🚀 Starting USD currency data collection (60-second intervals)..."
    );

    while (this.isRunning) {
      try {
        await this.collectCurrentCurrencyData();
        await new Promise((resolve) =>
          setTimeout(resolve, this.collectionInterval)
        );
      } catch (error) {
        console.error(
          "❌ [CURRENCY] Error in currency data collection:",
          error
        );
        await new Promise((resolve) =>
          setTimeout(resolve, this.collectionInterval)
        );
      }
    }
  }

  async collectCurrentCurrencyData() {
    try {
      const currentTimestamp = Date.now();
      const currentMinute = Math.floor(currentTimestamp / 60000) * 60000; // Round down to minute boundary

      // Only collect if this is a new minute we haven't processed
      if (
        this.lastProcessedTimestamp &&
        currentMinute <= this.lastProcessedTimestamp
      ) {
        console.log(
          `⏭️ [CURRENCY] Minute ${new Date(
            currentMinute
          ).toISOString()} already processed, waiting...`
        );
        return;
      }

      const currencyData = {};

      // Try to get data for each currency pair
      for (const currency of this.currencyPairs) {
        try {
          let rate = null;

          // Try Alpha Vantage first (if API key available)
          if (APIS.ALPHA_VANTAGE.API_KEY) {
            rate = await this.fetchCurrentFromAlphaVantage(currency);
          }

          // Fallback to ExchangeRate-API
          if (!rate) {
            rate = await this.fetchCurrentFromExchangeRateAPI(currency);
          }

          if (rate) {
            currencyData[`USD${currency}`] = rate;
          }
        } catch (error) {
          console.error(
            `❌ [CURRENCY] Error fetching USD${currency}:`,
            error.message
          );
        }
      }

      if (Object.keys(currencyData).length > 0) {
        const data = {
          timestamp: currentMinute,
          rates: currencyData,
          collected_at: new Date().toISOString(),
        };

        await this.storeCurrencyData(data);
        this.lastProcessedTimestamp = currentMinute;

        const ratesSummary = Object.entries(currencyData)
          .map(([pair, data]) => `${pair}: ${data.rate.toFixed(4)}`)
          .join(", ");
        console.log(
          `✅ [CURRENCY] New minute data collected - ${ratesSummary} at ${new Date(
            currentMinute
          ).toISOString()}`
        );

        // Emit public log
        if (this.publicLogStreamer) {
          this.publicLogStreamer.emitPublicLog(
            "Latest Currency Exchange Rates Synced"
          );
        }
      }
    } catch (error) {
      console.error(
        "❌ [CURRENCY] Failed to collect current currency data:",
        error.message
      );
    }
  }

  async fetchCurrentFromAlphaVantage(currency) {
    try {
      await rateLimiters.alphaVantage.waitForRateLimit();

      // Get current exchange rate
      const response = await axios.get(APIS.ALPHA_VANTAGE.BASE_URL, {
        params: {
          function: "CURRENCY_EXCHANGE_RATE",
          from_currency: "USD",
          to_currency: currency,
          apikey: APIS.ALPHA_VANTAGE.API_KEY,
        },
        timeout: APIS.ALPHA_VANTAGE.TIMEOUT,
      });

      const exchangeRate = response.data["Realtime Currency Exchange Rate"];
      if (!exchangeRate) return null;

      return {
        rate: parseFloat(exchangeRate["5. Exchange Rate"]),
        source: "alpha_vantage",
      };
    } catch (error) {
      console.error(
        `❌ [CURRENCY] Alpha Vantage USD${currency} error:`,
        error.message
      );
      return null;
    }
  }

  async fetchCurrentFromExchangeRateAPI(currency) {
    try {
      await rateLimiters.exchangeRateApi.waitForRateLimit();

      const response = await axios.get(
        `${APIS.EXCHANGERATE_API.BASE_URL}/latest/USD`,
        {
          timeout: APIS.EXCHANGERATE_API.TIMEOUT,
        }
      );

      const rate = response.data?.rates?.[currency];
      if (!rate) return null;

      return {
        rate: parseFloat(rate),
        source: "exchangerate_api",
      };
    } catch (error) {
      console.error(
        `❌ [CURRENCY] ExchangeRate-API USD${currency} error:`,
        error.message
      );
      return null;
    }
  }

  async storeCurrencyData(newDataPoint) {
    const key = "blackswan:currency:minute_data";

    try {
      const existingDataString = await redisClient.get(key);
      let dataArray = existingDataString ? JSON.parse(existingDataString) : [];

      // Check if this timestamp already exists
      const existingIndex = dataArray.findIndex(
        (item) => item.timestamp === newDataPoint.timestamp
      );

      if (existingIndex >= 0) {
        // Update existing data point
        dataArray[existingIndex] = newDataPoint;
        console.log(
          `🔄 [CURRENCY] Updated existing minute data for ${new Date(
            newDataPoint.timestamp
          ).toISOString()}`
        );
      } else {
        // Add new data point
        dataArray.push(newDataPoint);

        // Sort by timestamp to maintain order
        dataArray.sort((a, b) => a.timestamp - b.timestamp);

        // Keep only last 7 days (7 * 24 * 60 = 10080 minutes)
        if (dataArray.length > DATA_RETENTION.CURRENCY) {
          dataArray = dataArray.slice(-DATA_RETENTION.CURRENCY);
        }
      }

      await redisClient.set(key, JSON.stringify(dataArray));
      await redisClient.set(
        "blackswan:currency:last_update",
        new Date().toISOString()
      );
      await redisClient.set(
        "blackswan:currency:data_count",
        dataArray.length.toString()
      );
    } catch (error) {
      console.error("❌ [CURRENCY] Error storing data:", error);
    }
  }

  stop() {
    this.isRunning = false;
    console.log("🛑 Stopping currency data collection");
  }
}

// Bull Market Peak Indicator Collector (CoinGlass)
class BullMarketPeakIndicatorCollector {
  constructor(publicLogStreamer = null) {
    this.isRunning = false;
    this.collectionIntervalMs = 15 * 60 * 1000; // 15 minutes
    this.publicLogStreamer = publicLogStreamer;
    this.lastRunAligned = null;
    this.unsubscribe = null; // Firestore listener unsubscribe
    this.latestSnapshotCache = null; // in-memory latest for quick compare
  }

  async start() {
    if (this.isRunning) return;
    this.isRunning = true;

    console.log(
      "🚀 Starting Bull Market Peak Indicator collection (15-minute intervals)..."
    );

    // Setup Firestore real-time listener to keep latest in memory
    this.setupLatestSnapshotListener();

    // Run immediately on start
    await this.collectOnce();

    // Align to 15-min boundaries (00, 15, 30, 45)
    const scheduleNext = () => {
      if (!this.isRunning) return;
      const now = Date.now();
      const minutes = new Date(now).getMinutes();
      const minsToNext = (15 - (minutes % 15)) % 15;
      const msToNext =
        minsToNext === 0
          ? 15 * 60 * 1000 - (now % (15 * 60 * 1000))
          : minsToNext * 60 * 1000 - (now % (60 * 1000));

      setTimeout(async () => {
        try {
          await this.collectOnce();
        } catch (e) {
          console.error("❌ [BULL_PEAK] Collection error:", e.message || e);
        } finally {
          scheduleNext();
        }
      }, Math.max(msToNext, 1000));
    };

    scheduleNext();
  }

  stop() {
    this.isRunning = false;
    if (typeof this.unsubscribe === "function") {
      try {
        this.unsubscribe();
      } catch (_) {}
      this.unsubscribe = null;
    }
    console.log("🛑 Stopping Bull Market Peak Indicator collection");
  }

  setupLatestSnapshotListener() {
    try {
      const docRef = db.collection("bull-market-peak-indicators").doc("latest");

      this.unsubscribe = docRef.onSnapshot(
        (doc) => {
          if (doc.exists) {
            const data = doc.data();
            this.latestSnapshotCache = data;
          }
        },
        (error) => {
          console.error("❌ [BULL_PEAK] Firestore listener error:", error);
        }
      );

      console.log(
        "📡 [BULL_PEAK] Firestore real-time listener attached (latest)"
      );
    } catch (error) {
      console.error(
        "❌ [BULL_PEAK] Failed to attach Firestore listener:",
        error
      );
    }
  }

  // Fetch from CoinGlass API
  async fetchIndicators() {
    try {
      await rateLimiters.coinglass.waitForRateLimit();
      const url = `${APIS.COINGLASS.BASE_URL}/api/bull-market-peak-indicator`;
      const response = await axios.get(url, {
        headers: { "CG-API-KEY": APIS.COINGLASS.API_KEY },
        timeout: APIS.COINGLASS.TIMEOUT,
      });

      const payload = response.data;
      if (!payload || !payload.success || !Array.isArray(payload.data)) {
        throw new Error("Unexpected CoinGlass response shape");
      }

      // Normalize numbers and booleans
      const normalized = payload.data.map((item) => ({
        indicator_name: this.toStringSafe(item.indicator_name),
        current_value: this.toNumberSafe(item.current_value),
        target_value: this.toNumberSafe(item.target_value),
        previous_value: this.toNumberSafe(item.previous_value),
        change_value: this.toNumberSafe(item.change_value),
        comparison_type: this.toStringSafe(item.comparison_type),
        hit_status: Boolean(item.hit_status),
      }));

      return normalized;
    } catch (error) {
      console.error("❌ [BULL_PEAK] Fetch error:", error.message || error);
      return null;
    }
  }

  toNumberSafe(val) {
    const n = parseFloat(val);
    if (Number.isNaN(n) || !Number.isFinite(n)) return null;
    return n;
  }

  toStringSafe(val) {
    if (val === undefined || val === null) return null;
    const s = String(val);
    return s.length ? s : null;
  }

  // Create a deterministic hash for change detection
  computeHash(obj) {
    try {
      // If array of indicators, sort deterministically and stringify with sorted keys
      if (Array.isArray(obj)) {
        const sorted = [...obj]
          .map((o) => ({ ...o }))
          .sort((a, b) => {
            const an = (a.indicator_name || "").toString();
            const bn = (b.indicator_name || "").toString();
            return an.localeCompare(bn);
          })
          .map((o) => {
            const keys = Object.keys(o).sort();
            const ordered = {};
            keys.forEach((k) => (ordered[k] = o[k]));
            return ordered;
          });
        return crypto
          .createHash("sha256")
          .update(JSON.stringify(sorted))
          .digest("hex");
      }
      const json = JSON.stringify(obj, Object.keys(obj).sort());
      return crypto.createHash("sha256").update(json).digest("hex");
    } catch (_) {
      return null;
    }
  }

  async collectOnce() {
    const indicators = await this.fetchIndicators();
    if (!indicators) return;

    const docPayload = {
      timestamp: Date.now(),
      collected_at: new Date().toISOString(),
      source: "coinglass",
      indicators,
    };

    // Compare with latest snapshot cache if available
    let shouldWrite = true;
    try {
      if (this.latestSnapshotCache && this.latestSnapshotCache.indicators) {
        const prevHash = this.computeHash(this.latestSnapshotCache.indicators);
        const newHash = this.computeHash(indicators);
        shouldWrite = prevHash !== newHash;
      } else {
        // No cache yet, fetch current latest once for robust compare
        const latestDoc = await db
          .collection("bull-market-peak-indicators")
          .doc("latest")
          .get();
        if (latestDoc.exists) {
          const latestData = latestDoc.data();
          const prevHash = this.computeHash(latestData?.indicators || []);
          const newHash = this.computeHash(indicators);
          shouldWrite = prevHash !== newHash;
        }
      }
    } catch (e) {
      console.error("❌ [BULL_PEAK] Compare error:", e.message || e);
      shouldWrite = true; // fallback to writing to avoid missing updates
    }

    if (!shouldWrite) {
      console.log("⏭️ [BULL_PEAK] No change detected. Skipping new snapshot.");
      return;
    }

    await this.persistSnapshot(docPayload);
  }

  async persistSnapshot(docPayload) {
    // Write a new document into a time-series collection and update 'latest'
    const collectionRef = db.collection("bull-market-peak-indicators");
    const batch = db.batch();

    const latestDocRef = collectionRef.doc("latest");
    batch.set(latestDocRef, docPayload);

    try {
      await batch.commit();
      this.latestSnapshotCache = docPayload; // refresh cache
      await redisClient.set(
        "blackswan:bull_peak:last_update",
        docPayload.collected_at
      );
      await redisClient.set(
        "blackswan:bull_peak:latest",
        JSON.stringify(docPayload)
      );
      try {
        await redisClient.incr("blackswan:bull_peak:data_count");
      } catch (_) {}
      console.log(
        `✅ [BULL_PEAK] Snapshot saved with ${docPayload.indicators.length} indicators at ${docPayload.collected_at}`
      );

      if (this.publicLogStreamer) {
        this.publicLogStreamer.emitPublicLog(
          "Bull Market Peak Indicators Synced"
        );
      }
    } catch (error) {
      console.error("❌ [BULL_PEAK] Persist error:", error.message || error);
    }
  }
}

// Daily OHLCV history collector (CoinGlass) for BTC, ETH, SOL
class DailyOHLCVCollector {
  constructor(publicLogStreamer = null) {
    this.isRunning = false;
    this.publicLogStreamer = publicLogStreamer;
    this.symbolMap = {
      bitcoin: {
        symbolPair: "BTCUSDT",
        redisKey: "blackswan:bitcoin:daily_ohlcv",
      },
      ethereum: {
        symbolPair: "ETHUSDT",
        redisKey: "blackswan:ethereum:daily_ohlcv",
      },
      solana: {
        symbolPair: "SOLUSDT",
        redisKey: "blackswan:solana:daily_ohlcv",
      },
    };
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;

    console.log(
      "🚀 Starting Daily OHLCV collector (CoinGlass, 1d, ~3y window)..."
    );

    // Run once shortly after startup to populate cache
    setTimeout(() => this.collectAll().catch(() => {}), 10_000);

    // Schedule daily run at 02:10 UTC to ensure previous day candle is closed across exchanges
    cron.schedule("10 2 * * *", async () => {
      try {
        await this.collectAll();
      } catch (e) {
        console.error(
          "❌ [DAILY_OHLCV] Scheduled collection error:",
          e.message || e
        );
      }
    });
  }

  stop() {
    this.isRunning = false;
    console.log("🛑 Stopping Daily OHLCV collector");
  }

  async collectAll() {
    console.log(
      "📈 [DAILY_OHLCV] Collecting daily OHLCV for BTC, ETH, SOL (CoinGlass)..."
    );
    const entries = Object.entries(this.symbolMap);

    for (const [cryptoId, cfg] of entries) {
      try {
        const data = await this.fetchDailyHistory(cfg.symbolPair);
        if (!data || data.length === 0) {
          console.warn(
            `⚠️ [DAILY_OHLCV] No data returned for ${cryptoId} (${cfg.symbolPair})`
          );
          continue;
        }
        await this.storeDailyOhlcv(cfg.redisKey, cryptoId, data);
        const lastTs = data[data.length - 1]?.timestamp;
        console.log(
          `✅ [DAILY_OHLCV] ${cryptoId.toUpperCase()} stored ${
            data.length
          } daily candles. Latest: ${
            lastTs ? new Date(lastTs).toISOString() : "unknown"
          }`
        );
      } catch (e) {
        console.error(
          `❌ [DAILY_OHLCV] Failed for ${cryptoId}:`,
          e.message || e
        );
      }
    }

    if (this.publicLogStreamer) {
      this.publicLogStreamer.emitPublicLog("Daily OHLCV (BTC/ETH/SOL) Synced");
    }
  }

  async fetchDailyHistory(symbolPair) {
    await rateLimiters.coinglass.waitForRateLimit();
    const url = `${APIS.COINGLASS.BASE_URL}/api/futures/price/history`;
    const params = {
      exchange: "Binance",
      symbol: symbolPair,
      interval: "1d",
      limit: 1100, // ~3 years
    };

    try {
      const response = await axios.get(url, {
        params,
        headers: { "CG-API-KEY": APIS.COINGLASS.API_KEY },
        timeout: APIS.COINGLASS.TIMEOUT,
      });

      const payload = response.data;
      if (!payload) return [];

      // Some CoinGlass endpoints use {success, data}, others may return data directly
      const raw = Array.isArray(payload)
        ? payload
        : Array.isArray(payload.data)
        ? payload.data
        : [];
      const normalized = raw
        .map((c) => this.normalizeCandle(c))
        .filter(
          (c) =>
            c &&
            Number.isFinite(c.open) &&
            Number.isFinite(c.close) &&
            Number.isFinite(c.high) &&
            Number.isFinite(c.low) &&
            Number.isFinite(c.volume) &&
            Number.isFinite(c.timestamp)
        );

      // Sort ascending by timestamp
      normalized.sort((a, b) => a.timestamp - b.timestamp);
      return normalized;
    } catch (e) {
      console.error(
        "❌ [DAILY_OHLCV] CoinGlass fetch error:",
        e.response?.data || e.message || e
      );
      return [];
    }
  }

  normalizeCandle(item) {
    if (!item || typeof item !== "object") return null;
    const pick = (obj, keys) => {
      for (const k of keys) {
        if (obj[k] !== undefined && obj[k] !== null) return obj[k];
      }
      return null;
    };

    const tsRaw = pick(item, ["t", "timestamp", "time", "T", "ts"]);
    const openRaw = pick(item, ["open", "o", "openPrice", "Open"]);
    const highRaw = pick(item, ["high", "h", "highPrice", "High"]);
    const lowRaw = pick(item, ["low", "l", "lowPrice", "Low"]);
    const closeRaw = pick(item, ["close", "c", "closePrice", "Close"]);
    const volRaw = pick(item, ["volume", "v", "vol", "Volume"]);

    const toNum = (v) => {
      const n = typeof v === "string" ? parseFloat(v) : Number(v);
      return Number.isFinite(n) ? n : null;
    };

    // Timestamps may be in seconds; if so, convert to ms
    let ts = toNum(tsRaw);
    if (ts && ts < 10_000_000_000) ts = ts * 1000;

    return {
      timestamp: ts,
      open: toNum(openRaw),
      high: toNum(highRaw),
      low: toNum(lowRaw),
      close: toNum(closeRaw),
      volume: toNum(volRaw) ?? 0,
      source: "coinglass",
      interval: "1d",
    };
  }

  async storeDailyOhlcv(redisKey, cryptoId, candles) {
    try {
      await redisClient.set(redisKey, JSON.stringify(candles));
      await redisClient.set(
        `blackswan:${cryptoId}:daily_last_update`,
        new Date().toISOString()
      );
      await redisClient.set(
        `blackswan:${cryptoId}:daily_count`,
        candles.length.toString()
      );
    } catch (e) {
      console.error(
        `❌ [DAILY_OHLCV] Redis store error for ${cryptoId}:`,
        e.message || e
      );
    }
  }
}

// Snapshot service for Firestore backup
class SnapshotService {
  constructor() {
    this.isRunning = false;
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;

    console.log("🚀 Starting 30-minute snapshot service...");

    // Create snapshots every 30 minutes (at :00 and :30)
    cron.schedule("0,30 * * * *", async () => {
      try {
        await this.createSnapshot();
      } catch (error) {
        console.error("❌ [SNAPSHOT] Error creating snapshot:", error);
      }
    });
  }

  async createSnapshot() {
    try {
      console.log("📸 [SNAPSHOT] Creating Firestore snapshot...");

      // Get recent data (last 6 hours only for snapshots - enough for recovery)
      const sixHoursAgo = Date.now() - 6 * 60 * 60 * 1000; // 6 hours ago

      const allData = {
        bitcoin: await this.getRedisData("blackswan:bitcoin:minute_data"),
        ethereum: await this.getRedisData("blackswan:ethereum:minute_data"),
        solana: await this.getRedisData("blackswan:solana:minute_data"),
        sp500: await this.getRedisData("blackswan:sp500:minute_data"),
        fear_greed: await this.getRedisData("blackswan:fear_greed:minute_data"),
        currency: await this.getRedisData("blackswan:currency:minute_data"),
        bull_peak_latest: await this.getRedisData("blackswan:bull_peak:latest"),
      };

      // Filter to recent data only and sample for efficiency
      const recentData = {};
      Object.keys(allData).forEach((key) => {
        if (allData[key] && Array.isArray(allData[key])) {
          // Keep only last 6 hours of data
          const filtered = allData[key].filter(
            (item) => item.timestamp >= sixHoursAgo
          );
          // Sample every 5th data point to reduce size (still gives good coverage)
          recentData[key] = filtered.filter((item, index) => index % 5 === 0);
        } else {
          recentData[key] = allData[key];
        }
      });

      const snapshot = {
        timestamp: Date.now(),
        created_at: new Date().toISOString(),
        note: "Snapshot contains last 6 hours of data (sampled every 5 minutes) for recovery purposes",
        data: recentData,
        metadata: {
          last_updates: {
            bitcoin: await this.getRedisData("blackswan:bitcoin:last_update"),
            ethereum: await this.getRedisData("blackswan:ethereum:last_update"),
            solana: await this.getRedisData("blackswan:solana:last_update"),
            sp500: await this.getRedisData("blackswan:sp500:last_update"),
            fear_greed: await this.getRedisData(
              "blackswan:fear_greed:last_update"
            ),
            currency: await this.getRedisData("blackswan:currency:last_update"),
            bull_peak: await this.getRedisData(
              "blackswan:bull_peak:last_update"
            ),
          },
          data_counts: {
            bitcoin: recentData.bitcoin?.length || 0,
            ethereum: recentData.ethereum?.length || 0,
            solana: recentData.solana?.length || 0,
            sp500: recentData.sp500?.length || 0,
            fear_greed: recentData.fear_greed?.length || 0,
            currency: recentData.currency?.length || 0,
            bull_peak: allData.bull_peak_latest ? 1 : 0,
          },
          full_data_counts: {
            bitcoin: allData.bitcoin?.length || 0,
            ethereum: allData.ethereum?.length || 0,
            solana: allData.solana?.length || 0,
            sp500: allData.sp500?.length || 0,
            fear_greed: allData.fear_greed?.length || 0,
            currency: allData.currency?.length || 0,
            bull_peak: allData.bull_peak_latest ? 1 : 0,
          },
        },
      };

      // Save to Firestore (replaces previous snapshot)
      await db.collection("blackswan-snapshots").doc("latest").set(snapshot);

      console.log(
        `✅ [SNAPSHOT] Efficient snapshot created with ${JSON.stringify(
          snapshot.metadata.data_counts
        )} recent data points (reduced from ${JSON.stringify(
          snapshot.metadata.full_data_counts
        )} full dataset)`
      );
    } catch (error) {
      console.error("❌ [SNAPSHOT] Failed to create snapshot:", error);
    }
  }

  async getRedisData(key) {
    try {
      const data = await redisClient.get(key);
      if (!data) return null;

      // Check if this is a timestamp field (contains 'last_update' or 'data_count')
      if (key.includes("last_update") || key.includes("data_count")) {
        return data; // Return as plain string
      }

      // Otherwise, parse as JSON
      return JSON.parse(data);
    } catch (error) {
      console.error(
        `❌ [SNAPSHOT] Error getting Redis data for ${key}:`,
        error
      );
      return null;
    }
  }

  stop() {
    this.isRunning = false;
    console.log("🛑 Stopping snapshot service");
  }
}

// Express app setup
const app = express();
const port = process.env.PORT || 8083;
const logStreamer = new LogStreamer();
const publicLogStreamer = new PublicLogStreamer();

// Initialize data collectors
const cryptoCollector = new CryptocurrencyDataCollector(publicLogStreamer);
const sp500Collector = new SP500DataCollector(publicLogStreamer);
const fearGreedCollector = new FearGreedDataCollector(publicLogStreamer);
const currencyCollector = new CurrencyDataCollector(publicLogStreamer);
const bullPeakCollector = new BullMarketPeakIndicatorCollector(
  publicLogStreamer
);
const snapshotService = new SnapshotService();
const dailyOHLCVCollector = new DailyOHLCVCollector(publicLogStreamer);

// Setup log streaming
logStreamer.on("log", (logEntry) => {
  logStreamer.broadcastLog(logEntry);
});

publicLogStreamer.on("publicLog", (logEntry) => {
  publicLogStreamer.broadcastLog(logEntry);
});

app.set("trust proxy", 1);

// Rate limiting for API endpoints
const apiLimiter = rateLimit({
  windowMs: 1 * 60 * 1000, // 1 minute
  max: 1000000, // 100 requests per minute per IP
  message: {
    success: false,
    message: "Too many requests. Please try again later.",
  },
  standardHeaders: true,
  legacyHeaders: false,
});

// Middleware
app.use(helmet());
app.use(cors());
app.use(compression());
app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: true }));

// CORS preflight handler for log streaming endpoint
app.options("/logs/stream", (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Accept, Cache-Control, Content-Type, X-Requested-With"
  );
  res.setHeader("Access-Control-Max-Age", "86400"); // 24 hours
  res.status(204).end();
});

// Log streaming endpoint (Server-Sent Events)
app.get("/logs/stream", (req, res) => {
  try {
    // Add this client to the log streamer
    logStreamer.addClient(res);
  } catch (error) {
    console.error("❌ [LOG_STREAM] Error setting up stream:", error.message);
    res.status(500).json({
      success: false,
      error: "Failed to setup log stream",
      message: error.message,
    });
  }
});

// Bull Market Peak Indicators - latest
app.get("/bull-peak", apiLimiter, async (req, res) => {
  try {
    const latest = await redisClient.get("blackswan:bull_peak:latest");
    const lastUpdate = await redisClient.get("blackswan:bull_peak:last_update");

    if (latest) {
      return res
        .status(200)
        .json({ data: JSON.parse(latest), last_update: lastUpdate });
    }

    // Fallback to Firestore latest if Redis not yet populated
    const doc = await db
      .collection("bull-market-peak-indicators")
      .doc("latest")
      .get();
    if (!doc.exists) {
      return res
        .status(404)
        .json({ error: "No Bull Market Peak Indicators available" });
    }
    const data = doc.data();
    return res
      .status(200)
      .json({ data, last_update: data?.collected_at || null });
  } catch (error) {
    console.error("Error fetching Bull Market Peak Indicators:", error);
    res
      .status(500)
      .json({ error: "Failed to fetch Bull Market Peak Indicators" });
  }
});

// Public log streaming endpoint (Server-Sent Events)
app.get("/publicStream", (req, res) => {
  try {
    // Add this client to the public log streamer
    publicLogStreamer.addClient(res);
  } catch (error) {
    console.error(
      "❌ [PUBLIC_STREAM] Error setting up public stream:",
      error.message
    );
    res.status(500).json({
      success: false,
      error: "Failed to setup public stream",
      message: error.message,
    });
  }
});

// Health check endpoint
app.get("/health", async (req, res) => {
  try {
    const redisStatus = redisClient.status === "ready";
    let redisPing = false;

    if (redisStatus) {
      try {
        await redisClient.ping();
        redisPing = true;
      } catch (error) {
        console.error("Redis ping failed:", error);
      }
    }

    // Get last update times and data counts
    const lastUpdates = {
      bitcoin: await redisClient.get("blackswan:bitcoin:last_update"),
      ethereum: await redisClient.get("blackswan:ethereum:last_update"),
      solana: await redisClient.get("blackswan:solana:last_update"),
      sp500: await redisClient.get("blackswan:sp500:last_update"),
      fear_greed: await redisClient.get("blackswan:fear_greed:last_update"),
      currency: await redisClient.get("blackswan:currency:last_update"),
      bull_peak: await redisClient.get("blackswan:bull_peak:last_update"),
    };

    const dataCounts = {
      bitcoin: parseInt(
        (await redisClient.get("blackswan:bitcoin:data_count")) || "0"
      ),
      ethereum: parseInt(
        (await redisClient.get("blackswan:ethereum:data_count")) || "0"
      ),
      solana: parseInt(
        (await redisClient.get("blackswan:solana:data_count")) || "0"
      ),
      sp500: parseInt(
        (await redisClient.get("blackswan:sp500:data_count")) || "0"
      ),
      fear_greed: parseInt(
        (await redisClient.get("blackswan:fear_greed:data_count")) || "0"
      ),
      currency: parseInt(
        (await redisClient.get("blackswan:currency:data_count")) || "0"
      ),
      bull_peak: parseInt(
        (await redisClient.get("blackswan:bull_peak:data_count")) || "0"
      ),
    };

    res.status(200).json({
      status: "healthy",
      timestamp: new Date().toISOString(),
      redis: {
        connected: redisStatus,
        ping: redisPing,
        status: redisClient.status,
      },
      firebase: admin.apps.length > 0,
      services: {
        cryptocurrencies: cryptoCollector.isRunning,
        sp500: sp500Collector.isRunning,
        fear_greed: fearGreedCollector.isRunning,
        currency: currencyCollector.isRunning,
        bull_peak: bullPeakCollector.isRunning,
        snapshots: snapshotService.isRunning,
      },
      last_updates: lastUpdates,
      data_counts: dataCounts,
      data_retention_days: 7,
      cryptocurrency_collection_status: {
        last_processed_timestamp: cryptoCollector.lastProcessedTimestamp
          ? new Date(cryptoCollector.lastProcessedTimestamp).toISOString()
          : null,
      },
      sp500_collection_status: {
        last_processed_timestamp: sp500Collector.lastProcessedTimestamp
          ? new Date(sp500Collector.lastProcessedTimestamp).toISOString()
          : null,
        market_status: sp500Collector.isMarketHours() ? "OPEN" : "CLOSED",
        collection_mode: "24/7",
      },
      fear_greed_collection_status: {
        last_processed_timestamp: fearGreedCollector.lastProcessedTimestamp
          ? new Date(fearGreedCollector.lastProcessedTimestamp).toISOString()
          : null,
      },
      currency_collection_status: {
        last_processed_timestamp: currencyCollector.lastProcessedTimestamp
          ? new Date(currencyCollector.lastProcessedTimestamp).toISOString()
          : null,
      },
      logStreamer: logStreamer.getStats(),
    });
  } catch (error) {
    console.error("Health check error:", error);
    res.status(500).json({
      status: "unhealthy",
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// Generic function to get cryptocurrency data
const getCryptocurrencyData = async (cryptoId, cryptoName) => {
  const data = await redisClient.get(`blackswan:${cryptoId}:minute_data`);

  if (!data) {
    return null;
  }

  let cryptoData = JSON.parse(data);
  return {
    data: cryptoData,
    lastUpdate: await redisClient.get(`blackswan:${cryptoId}:last_update`),
    count: (await redisClient.get(`blackswan:${cryptoId}:data_count`)) || "0",
  };
};

// Generic cryptocurrency endpoint handler
const handleCryptocurrencyRequest = async (req, res, cryptoId, cryptoName) => {
  try {
    const { hours, detailed } = req.query;
    const result = await getCryptocurrencyData(cryptoId, cryptoName);

    if (!result) {
      return res.status(404).json({ error: `No ${cryptoName} data available` });
    }

    let cryptoData = result.data;

    // Filter by hours if specified
    if (hours) {
      const hoursBack = parseInt(hours, 10);
      if (!isNaN(hoursBack)) {
        const cutoffTime = Date.now() - hoursBack * 60 * 60 * 1000;
        cryptoData = cryptoData.filter((item) => item.timestamp >= cutoffTime);
      }
    }

    // Sort by timestamp to ensure proper order
    cryptoData.sort((a, b) => a.timestamp - b.timestamp);

    // Calculate data statistics
    const now = Date.now();
    const oneHourAgo = now - 60 * 60 * 1000;
    const oneDayAgo = now - 24 * 60 * 60 * 1000;

    const recentData = cryptoData.filter(
      (item) => item.timestamp >= oneHourAgo
    );
    const dayData = cryptoData.filter((item) => item.timestamp >= oneDayAgo);

    // Calculate minute gaps (should be minimal for good data quality)
    const minuteGaps = [];
    for (let i = 1; i < cryptoData.length; i++) {
      const timeDiff = cryptoData[i].timestamp - cryptoData[i - 1].timestamp;
      const minuteDiff = timeDiff / (60 * 1000);
      if (minuteDiff > 1.5) {
        // Allow some tolerance
        minuteGaps.push({
          gap_minutes: Math.round(minuteDiff),
          from: new Date(cryptoData[i - 1].timestamp).toISOString(),
          to: new Date(cryptoData[i].timestamp).toISOString(),
        });
      }
    }

    const response = {
      data: cryptoData,
      count: cryptoData.length,
      last_update: result.lastUpdate,
      retention_days: 7,
      data_quality: {
        total_minutes_expected_7d: 7 * 24 * 60,
        actual_data_points: cryptoData.length,
        coverage_percentage: (
          (cryptoData.length / (7 * 24 * 60)) *
          100
        ).toFixed(2),
        recent_hour_points: recentData.length,
        recent_day_points: dayData.length,
        minute_gaps_count: minuteGaps.length,
        data_completeness:
          minuteGaps.length === 0
            ? "excellent"
            : minuteGaps.length < 10
            ? "good"
            : "needs_attention",
      },
      time_range:
        cryptoData.length > 0
          ? {
              earliest: new Date(cryptoData[0].timestamp).toISOString(),
              latest: new Date(
                cryptoData[cryptoData.length - 1].timestamp
              ).toISOString(),
              span_hours: (
                (cryptoData[cryptoData.length - 1].timestamp -
                  cryptoData[0].timestamp) /
                (60 * 60 * 1000)
              ).toFixed(1),
            }
          : null,
    };

    // Include detailed gap analysis if requested
    if (detailed === "true" && minuteGaps.length > 0) {
      response.minute_gaps = minuteGaps.slice(0, 20); // Show up to 20 gaps
    }

    res.status(200).json(response);
  } catch (error) {
    console.error(`Error fetching ${cryptoName} data:`, error);
    res.status(500).json({ error: `Failed to fetch ${cryptoName} data` });
  }
};

// Get Bitcoin data
app.get("/bitcoin", apiLimiter, async (req, res) => {
  await handleCryptocurrencyRequest(req, res, "bitcoin", "Bitcoin");
});

// Get Ethereum data
app.get("/ethereum", apiLimiter, async (req, res) => {
  await handleCryptocurrencyRequest(req, res, "ethereum", "Ethereum");
});

// Get Solana data
app.get("/solana", apiLimiter, async (req, res) => {
  await handleCryptocurrencyRequest(req, res, "solana", "Solana");
});

// Get Daily OHLCV data (CoinGlass) for BTC/ETH/SOL
app.get("/bitcoin/daily", apiLimiter, async (req, res) => {
  try {
    const { days } = req.query;
    const raw = await redisClient.get("blackswan:bitcoin:daily_ohlcv");
    if (!raw)
      return res
        .status(404)
        .json({ error: "No Bitcoin daily OHLCV available" });
    let data = JSON.parse(raw);
    if (days) {
      const d = parseInt(days, 10);
      if (!isNaN(d) && d > 0) {
        data = data.slice(-d);
      }
    }
    res.status(200).json({
      data,
      count: data.length,
      last_update: await redisClient.get("blackswan:bitcoin:daily_last_update"),
      source: "coinglass",
      interval: "1d",
    });
  } catch (e) {
    console.error("Error fetching Bitcoin daily OHLCV:", e);
    res.status(500).json({ error: "Failed to fetch Bitcoin daily OHLCV" });
  }
});

app.get("/ethereum/daily", apiLimiter, async (req, res) => {
  try {
    const { days } = req.query;
    const raw = await redisClient.get("blackswan:ethereum:daily_ohlcv");
    if (!raw)
      return res
        .status(404)
        .json({ error: "No Ethereum daily OHLCV available" });
    let data = JSON.parse(raw);
    if (days) {
      const d = parseInt(days, 10);
      if (!isNaN(d) && d > 0) {
        data = data.slice(-d);
      }
    }
    res.status(200).json({
      data,
      count: data.length,
      last_update: await redisClient.get(
        "blackswan:ethereum:daily_last_update"
      ),
      source: "coinglass",
      interval: "1d",
    });
  } catch (e) {
    console.error("Error fetching Ethereum daily OHLCV:", e);
    res.status(500).json({ error: "Failed to fetch Ethereum daily OHLCV" });
  }
});

app.get("/solana/daily", apiLimiter, async (req, res) => {
  try {
    const { days } = req.query;
    const raw = await redisClient.get("blackswan:solana:daily_ohlcv");
    if (!raw)
      return res.status(404).json({ error: "No Solana daily OHLCV available" });
    let data = JSON.parse(raw);
    if (days) {
      const d = parseInt(days, 10);
      if (!isNaN(d) && d > 0) {
        data = data.slice(-d);
      }
    }
    res.status(200).json({
      data,
      count: data.length,
      last_update: await redisClient.get("blackswan:solana:daily_last_update"),
      source: "coinglass",
      interval: "1d",
    });
  } catch (e) {
    console.error("Error fetching Solana daily OHLCV:", e);
    res.status(500).json({ error: "Failed to fetch Solana daily OHLCV" });
  }
});

// Get S&P 500 data
app.get("/sp500", apiLimiter, async (req, res) => {
  try {
    const { hours } = req.query;
    const data = await redisClient.get("blackswan:sp500:minute_data");

    if (!data) {
      return res.status(404).json({ error: "No S&P 500 data available" });
    }

    let sp500Data = JSON.parse(data);

    if (hours) {
      const hoursBack = parseInt(hours, 10);
      if (!isNaN(hoursBack)) {
        const cutoffTime = Date.now() - hoursBack * 60 * 60 * 1000;
        sp500Data = sp500Data.filter((item) => item.timestamp >= cutoffTime);
      }
    }

    res.status(200).json({
      data: sp500Data,
      count: sp500Data.length,
      last_update: await redisClient.get("blackswan:sp500:last_update"),
      retention_days: 7,
      current_market_status: sp500Collector.isMarketHours() ? "OPEN" : "CLOSED",
      collection_mode: "24/7 (tracks latest available price)",
    });
  } catch (error) {
    console.error("Error fetching S&P 500 data:", error);
    res.status(500).json({ error: "Failed to fetch S&P 500 data" });
  }
});

// Get Fear & Greed data
app.get("/fear-greed", apiLimiter, async (req, res) => {
  try {
    const data = await redisClient.get("blackswan:fear_greed:minute_data");

    if (!data) {
      return res.status(404).json({ error: "No Fear & Greed data available" });
    }

    const fearGreedData = JSON.parse(data);

    res.status(200).json({
      data: fearGreedData,
      count: fearGreedData.length,
      last_update: await redisClient.get("blackswan:fear_greed:last_update"),
      retention_days: 7,
      collection_interval: "60 seconds",
    });
  } catch (error) {
    console.error("Error fetching Fear & Greed data:", error);
    res.status(500).json({ error: "Failed to fetch Fear & Greed data" });
  }
});

// Get Currency data
app.get("/currency", apiLimiter, async (req, res) => {
  try {
    const { hours, pair } = req.query;
    const data = await redisClient.get("blackswan:currency:minute_data");

    if (!data) {
      return res.status(404).json({ error: "No currency data available" });
    }

    let currencyData = JSON.parse(data);

    if (hours) {
      const hoursBack = parseInt(hours, 10);
      if (!isNaN(hoursBack)) {
        const cutoffTime = Date.now() - hoursBack * 60 * 60 * 1000;
        currencyData = currencyData.filter(
          (item) => item.timestamp >= cutoffTime
        );
      }
    }

    // Filter by specific currency pair if requested
    if (pair) {
      const upperPair = pair.toUpperCase();
      currencyData = currencyData
        .map((item) => ({
          ...item,
          rates: item.rates[`USD${upperPair}`]
            ? { [`USD${upperPair}`]: item.rates[`USD${upperPair}`] }
            : {},
        }))
        .filter((item) => Object.keys(item.rates).length > 0);
    }

    res.status(200).json({
      data: currencyData,
      count: currencyData.length,
      last_update: await redisClient.get("blackswan:currency:last_update"),
      retention_days: 7,
      collection_interval: "60 seconds",
      available_pairs: ["USDGBP", "USDEUR", "USDJPY", "USDCHF"],
    });
  } catch (error) {
    console.error("Error fetching currency data:", error);
    res.status(500).json({ error: "Failed to fetch currency data" });
  }
});

// Get dashboard stats (current vs 5 minutes ago)
app.get("/dashboard-stats", apiLimiter, async (req, res) => {
  try {
    console.log("📊 [DASHBOARD_STATS] Fetching dashboard statistics...");

    // Fetch all data in parallel
    const [
      bitcoinData,
      ethereumData,
      solanaData,
      sp500Data,
      fearGreedData,
      currencyData,
    ] = await Promise.all([
      redisClient.get("blackswan:bitcoin:minute_data"),
      redisClient.get("blackswan:ethereum:minute_data"),
      redisClient.get("blackswan:solana:minute_data"),
      redisClient.get("blackswan:sp500:minute_data"),
      redisClient.get("blackswan:fear_greed:minute_data"),
      redisClient.get("blackswan:currency:minute_data"),
    ]);

    const now = Date.now();
    const fiveMinutesAgo = now - 5 * 60 * 1000; // 5 minutes in milliseconds

    const generateDashboardStat = (
      data,
      currentValueExtractor,
      previousValueExtractor,
      formatter
    ) => {
      if (!data || data.length === 0) {
        return null;
      }

      // Sort by timestamp descending
      data.sort((a, b) => b.timestamp - a.timestamp);

      // Get current (most recent) value
      const currentItem = data[0];
      const currentValue = currentValueExtractor(currentItem);

      // Find closest item to 5 minutes ago
      const fiveMinItem =
        data.find((item) => item.timestamp <= fiveMinutesAgo) ||
        data[data.length - 1];
      const previousValue = previousValueExtractor(fiveMinItem);

      // Calculate change
      const change = currentValue - previousValue;
      const changePercent =
        previousValue !== 0 ? (change / previousValue) * 100 : 0;

      return {
        current: formatter(currentValue),
        currentRaw: currentValue,
        previous: formatter(previousValue),
        previousRaw: previousValue,
        change: change,
        changePercent: changePercent,
        changeType: change > 0 ? "up" : change < 0 ? "down" : "neutral",
        lastUpdated: new Date(currentItem.timestamp).toISOString(),
        dataAge: Math.round((now - currentItem.timestamp) / (60 * 1000)), // age in minutes
      };
    };

    const stats = {};

    // Bitcoin Price
    if (bitcoinData) {
      const bitcoin = JSON.parse(bitcoinData);
      stats.bitcoin = generateDashboardStat(
        bitcoin,
        (item) => item.price,
        (item) => item.price,
        (value) => parseFloat(value).toFixed(2)
      );
    }

    // Ethereum Price
    if (ethereumData) {
      const ethereum = JSON.parse(ethereumData);
      stats.ethereum = generateDashboardStat(
        ethereum,
        (item) => item.price,
        (item) => item.price,
        (value) => parseFloat(value).toFixed(2)
      );
    }

    // Solana Price
    if (solanaData) {
      const solana = JSON.parse(solanaData);
      stats.solana = generateDashboardStat(
        solana,
        (item) => item.price,
        (item) => item.price,
        (value) => parseFloat(value).toFixed(2)
      );
    }

    // S&P 500 Price
    if (sp500Data) {
      const sp500 = JSON.parse(sp500Data);
      stats.sp500 = generateDashboardStat(
        sp500,
        (item) => item.price,
        (item) => item.price,
        (value) => parseFloat(value).toFixed(2)
      );
    }

    // Crypto Fear & Greed - only if data actually exists
    if (fearGreedData) {
      const fearGreed = JSON.parse(fearGreedData);

      // Check if any recent data points actually have crypto fear & greed values
      const recentData = [...fearGreed]
        .sort((a, b) => b.timestamp - a.timestamp)
        .slice(0, 10);
      const hasCryptoData = recentData.some(
        (item) =>
          item.crypto_fear_greed &&
          item.crypto_fear_greed.value != null &&
          item.crypto_fear_greed.value > 0
      );

      if (hasCryptoData) {
        stats.crypto_fear_greed = generateDashboardStat(
          fearGreed,
          (item) => item.crypto_fear_greed?.value || 0,
          (item) => item.crypto_fear_greed?.value || 0,
          (value) => Math.round(value).toString()
        );

        // Add classification for current value
        if (stats.crypto_fear_greed) {
          const current = stats.crypto_fear_greed.currentRaw;
          stats.crypto_fear_greed.classification =
            current >= 75
              ? "Extreme Greed"
              : current >= 55
              ? "Greed"
              : current >= 45
              ? "Neutral"
              : current >= 25
              ? "Fear"
              : "Extreme Fear";
        }
      }
    }

    // Traditional Fear & Greed (VIX-derived) - only if data actually exists
    if (fearGreedData) {
      const fearGreed = JSON.parse(fearGreedData);

      // Check if any recent data points actually have traditional fear & greed values
      const recentData = [...fearGreed]
        .sort((a, b) => b.timestamp - a.timestamp)
        .slice(0, 10);
      const hasTraditionalData = recentData.some(
        (item) =>
          item.traditional_fear_greed &&
          item.traditional_fear_greed.value != null &&
          item.traditional_fear_greed.value > 0
      );

      if (hasTraditionalData) {
        stats.traditional_fear_greed = generateDashboardStat(
          fearGreed,
          (item) => item.traditional_fear_greed?.value || 0,
          (item) => item.traditional_fear_greed?.value || 0,
          (value) => Math.round(value).toString()
        );

        // Add VIX value and classification for current value
        if (stats.traditional_fear_greed) {
          const fearGreedSorted = [...fearGreed].sort(
            (a, b) => b.timestamp - a.timestamp
          );
          const currentItem = fearGreedSorted[0];
          stats.traditional_fear_greed.vix_value =
            currentItem.traditional_fear_greed?.vix_value || 0;

          const current = stats.traditional_fear_greed.currentRaw;
          stats.traditional_fear_greed.classification =
            current >= 75
              ? "Extreme Greed"
              : current >= 55
              ? "Greed"
              : current >= 45
              ? "Neutral"
              : current >= 25
              ? "Fear"
              : "Extreme Fear";
        }
      }
    }

    // Currency Pairs - only include pairs that have actual data
    if (currencyData) {
      const currency = JSON.parse(currencyData);
      const pairs = ["USDGBP", "USDEUR", "USDJPY", "USDCHF"];

      pairs.forEach((pair) => {
        // Check if this currency pair has valid data in recent entries
        const recentData = [...currency]
          .sort((a, b) => b.timestamp - a.timestamp)
          .slice(0, 10);
        const hasData = recentData.some(
          (item) => item.rates?.[pair]?.rate && item.rates[pair].rate > 0
        );

        if (hasData) {
          const stat = generateDashboardStat(
            currency,
            (item) => item.rates?.[pair]?.rate || 0,
            (item) => item.rates?.[pair]?.rate || 0,
            (value) => parseFloat(value).toFixed(4)
          );

          // Only add to stats if we got a valid stat with actual values
          if (stat && stat.currentRaw > 0) {
            stats[pair.toLowerCase()] = stat;
          }
        }
      });
    }

    // Get last update times
    const lastUpdates = {
      bitcoin: await redisClient.get("blackswan:bitcoin:last_update"),
      ethereum: await redisClient.get("blackswan:ethereum:last_update"),
      solana: await redisClient.get("blackswan:solana:last_update"),
      sp500: await redisClient.get("blackswan:sp500:last_update"),
      fear_greed: await redisClient.get("blackswan:fear_greed:last_update"),
      currency: await redisClient.get("blackswan:currency:last_update"),
    };

    const response = {
      stats,
      metadata: {
        timestamp: new Date().toISOString(),
        comparison_period: "5_minutes",
        last_updates: lastUpdates,
        data_sources: {
          bitcoin: !!bitcoinData,
          ethereum: !!ethereumData,
          solana: !!solanaData,
          sp500: !!sp500Data,
          fear_greed: !!fearGreedData,
          currency: !!currencyData,
        },
      },
    };

    const availableDataSources = Object.keys(stats);
    console.log(
      `✅ [DASHBOARD_STATS] Generated stats for ${
        availableDataSources.length
      } metrics: ${availableDataSources.join(", ")}`
    );

    // Add available data sources to response for debugging
    response.metadata.available_data_sources = availableDataSources;

    res.status(200).json(response);
  } catch (error) {
    console.error(
      "❌ [DASHBOARD_STATS] Error generating dashboard stats:",
      error
    );
    res.status(500).json({
      error: "Failed to generate dashboard stats",
      message: error.message,
    });
  }
});

// Get logs for polling (last N entries)
app.get("/logs/poll", apiLimiter, async (req, res) => {
  try {
    const { limit = 20, since } = req.query;
    const maxLimit = Math.min(parseInt(limit) || 20, 100); // Cap at 100 entries

    let logs = [...logStreamer.logBuffer];

    // Filter by timestamp if 'since' parameter provided
    if (since) {
      const sinceTimestamp = new Date(since).getTime();
      if (!isNaN(sinceTimestamp)) {
        logs = logs.filter((log) => {
          const logTimestamp = new Date(log.timestamp).getTime();
          return logTimestamp > sinceTimestamp;
        });
      }
    }

    // Sort by timestamp descending (newest first) and limit
    logs.sort(
      (a, b) =>
        new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
    );
    const limitedLogs = logs.slice(0, maxLimit);

    // Convert to format expected by frontend
    const formattedLogs = limitedLogs.map((log) => ({
      id: `${log.timestamp}_${Math.random().toString(36).substr(2, 9)}`,
      timestamp: new Date(log.timestamp).toLocaleString(),
      level: log.level.toLowerCase(), // Convert to lowercase for consistency
      message: log.message,
      source: "data-collection-service",
    }));

    res.status(200).json({
      logs: formattedLogs,
      count: formattedLogs.length,
      total_available: logStreamer.logBuffer.length,
      timestamp: new Date().toISOString(),
      has_more: logs.length > maxLimit,
    });
  } catch (error) {
    console.error("❌ [LOGS_POLL] Error fetching logs:", error);
    res
      .status(500)
      .json({ error: "Failed to fetch logs", message: error.message });
  }
});

// Get all data (consolidated endpoint)
app.get("/all", apiLimiter, async (req, res) => {
  try {
    const { hours } = req.query;
    const cutoffTime = hours
      ? Date.now() - parseInt(hours, 10) * 60 * 60 * 1000
      : null;

    // Fetch all data in parallel
    const [
      bitcoinData,
      ethereumData,
      solanaData,
      sp500Data,
      fearGreedData,
      currencyData,
      bullPeakLatest,
    ] = await Promise.all([
      redisClient.get("blackswan:bitcoin:minute_data"),
      redisClient.get("blackswan:ethereum:minute_data"),
      redisClient.get("blackswan:solana:minute_data"),
      redisClient.get("blackswan:sp500:minute_data"),
      redisClient.get("blackswan:fear_greed:minute_data"),
      redisClient.get("blackswan:currency:minute_data"),
      redisClient.get("blackswan:bull_peak:latest"),
    ]);

    const filterByTime = (data) => {
      if (!cutoffTime) return data;
      return data.filter((item) => item.timestamp >= cutoffTime);
    };

    const result = {
      bitcoin: bitcoinData ? filterByTime(JSON.parse(bitcoinData)) : [],
      ethereum: ethereumData ? filterByTime(JSON.parse(ethereumData)) : [],
      solana: solanaData ? filterByTime(JSON.parse(solanaData)) : [],
      sp500: sp500Data ? filterByTime(JSON.parse(sp500Data)) : [],
      fear_greed: fearGreedData ? filterByTime(JSON.parse(fearGreedData)) : [],
      currency: currencyData ? filterByTime(JSON.parse(currencyData)) : [],
      bull_peak_latest: bullPeakLatest ? JSON.parse(bullPeakLatest) : null,
    };

    // Get last update times
    const lastUpdates = {
      bitcoin: await redisClient.get("blackswan:bitcoin:last_update"),
      ethereum: await redisClient.get("blackswan:ethereum:last_update"),
      solana: await redisClient.get("blackswan:solana:last_update"),
      sp500: await redisClient.get("blackswan:sp500:last_update"),
      fear_greed: await redisClient.get("blackswan:fear_greed:last_update"),
      currency: await redisClient.get("blackswan:currency:last_update"),
    };

    res.status(200).json({
      data: result,
      counts: {
        bitcoin: result.bitcoin.length,
        ethereum: result.ethereum.length,
        solana: result.solana.length,
        sp500: result.sp500.length,
        fear_greed: result.fear_greed.length,
        currency: result.currency.length,
      },
      last_updates: lastUpdates,
      retention_days: 7,
      filtered_hours: hours ? parseInt(hours, 10) : null,
    });
  } catch (error) {
    console.error("Error fetching all data:", error);
    res.status(500).json({ error: "Failed to fetch data" });
  }
});

// Restore from snapshot endpoint
app.post("/restore-snapshot", apiLimiter, async (req, res) => {
  try {
    console.log("🔄 [RESTORE] Restoring data from Firestore snapshot...");

    // Get latest snapshot from Firestore
    const snapshotDoc = await db
      .collection("blackswan-snapshots")
      .doc("latest")
      .get();

    if (!snapshotDoc.exists) {
      return res.status(404).json({ error: "No snapshot available" });
    }

    const snapshot = snapshotDoc.data();
    const { data, metadata } = snapshot;

    // Restore data to Redis
    const pipeline = redisClient.pipeline();

    if (data.bitcoin) {
      pipeline.set(
        "blackswan:bitcoin:minute_data",
        JSON.stringify(data.bitcoin)
      );
    }
    if (data.ethereum) {
      pipeline.set(
        "blackswan:ethereum:minute_data",
        JSON.stringify(data.ethereum)
      );
    }
    if (data.solana) {
      pipeline.set("blackswan:solana:minute_data", JSON.stringify(data.solana));
    }
    if (data.sp500) {
      pipeline.set("blackswan:sp500:minute_data", JSON.stringify(data.sp500));
    }
    if (data.fear_greed) {
      pipeline.set(
        "blackswan:fear_greed:minute_data",
        JSON.stringify(data.fear_greed)
      );
    }
    if (data.currency) {
      pipeline.set(
        "blackswan:currency:minute_data",
        JSON.stringify(data.currency)
      );
    }

    // Restore last update times
    if (metadata?.last_updates) {
      Object.entries(metadata.last_updates).forEach(([key, value]) => {
        if (value) {
          pipeline.set(`blackswan:${key}:last_update`, value);
        }
      });
    }

    await pipeline.exec();

    console.log(
      `✅ [RESTORE] Data restored from snapshot created at ${snapshot.created_at}`
    );

    res.status(200).json({
      success: true,
      message: "Data restored from snapshot",
      snapshot_date: snapshot.created_at,
      restored_counts: metadata?.data_counts || {},
    });
  } catch (error) {
    console.error("❌ [RESTORE] Error restoring snapshot:", error);
    res.status(500).json({ error: "Failed to restore snapshot" });
  }
});

// Error handler
app.use((err, req, res, next) => {
  console.error("Unhandled error:", err);
  res.status(500).json({ message: "Internal server error" });
});

// Graceful shutdown
const gracefulShutdown = async () => {
  console.log("🛑 Shutting down gracefully...");
  try {
    cryptoCollector.stop();
    sp500Collector.stop();
    fearGreedCollector.stop();
    currencyCollector.stop();
    bullPeakCollector.stop();
    snapshotService.stop();

    // Restore original console
    logStreamer.restoreOriginalConsole();

    await redisClient.quit();
    console.log("✅ Service shutdown completed");
  } catch (error) {
    console.error("❌ Error during shutdown:", error);
  }
  process.exit(0);
};

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);

// Start server
async function startServer() {
  try {
    console.log("🚀 BlackSwan Data Collection Service Starting...");
    console.log("");
    console.log("📊 DATA SOURCES:");
    console.log("  🔸 Bitcoin Price: CoinGecko API (1-minute intervals)");
    console.log(
      "  🔸 S&P 500: Alpha Vantage + Yahoo Finance (1-minute intervals, 24/7)"
    );
    console.log(
      "  🔸 Fear & Greed: Alternative.me (crypto) + VIX-derived (traditional) - 60-second intervals"
    );
    console.log(
      "  🔸 USD Currency: Alpha Vantage + ExchangeRate-API (60-second intervals)"
    );
    console.log("");
    console.log("💾 STORAGE:");
    console.log("  🔸 Redis Cache: Real-time data (7-day retention)");
    console.log("  🔸 Firestore Snapshots: Every 30 minutes backup");
    console.log("");

    // Start all data collection services
    console.log("🚀 Starting data collection services...");

    // Start collectors in parallel
    cryptoCollector.start();
    sp500Collector.start();
    fearGreedCollector.start();
    currencyCollector.start();
    bullPeakCollector.start();
    dailyOHLCVCollector.start();

    // Start snapshot service
    snapshotService.start();

    app.listen(port, () => {
      console.log(
        `🚀 BlackSwan Data Collection Service running on port ${port}`
      );
      console.log("");
      console.log("📡 API ENDPOINTS:");
      console.log(`  🔗 Health Check: GET /health`);
      console.log(`  🔗 Log Stream: GET /logs/stream (SSE)`);
      console.log(`  🔗 Public Stream: GET /publicStream (SSE)`);
      console.log(`  🔗 Bitcoin Data: GET /bitcoin?hours=24`);
      console.log(`  🔗 Ethereum Data: GET /ethereum?hours=24`);
      console.log(`  🔗 Solana Data: GET /solana?hours=24`);
      console.log(`  🔗 Bitcoin Daily OHLCV: GET /bitcoin/daily?days=730`);
      console.log(`  🔗 Ethereum Daily OHLCV: GET /ethereum/daily?days=730`);
      console.log(`  🔗 Solana Daily OHLCV: GET /solana/daily?days=730`);
      console.log(`  🔗 S&P 500 Data: GET /sp500?hours=24`);
      console.log(`  🔗 Fear & Greed: GET /fear-greed`);
      console.log(`  🔗 Currency Data: GET /currency?hours=24&pair=gbp`);
      console.log(`  🔗 Bull Market Peak Indicators (latest): GET /bull-peak`);
      console.log(`  🔗 All Data: GET /all?hours=24`);
      console.log(`  🔗 Restore Snapshot: POST /restore-snapshot`);
      console.log("");
      console.log("⚡ COLLECTION STATUS:");
      console.log(
        "  📈 Cryptocurrencies (Bitcoin, Ethereum, Solana): Every 60 seconds"
      );
      console.log(
        "  📊 S&P 500: Every 60 seconds (24/7 - tracks latest price)"
      );
      console.log("  😱 Fear & Greed: Every 60 seconds");
      console.log("  💱 Currency: Every 60 seconds");
      console.log("  🐂 Bull Market Peak Indicators: Every 15 minutes");
      console.log(
        "  📅 Daily OHLCV (BTC/ETH/SOL via CoinGlass): Once daily at 02:10 UTC"
      );
      console.log("  📸 Snapshots: Every 30 minutes");
      console.log("");
      console.log("✅ All services running and collecting data!");
    });
  } catch (error) {
    console.error("❌ Failed to start server:", error);
    process.exit(1);
  }
}

// Start the server
startServer();

module.exports = app;
