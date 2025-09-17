# Black Swan Data Collection Service

[![Node.js](https://img.shields.io/badge/Node.js-18.0.0+-green.svg)](https://nodejs.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Author](https://img.shields.io/badge/Author-Muhammad%20Bilal%20Motiwala-orange.svg)](https://github.com/bilalmotiwala)

A high-frequency financial data collection service designed to monitor and collect real-time market data for Black Swan.

**Author:** Muhammad Bilal Motiwala  
**Project:** Black Swan  
**Version:** 1.0.0

## 🎯 Overview

The Black Swan Data Collection Service is a robust, production-ready Node.js service that continuously collects financial market data from multiple sources and stores it in Redis for real-time access. It's specifically designed for monitoring market conditions that could indicate black swan events - rare, high-impact market disruptions.

### Key Features

- **Real-time Data Collection**: High-frequency data collection from multiple financial APIs
- **Multi-Source Integration**: Aggregates data from 6+ different financial data providers
- **Redis Caching**: High-performance in-memory storage for real-time data access
- **Firestore Backup**: Automated snapshots for data recovery and historical analysis
- **Live Log Streaming**: Real-time log monitoring via Server-Sent Events (SSE)
- **Rate Limiting**: Intelligent API rate limiting to respect provider limits
- **Graceful Error Handling**: Robust error handling with automatic retries
- **Health Monitoring**: Comprehensive health checks and status endpoints

## 📊 Data Sources

| Data Type                  | Source                           | Collection Interval     | API Provider                    |
| -------------------------- | -------------------------------- | ----------------------- | ------------------------------- |
| **Bitcoin Price**          | CoinGecko API                    | Every 60 seconds        | CoinGecko                       |
| **Ethereum Price**         | CoinGecko API                    | Every 60 seconds        | CoinGecko                       |
| **Solana Price**           | CoinGecko API                    | Every 60 seconds        | CoinGecko                       |
| **S&P 500**                | Alpha Vantage + Yahoo Finance    | Every 60 seconds (24/7) | Alpha Vantage, Yahoo Finance    |
| **Fear & Greed Index**     | Alternative.me + VIX             | Every 60 seconds        | Alternative.me, Alpha Vantage   |
| **Currency Rates**         | Alpha Vantage + ExchangeRate-API | Every 60 seconds        | Alpha Vantage, ExchangeRate-API |
| **Bull Market Indicators** | CoinGlass API                    | Every 15 minutes        | CoinGlass                       |
| **Daily OHLCV**            | CoinGlass API                    | Once daily (02:10 UTC)  | CoinGlass                       |

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   External      │    │   Data          │    │   Storage       │
│   APIs          │───▶│   Collection    │───▶│   Layer         │
│                 │    │   Service       │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   Redis Cache   │    │   Firestore     │
                       │   (Real-time)   │    │   (Backup)      │
                       └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   API Endpoints │
                       │   (REST + SSE)  │
                       └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- **Node.js** >= 18.0.0
- **Redis** server (local or cloud)
- **Firebase** project with Firestore enabled
- **API Keys** for external services (optional but recommended)

### Installation

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd data-collection-service
   ```

2. **Install dependencies**

   ```bash
   npm install
   ```

3. **Environment Setup**

   ```bash
   cp .env.example .env
   ```

4. **Configure environment variables**

   ```env
   # Redis Configuration
   REDIS_HOST_URL=localhost
   REDIS_PORT=6379
   REDIS_PASSWORD=your-redis-password

   # API Keys (Optional - service works without them but with limited data)
   ALPHA_VANTAGE_API_KEY=your-alpha-vantage-api-key
   COINGLASS_API_KEY=your-coinglass-api-key

   # Server Configuration
   PORT=8083
   ```

5. **Start the service**

   ```bash
   # Development mode
   npm run dev

   # Production mode
   npm start
   ```

## 🔧 Configuration

### Environment Variables

| Variable                | Required | Default | Description                                  |
| ----------------------- | -------- | ------- | -------------------------------------------- |
| `REDIS_HOST_URL`        | Yes      | -       | Redis server hostname or IP                  |
| `REDIS_PORT`            | Yes      | -       | Redis server port                            |
| `REDIS_PASSWORD`        | Yes      | -       | Redis server password                        |
| `ALPHA_VANTAGE_API_KEY` | No       | -       | Alpha Vantage API key for enhanced data      |
| `COINGLASS_API_KEY`     | No       | -       | CoinGlass API key for bull market indicators |
| `PORT`                  | No       | 8083    | Server port                                  |

### Data Retention

The service automatically manages data retention to prevent memory bloat:

- **Minute Data**: 7 days (10,080 data points)
- **Bull Market Indicators**: 7 days (672 data points)
- **Daily OHLCV**: ~3 years (1,100 data points)

## 📡 API Endpoints

### Health & Status

#### `GET /health`

Comprehensive health check endpoint.

**Response:**

```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "redis": {
    "connected": true,
    "ping": true,
    "status": "ready"
  },
  "firebase": true,
  "services": {
    "cryptocurrencies": true,
    "sp500": true,
    "fear_greed": true,
    "currency": true,
    "bull_peak": true,
    "snapshots": true
  },
  "last_updates": {
    "bitcoin": "2024-01-15T10:29:00.000Z",
    "ethereum": "2024-01-15T10:29:00.000Z",
    "solana": "2024-01-15T10:29:00.000Z",
    "sp500": "2024-01-15T10:29:00.000Z",
    "fear_greed": "2024-01-15T10:29:00.000Z",
    "currency": "2024-01-15T10:29:00.000Z",
    "bull_peak": "2024-01-15T10:15:00.000Z"
  },
  "data_counts": {
    "bitcoin": 10080,
    "ethereum": 10080,
    "solana": 10080,
    "sp500": 10080,
    "fear_greed": 10080,
    "currency": 10080,
    "bull_peak": 672
  }
}
```

### Data Endpoints

#### `GET /bitcoin`

Get Bitcoin price data.

**Query Parameters:**

- `hours` (optional): Filter data to last N hours
- `detailed` (optional): Include detailed gap analysis

**Response:**

```json
{
  "data": [
    {
      "timestamp": 1705315200000,
      "price": 42500.5,
      "collected_at": "2024-01-15T10:30:00.000Z",
      "source": "real_time"
    }
  ],
  "count": 10080,
  "last_update": "2024-01-15T10:30:00.000Z",
  "retention_days": 7,
  "data_quality": {
    "total_minutes_expected_7d": 10080,
    "actual_data_points": 10080,
    "coverage_percentage": "100.00",
    "recent_hour_points": 60,
    "recent_day_points": 1440,
    "minute_gaps_count": 0,
    "data_completeness": "excellent"
  }
}
```

#### `GET /ethereum`

Get Ethereum price data (same format as Bitcoin).

#### `GET /solana`

Get Solana price data (same format as Bitcoin).

#### `GET /sp500`

Get S&P 500 data.

**Response:**

```json
{
  "data": [
    {
      "timestamp": 1705315200000,
      "price": 4785.25,
      "open": 4780.1,
      "high": 4790.5,
      "low": 4775.3,
      "volume": 2500000,
      "change": 5.15,
      "change_percent": "0.11%",
      "source": "alpha_vantage",
      "market_hours": true,
      "collected_at": "2024-01-15T10:30:00.000Z"
    }
  ],
  "count": 10080,
  "last_update": "2024-01-15T10:30:00.000Z",
  "retention_days": 7,
  "current_market_status": "OPEN",
  "collection_mode": "24/7 (tracks latest available price)"
}
```

#### `GET /fear-greed`

Get Fear & Greed Index data.

**Response:**

```json
{
  "data": [
    {
      "timestamp": 1705315200000,
      "crypto_fear_greed": {
        "value": 65,
        "classification": "Greed",
        "source": "alternative_me"
      },
      "traditional_fear_greed": {
        "value": 45,
        "vix_value": 18.5,
        "classification": "Neutral",
        "source": "vix_derived"
      },
      "collected_at": "2024-01-15T10:30:00.000Z"
    }
  ],
  "count": 10080,
  "last_update": "2024-01-15T10:30:00.000Z",
  "retention_days": 7,
  "collection_interval": "60 seconds"
}
```

#### `GET /currency`

Get currency exchange rate data.

**Query Parameters:**

- `hours` (optional): Filter data to last N hours
- `pair` (optional): Filter to specific currency pair (e.g., "gbp", "eur")

**Response:**

```json
{
  "data": [
    {
      "timestamp": 1705315200000,
      "rates": {
        "USDGBP": {
          "rate": 0.7892,
          "source": "alpha_vantage"
        },
        "USDEUR": {
          "rate": 0.9125,
          "source": "alpha_vantage"
        },
        "USDJPY": {
          "rate": 148.25,
          "source": "alpha_vantage"
        },
        "USDCHF": {
          "rate": 0.8756,
          "source": "alpha_vantage"
        }
      },
      "collected_at": "2024-01-15T10:30:00.000Z"
    }
  ],
  "count": 10080,
  "last_update": "2024-01-15T10:30:00.000Z",
  "retention_days": 7,
  "collection_interval": "60 seconds",
  "available_pairs": ["USDGBP", "USDEUR", "USDJPY", "USDCHF"]
}
```

#### `GET /bull-peak`

Get latest Bull Market Peak Indicators.

**Response:**

```json
{
  "data": {
    "timestamp": 1705315200000,
    "collected_at": "2024-01-15T10:30:00.000Z",
    "source": "coinglass",
    "indicators": [
      {
        "indicator_name": "Bitcoin Dominance",
        "current_value": 42.5,
        "target_value": 50.0,
        "previous_value": 42.3,
        "change_value": 0.2,
        "comparison_type": "percentage",
        "hit_status": false
      }
    ]
  },
  "last_update": "2024-01-15T10:30:00.000Z"
}
```

#### `GET /bitcoin/daily`

Get Bitcoin daily OHLCV data.

**Query Parameters:**

- `days` (optional): Number of days to retrieve (default: all available)

**Response:**

```json
{
  "data": [
    {
      "timestamp": 1705190400000,
      "open": 42000.0,
      "high": 43500.0,
      "low": 41800.0,
      "close": 42500.5,
      "volume": 25000000000,
      "source": "coinglass",
      "interval": "1d"
    }
  ],
  "count": 1100,
  "last_update": "2024-01-15T02:10:00.000Z",
  "source": "coinglass",
  "interval": "1d"
}
```

#### `GET /ethereum/daily`

Get Ethereum daily OHLCV data (same format as Bitcoin).

#### `GET /solana/daily`

Get Solana daily OHLCV data (same format as Bitcoin).

### Dashboard & Analytics

#### `GET /dashboard-stats`

Get current market statistics for dashboard display.

**Response:**

```json
{
  "stats": {
    "bitcoin": {
      "current": "42500.50",
      "currentRaw": 42500.5,
      "previous": "42450.25",
      "previousRaw": 42450.25,
      "change": 50.25,
      "changePercent": 0.12,
      "changeType": "up",
      "lastUpdated": "2024-01-15T10:30:00.000Z",
      "dataAge": 0
    },
    "crypto_fear_greed": {
      "current": "65",
      "currentRaw": 65,
      "previous": "62",
      "previousRaw": 62,
      "change": 3,
      "changePercent": 4.84,
      "changeType": "up",
      "classification": "Greed",
      "lastUpdated": "2024-01-15T10:30:00.000Z",
      "dataAge": 0
    }
  },
  "metadata": {
    "timestamp": "2024-01-15T10:30:00.000Z",
    "comparison_period": "5_minutes",
    "last_updates": {
      "bitcoin": "2024-01-15T10:30:00.000Z",
      "ethereum": "2024-01-15T10:30:00.000Z"
    },
    "available_data_sources": [
      "bitcoin",
      "ethereum",
      "solana",
      "sp500",
      "crypto_fear_greed"
    ]
  }
}
```

#### `GET /all`

Get all data types in a single request.

**Query Parameters:**

- `hours` (optional): Filter all data to last N hours

### Logging & Monitoring

#### `GET /logs/stream`

Server-Sent Events endpoint for real-time log streaming.

**Usage:**

```javascript
const eventSource = new EventSource("/logs/stream");
eventSource.onmessage = function (event) {
  const logEntry = JSON.parse(event.data);
  console.log(logEntry);
};
```

#### `GET /publicStream`

Public log stream for sanitized information.

#### `GET /logs/poll`

Polling endpoint for log retrieval.

**Query Parameters:**

- `limit` (optional): Number of logs to retrieve (max 100, default 20)
- `since` (optional): ISO timestamp to filter logs after

### Data Management

#### `POST /restore-snapshot`

Restore data from Firestore snapshot.

**Response:**

```json
{
  "success": true,
  "message": "Data restored from snapshot",
  "snapshot_date": "2024-01-15T10:00:00.000Z",
  "restored_counts": {
    "bitcoin": 720,
    "ethereum": 720,
    "solana": 720
  }
}
```

## 🔄 Data Collection Process

### Collection Flow

1. **Rate Limiting**: Each API request is rate-limited according to provider limits
2. **Data Fetching**: Data is fetched from external APIs with timeout handling
3. **Data Processing**: Raw data is normalized and validated
4. **Storage**: Data is stored in Redis with automatic retention management
5. **Backup**: Periodic snapshots are created in Firestore
6. **Monitoring**: All operations are logged and streamed to connected clients

### Error Handling

- **API Failures**: Automatic retries with exponential backoff
- **Rate Limiting**: Intelligent queuing and waiting
- **Data Validation**: Invalid data is filtered out
- **Connection Issues**: Automatic reconnection to Redis and external APIs
- **Graceful Degradation**: Service continues with available data sources

## 🛠️ Development

### Project Structure

```
data-collection-service/
├── index.js                 # Main application file
├── package.json            # Dependencies and scripts
├── serviceAccountKey.json  # Firebase service account (keep secure!)
├── .env.example           # Environment variables template
├── .gitignore            # Git ignore rules
└── README.md             # This file
```

### Key Classes

- **`LogStreamer`**: Real-time log streaming via SSE
- **`PublicLogStreamer`**: Sanitized public log streaming
- **`APIRateLimiter`**: Rate limiting for external APIs
- **`CryptocurrencyDataCollector`**: Bitcoin, Ethereum, Solana data collection
- **`SP500DataCollector`**: S&P 500 data collection
- **`FearGreedDataCollector`**: Fear & Greed Index collection
- **`CurrencyDataCollector`**: Currency exchange rate collection
- **`BullMarketPeakIndicatorCollector`**: Bull market indicators
- **`DailyOHLCVCollector`**: Daily OHLCV data collection
- **`SnapshotService`**: Firestore backup service

### Adding New Data Sources

1. **Create Collector Class**:

   ```javascript
   class NewDataCollector {
     constructor(publicLogStreamer = null) {
       this.isRunning = false;
       this.collectionInterval = 60000; // 1 minute
       this.publicLogStreamer = publicLogStreamer;
     }

     async start() {
       // Implementation
     }

     async collectData() {
       // Implementation
     }

     stop() {
       this.isRunning = false;
     }
   }
   ```

2. **Add API Configuration**:

   ```javascript
   const APIS = {
     NEW_API: {
       BASE_URL: "https://api.example.com",
       API_KEY: process.env.NEW_API_KEY,
       RATE_LIMIT: 60,
       TIMEOUT: 10000,
     },
   };
   ```

3. **Initialize and Start**:
   ```javascript
   const newCollector = new NewDataCollector(publicLogStreamer);
   newCollector.start();
   ```

### Testing

```bash
# Test health endpoint
curl http://localhost:8083/health

# Test data endpoints
curl http://localhost:8083/bitcoin
curl http://localhost:8083/dashboard-stats

# Test log streaming (in browser)
# Open: http://localhost:8083/logs/stream
```

## 🚀 Deployment

### Docker Deployment

```dockerfile
FROM node:18-alpine

WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production

COPY . .

EXPOSE 8083
CMD ["npm", "start"]
```

```bash
# Build and run
docker build -t blackswan-data-service .
docker run -p 8083:8083 --env-file .env blackswan-data-service
```

### Environment-Specific Configuration

#### Development

```env
REDIS_HOST_URL=localhost
REDIS_PORT=6379
REDIS_PASSWORD=dev-password
PORT=8083
```

#### Production

```env
REDIS_HOST_URL=your-redis-cluster.cache.amazonaws.com
REDIS_PORT=6379
REDIS_PASSWORD=your-secure-password
ALPHA_VANTAGE_API_KEY=your-production-key
COINGLASS_API_KEY=your-production-key
PORT=8083
```

### Monitoring & Alerting

#### Health Checks

- **Endpoint**: `GET /health`
- **Frequency**: Every 30 seconds
- **Alerts**: Redis connection, API failures, data staleness

#### Log Monitoring

- **Real-time**: SSE endpoint `/logs/stream`
- **Historical**: Polling endpoint `/logs/poll`
- **Levels**: ERROR, WARN, INFO, DEBUG

#### Metrics to Monitor

- **Data Freshness**: Time since last successful collection
- **API Success Rate**: Percentage of successful API calls
- **Redis Memory Usage**: Monitor for memory leaks
- **Error Rates**: Track and alert on error spikes

## 🔒 Security

### Best Practices

1. **Environment Variables**: All sensitive data in environment variables
2. **API Keys**: Store in secure environment variables, never in code
3. **Firebase Key**: Keep `serviceAccountKey.json` secure and never commit
4. **Rate Limiting**: Built-in rate limiting to prevent abuse
5. **CORS**: Properly configured CORS headers
6. **Helmet**: Security headers via Helmet.js

### Security Checklist

- [ ] All API keys in environment variables
- [ ] `serviceAccountKey.json` not committed to version control
- [ ] Redis password configured
- [ ] CORS properly configured for production
- [ ] Rate limiting enabled
- [ ] Security headers via Helmet
- [ ] Regular security updates for dependencies

## 📈 Performance

### Optimization Features

- **Redis Caching**: High-performance in-memory storage
- **Connection Pooling**: Efficient database connections
- **Auto-pipelining**: Redis command batching
- **Compression**: Response compression via gzip
- **Rate Limiting**: Prevents API abuse and ensures compliance

### Performance Metrics

- **Response Time**: < 100ms for cached data
- **Throughput**: 1000+ requests per second
- **Memory Usage**: ~50MB base + data storage
- **CPU Usage**: Low during normal operation
- **Network**: Optimized for minimal bandwidth usage

## 🐛 Troubleshooting

### Common Issues

#### Redis Connection Issues

```bash
# Check Redis connection
redis-cli -h your-redis-host -p 6379 -a your-password ping

# Check Redis logs
tail -f /var/log/redis/redis-server.log
```

#### API Rate Limiting

- **Symptom**: "Rate limit reached" messages
- **Solution**: Wait for rate limit reset or upgrade API plan
- **Prevention**: Monitor rate limit usage in logs

#### Data Not Updating

- **Check**: `/health` endpoint for service status
- **Check**: Logs for API errors
- **Check**: Network connectivity to external APIs

#### Memory Issues

- **Symptom**: High memory usage
- **Solution**: Check data retention settings
- **Monitor**: Redis memory usage

### Debug Mode

```bash
# Enable debug logging
DEBUG=* npm start

# Check specific service
DEBUG=blackswan:* npm start
```

### Log Analysis

```bash
# Filter error logs
curl "http://localhost:8083/logs/poll?limit=100" | jq '.logs[] | select(.level == "error")'

# Check recent activity
curl "http://localhost:8083/logs/poll?since=2024-01-15T10:00:00.000Z"
```

## 📚 API Documentation

### Rate Limits

- **API Endpoints**: 100 requests per minute per IP
- **Log Streaming**: No limit (SSE connections)
- **Health Checks**: No limit

### Error Codes

| Code | Description           |
| ---- | --------------------- |
| 200  | Success               |
| 404  | Data not found        |
| 429  | Rate limit exceeded   |
| 500  | Internal server error |

### Data Formats

#### Timestamp Format

- **Format**: Unix timestamp in milliseconds
- **Example**: `1705315200000`
- **Conversion**: `new Date(1705315200000).toISOString()`

#### Price Format

- **Type**: Number (float)
- **Precision**: 2 decimal places for prices, 4 for exchange rates
- **Currency**: USD for all price data

## 🤝 Contributing

### Development Setup

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

### Code Style

- **ESLint**: Follow project ESLint configuration
- **Comments**: Add JSDoc comments for new functions
- **Error Handling**: Always handle errors gracefully
- **Logging**: Use appropriate log levels

### Testing

```bash
# Run tests (when available)
npm test

# Lint code
npm run lint

# Check formatting
npm run format
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Muhammad Bilal Motiwala**

- GitHub: [@bilalmotiwala](https://github.com/bilalmotiwala)
- Email: [bilal@oaiaolabs.com](mailto:bilal@oaiaolabs.com)

## 🆘 Support

### Getting Help

1. **Check Documentation**: Review this README and code comments
2. **Check Logs**: Use `/logs/stream` or `/logs/poll` endpoints
3. **Health Check**: Use `/health` endpoint for service status
4. **GitHub Issues**: Create an issue for bugs or feature requests

---

**⚠️ Important Notes:**

- Keep `serviceAccountKey.json` secure and never commit it to version control
- Monitor API rate limits to avoid service interruptions
- Regularly update dependencies for security patches
- Test thoroughly before deploying to production
- Monitor Redis memory usage and data retention settings

**🚀 Happy Data Collecting!**
