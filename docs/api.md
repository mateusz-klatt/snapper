# API

Snapper provides a REST API and WebSocket interface for platform interaction.
API requires JWT authentication via HTTP-only cookies.

## Authentication

Authentication uses HTTP-only cookies for security. After login, the server
sets `access_token`, `refresh_token`, and `csrf_token` cookies automatically.

### Login

```http
POST /api/auth/login
Content-Type: application/json

{
    "username": "admin",
    "password": "password123"
}
```

**Response:**

The server sets HTTP-only cookies and returns user info:

```json
{
    "user": {
        "id": 1,
        "username": "admin",
        "role": "admin",
        "is_active": true
    }
}
```

**Cookies set:**

| Cookie | HttpOnly | Secure | SameSite | Description |
| ------ | -------- | ------ | -------- | ----------- |
| `access_token` | Yes | Yes (prod) | Lax/Strict | JWT access token (15min) |
| `refresh_token` | Yes | Yes (prod) | Lax/Strict | JWT refresh token (7 days) |
| `csrf_token` | No | Yes (prod) | Lax/Strict | CSRF protection token |

### Refresh Token

```http
POST /api/auth/refresh
```

The `refresh_token` cookie is sent automatically by the browser. Server
returns new tokens in cookies.

### Logout

```http
POST /api/auth/logout
X-CSRF-Token: <csrf_token>
```

Clears all authentication cookies.

### CSRF Token

For mutating requests (POST, PUT, DELETE), a CSRF token is required:

```http
GET /api/auth/csrf
```

**Response:**

```json
{
    "csrf_token": "abc123..."
}
```

Then add the header to mutating requests:

```http
X-CSRF-Token: abc123...
```

## REST Endpoints

### Health

```http
GET /api/health
```

**Response:**

```json
{
    "status": "healthy",
    "timestamp": "2026-01-18T12:00:00Z",
    "version": "0.1.0",
    "connections": {
        "active": 5,
        "total": 100
    },
    "topics": {
        "available": 50,
        "active": 12
    }
}
```

### Candles (OHLCV)

```http
GET /api/candles?instrument=BTC-USD&timeframe=1h&limit=100
X-CSRF-Token: <csrf_token>
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `instrument` | string | yes | Instrument symbol |
| `timeframe` | string | yes | Timeframe (`1m`, `5m`, `15m`, `1h`, `4h`, `1d`) |
| `limit` | int | no | Number of candles (max 1000, default 100) |

**Response:**

```json
[
    {
        "instrument": "BTC-USD",
        "timeframe": "1h",
        "timestamp": "2026-01-18T11:00:00Z",
        "open": 42000.0,
        "high": 42500.0,
        "low": 41800.0,
        "close": 42300.0,
        "volume": 1234.56,
        "vwap": 42150.0,
        "trades": 5678
    }
]
```

### Orders

```http
GET /api/orders?symbol=BTC-USD&limit=100&offset=0
X-CSRF-Token: <csrf_token>
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `symbol` | string | no | Filter by symbol |
| `limit` | int | no | Number of orders (max 1000, default 100) |
| `offset` | int | no | Skip N orders (default 0) |

**Response:**

```json
[
    {
        "id": 1,
        "instrument": "BTC-USD",
        "exchange": "kraken",
        "client_order_id": "ord_123",
        "exchange_order_id": "KRAKEN-456",
        "created_at": "2026-01-18T12:00:00Z",
        "updated_at": "2026-01-18T12:01:00Z",
        "side": "buy",
        "type": "limit",
        "price": 42000.0,
        "size": 0.1,
        "status": "filled",
        "time_in_force": "GTC",
        "error": null
    }
]
```

### Signals

```http
GET /api/signals?instrument=BTC-USD&strategy=rsi&hours=24&limit=100
X-CSRF-Token: <csrf_token>
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `instrument` | string | no | Filter by instrument |
| `strategy` | string | no | Filter by strategy |
| `hours` | int | no | History hours (max 168, default 24) |
| `limit` | int | no | Number of signals (max 1000, default 100) |

**Response:**

```json
[
    {
        "id": 1,
        "instrument": "BTC-USD",
        "exchange": "paper",
        "timestamp": "2026-01-18T12:00:00Z",
        "side": "buy",
        "strength": 0.85,
        "reason": "RSI 28.5 <= 30",
        "strategy_name": "rsi_btc_1h",
        "price": 42000.0
    }
]
```

### Executions

```http
GET /api/executions?limit=100
X-CSRF-Token: <csrf_token>
```

**Response:**

```json
[
    {
        "id": 1,
        "order_id": 123,
        "timestamp": "2026-01-18T12:01:00Z",
        "price": 42000.0,
        "size": 0.1,
        "fee": 0.001,
        "fee_asset": "USD",
        "instrument": "BTC-USD",
        "side": "buy",
        "exchange": "kraken"
    }
]
```

### Positions

```http
GET /api/positions
X-CSRF-Token: <csrf_token>
```

**Response:**

```json
[
    {
        "id": 1,
        "instrument": "BTC-USD",
        "exchange": "kraken",
        "quantity": 0.5,
        "average_price": 41500.0,
        "unrealized_pnl": 250.0,
        "realized_pnl": 100.0,
        "updated_at": "2026-01-18T12:00:00Z"
    }
]
```

### WebSocket Stats

```http
GET /api/ws/stats
X-CSRF-Token: <csrf_token>
```

**Response:**

```json
{
    "websocket": {
        "active_connections": 5,
        "topic_subscribers": {
            "market.kraken.BTC-USD.candles.1h": 3,
            "signals.paper.BTC-USD": 2
        },
        "client_count": 5
    },
    "zmq_bridge": {
        "active_topics": 12,
        "subscriber_tasks": 12,
        "available_topics": ["market.kraken.BTC-USD.candles.1h", "..."]
    },
    "config": {
        "max_connections": 100,
        "heartbeat_interval": 30
    }
}
```

### ZMQ Health

```http
GET /api/zmq/health
X-CSRF-Token: <csrf_token>
```

**Response:**

```json
{
    "status": "healthy",
    "components": {
        "broker": "running",
        "bridge": "running"
    },
    "config": {
        "xsub_endpoint": "tcp://127.0.0.1:7500",
        "xpub_endpoint": "tcp://127.0.0.1:7501"
    }
}
```

## Process Management

### List Processes

```http
GET /api/processes
```

**Response:**

```json
[
    {
        "name": "zmq_broker",
        "description": "ZeroMQ XPUB/XSUB message broker",
        "status": "running",
        "role": "core",
        "priority": 10,
        "enabled": true,
        "pid": 12345,
        "started_at": "2026-01-18T10:00:00Z"
    },
    {
        "name": "strategy_rsi_btc_1h",
        "description": "RSI Reversion Strategy for BTC",
        "status": "stopped",
        "role": "strategy",
        "priority": 50,
        "enabled": false
    }
]
```

### Start Process

```http
POST /api/processes/{name}/start
X-CSRF-Token: <csrf_token>
```

### Stop Process

```http
POST /api/processes/{name}/stop
X-CSRF-Token: <csrf_token>
```

### System Status

```http
GET /api/system/status
```

**Response:**

```json
{
    "running_processes": 5,
    "total_processes": 10,
    "cpu_usage": 25.5,
    "memory_usage": 512000000,
    "uptime_seconds": 3600
}
```

## Settings API

### List Settings

```http
GET /api/settings
```

### Get Setting

```http
GET /api/settings/{key}
```

### Save Setting

```http
PUT /api/settings/{key}
X-CSRF-Token: <csrf_token>
Content-Type: application/json

{
    "value": "new_value",
    "encrypted": true
}
```

## WebSocket

### Connection

WebSocket endpoint requires a one-time token for authentication:

```
ws://localhost:8000/api/ws?token=<ws_token>
```

Get the WebSocket token from:

```http
GET /api/auth/ws-token
```

### Message Protocol

All messages are in JSON format.

#### Subscribe

```json
{
    "type": "subscribe",
    "topics": ["market.kraken.BTC-USD.candles.1h", "signals.paper.BTC-USD"]
}
```

#### Unsubscribe

```json
{
    "type": "unsubscribe",
    "topics": ["market.kraken.BTC-USD.candles.1h"]
}
```

#### Ping/Pong

```json
{
    "type": "ping"
}
```

Response:

```json
{
    "type": "pong",
    "timestamp": 1705579200000
}
```

### Server Messages

#### Market Data (Bar)

```json
{
    "type": "candle",
    "topic": "market.kraken.BTC-USD.candles.1h",
    "data": {
        "open": 42000.0,
        "high": 42500.0,
        "low": 41800.0,
        "close": 42300.0,
        "volume": 1234.56,
        "timestamp": 1705579200,
        "timeframe": "1h",
        "instrument": "BTC-USD"
    }
}
```

#### Tick

```json
{
    "type": "tick",
    "topic": "market.kraken.BTC-USD.ticks",
    "data": {
        "price": 42150.0,
        "volume": 0.5,
        "timestamp": 1705579200123,
        "instrument": "BTC-USD"
    }
}
```

#### Signal

```json
{
    "type": "signal",
    "topic": "signals.paper.BTC-USD.rsi_btc_1h",
    "data": {
        "instrument": "BTC-USD",
        "side": "buy",
        "strength": 0.85,
        "reason": "RSI 28.5 <= 30",
        "price": 42000.0,
        "timestamp": 1705579200,
        "metadata": {
            "rsi_value": 28.5,
            "period": 14
        }
    }
}
```

#### Trade Execution

```json
{
    "type": "trade",
    "topic": "fills.paper.BTC-USD",
    "data": {
        "order_id": "ord_123",
        "instrument": "BTC-USD",
        "side": "buy",
        "price": 42000.0,
        "size": 0.1,
        "fee": 0.001,
        "timestamp": 1705579200
    }
}
```

#### Heartbeat

```json
{
    "type": "heartbeat",
    "topic": "system.heartbeat",
    "data": {
        "component": "zmq_broker",
        "status": "healthy",
        "timestamp": 1705579200
    }
}
```

#### Error

```json
{
    "type": "error",
    "code": "INVALID_TOPIC",
    "message": "Topic 'invalid.topic' does not exist"
}
```

### Available Topics

#### Market Data

- `market.{exchange}.{instrument}.candles.{timeframe}` — OHLCV candles
- `market.{exchange}.{instrument}.ticks` — Price ticks

#### Signals

- `signals.paper.{instrument}.{strategy}` — Paper trading signals
- `signals.{exchange}.{instrument}.live` — Live signals

#### Fills

- `fills.{exchange}.{instrument}` — Order executions

#### System

- `system.heartbeat` — Component heartbeats
- `system.process.{name}` — Process status

## Error Codes

| Code | HTTP Status | Description |
| ---- | ----------- | ----------- |
| `UNAUTHORIZED` | 401 | Missing or invalid token |
| `FORBIDDEN` | 403 | Insufficient permissions |
| `CSRF_INVALID` | 403 | Invalid CSRF token |
| `NOT_FOUND` | 404 | Resource not found |
| `VALIDATION_ERROR` | 422 | Data validation error |
| `INTERNAL_ERROR` | 500 | Server error |

## Usage Example (Python)

```python
import httpx

BASE_URL = "http://localhost:8000/api"

async def main():
    async with httpx.AsyncClient() as client:
        # Login - cookies are set automatically
        response = await client.post(
            f"{BASE_URL}/auth/login",
            json={"username": "admin", "password": "password123"}
        )
        # Cookies are stored in client automatically

        # Get CSRF token from cookie
        csrf_token = client.cookies.get("csrf_token")

        # Fetch candles - cookies sent automatically
        response = await client.get(
            f"{BASE_URL}/candles",
            params={"instrument": "BTC-USD", "timeframe": "1h"},
            headers={"X-CSRF-Token": csrf_token}
        )
        candles = response.json()
        print(candles)
```

## WebSocket Example (JavaScript)

```javascript
// Get WS token first (requires authenticated session)
const tokenResponse = await fetch('/api/auth/ws-token', {
    credentials: 'include'  // Include cookies
});
const { token } = await tokenResponse.json();

const ws = new WebSocket(`ws://localhost:8000/api/ws?token=${token}`);

ws.onopen = () => {
    // Subscribe to topics
    ws.send(JSON.stringify({
        type: 'subscribe',
        topics: ['market.kraken.BTC-USD.candles.1h', 'signals.paper.BTC-USD']
    }));
};

ws.onmessage = (event) => {
    const message = JSON.parse(event.data);
    console.log('Received:', message.type, message.data);
};

// Heartbeat
setInterval(() => {
    ws.send(JSON.stringify({ type: 'ping' }));
}, 30000);
```
