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
        "id": "admin",
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

REST endpoints return the same Data schemas used by WebSocket envelopes
(`OrderData`, `SignalData`, `ExecutionData`, `PositionData`, `CandleData`
from `messaging.schemas.data`). This means the wire format is identical
whether data arrives via REST or the WebSocket feed.

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
        "active_connections": 5,
        "zmq_subscribers": 12,
        "subscriber_tasks": 12,
        "active_topics": 8,
        "active_clients": 3
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
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |

**Response:**

```json
[
    {
        "public_id": "019e1a2b-...",
        "type": "candle",
        "timestamp": "2026-01-18T12:00:00Z",
        "instrument": "BTC-USD",
        "exchange": "kraken",
        "timeframe": "1h",
        "open_at": "2026-01-18T11:00:00Z",
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
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |

**Response:**

```json
[
    {
        "public_id": "019e1a2b-...",
        "type": "order",
        "timestamp": "2026-01-18T12:00:00Z",
        "instrument": "BTC-USD",
        "exchange": "kraken",
        "client_order_id": "ord_123",
        "exchange_order_id": "KRAKEN-456",
        "created_at": "2026-01-18T12:00:00Z",
        "updated_at": "2026-01-18T12:01:00Z",
        "side": "buy",
        "order_type": "limit",
        "price": 42000.0,
        "size": 0.1,
        "filled_size": 0.1,
        "average_price": 42000.0,
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
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |

**Response:**

```json
[
    {
        "public_id": "019e1a2b-...",
        "type": "signal",
        "timestamp": "2026-01-18T12:00:00Z",
        "fired_at": "2026-01-18T12:00:00Z",
        "instrument": "BTC-USD",
        "exchange": "paper",
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

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `limit` | int | no | Number of executions (max 1000, default 100) |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |

**Response:**

```json
[
    {
        "public_id": "019e1a2b-...",
        "type": "execution",
        "timestamp": "2026-01-18T12:01:00Z",
        "trade_id": "TTRAD-456",
        "exchange_order_id": "KRAKEN-456",
        "client_order_id": "signal-a1b2c3d4",
        "instrument": "BTC-USD",
        "exchange": "kraken",
        "side": "buy",
        "size": 0.1,
        "price": 42000.0,
        "fee": 0.001,
        "fee_asset": "USD",
        "status": "filled",
        "executed_at": "2026-01-18T12:00:59Z"
    }
]
```

### Positions

```http
GET /api/positions
X-CSRF-Token: <csrf_token>
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |

**Response:**

```json
[
    {
        "public_id": "019e1a2b-...",
        "type": "position",
        "timestamp": "2026-01-18T12:00:00Z",
        "instrument": "BTC-USD",
        "exchange": "kraken",
        "quantity": 0.5,
        "average_price": 41500.0,
        "unrealized_pnl": 250.0,
        "realized_pnl": 100.0
    }
]
```

### Exchanges

```http
GET /api/exchanges
X-CSRF-Token: <csrf_token>
```

Returns distinct exchange names from active symbol aliases.

**Response:**

```json
["kraken", "polygon", "walutomat", "zonda"]
```

### Exchange Instruments

```http
GET /api/exchanges/{exchange}/instruments
X-CSRF-Token: <csrf_token>
```

Returns distinct native symbols available on the given exchange.

**Response:**

```json
["BTC-USD", "ETH-USD", "SOL-USD"]
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

#### Candle (OHLCV)

Messages are flat envelopes (no `"data"` wrapper). ZMQ payloads are
forwarded directly to WebSocket clients.

```json
{
    "public_id": "019e1a2b-...",
    "type": "candle",
    "timestamp": "2026-01-18T11:00:00Z",
    "instrument": "BTC-USD",
    "exchange": "kraken",
    "timeframe": "1h",
    "open_at": "2026-01-18T11:00:00Z",
    "open": 42000.0,
    "high": 42500.0,
    "low": 41800.0,
    "close": 42300.0,
    "volume": 1234.56,
    "vwap": 42150.0,
    "trades": 5678
}
```

#### Tick

```json
{
    "public_id": "019e1a2b-...",
    "type": "tick",
    "timestamp": "2026-01-18T12:00:00Z",
    "instrument": "BTC-USD",
    "exchange": "kraken",
    "volume": 1234.5,
    "bid": 42000.0,
    "ask": 42001.0,
    "last": 42000.5
}
```

#### Signal

```json
{
    "public_id": "019e1a2b-...",
    "type": "signal",
    "timestamp": "2026-01-18T12:00:00Z",
    "fired_at": "2026-01-18T12:00:00Z",
    "instrument": "BTC-USD",
    "exchange": "paper",
    "side": "buy",
    "strength": 0.85,
    "reason": "RSI 28.5 <= 30",
    "price": 42000.0,
    "strategy_name": "rsi_btc_1h"
}
```

#### Execution

```json
{
    "public_id": "019e1a2b-...",
    "type": "execution",
    "timestamp": "2026-01-18T12:01:00Z",
    "client_order_id": "signal-a1b2c3d4",
    "exchange_order_id": "KRAKEN-456",
    "trade_id": "TTRAD-789",
    "instrument": "BTC-USD",
    "exchange": "kraken",
    "side": "buy",
    "size": 0.1,
    "price": 42000.0,
    "fee": 0.001,
    "fee_asset": "USD",
    "status": "filled",
    "executed_at": "2026-01-18T12:00:59Z"
}
```

#### Order

```json
{
    "public_id": "019e1a2b-...",
    "type": "order",
    "timestamp": "2026-01-18T12:00:00Z",
    "exchange_order_id": "KRAKEN-456",
    "client_order_id": "signal-a1b2c3d4",
    "instrument": "BTC-USD",
    "exchange": "kraken",
    "side": "buy",
    "status": "submitted",
    "order_type": "limit",
    "size": 0.1,
    "filled_size": 0.0,
    "price": 42000.0,
    "average_price": null,
    "created_at": "2026-01-18T12:00:00Z"
}
```

#### Heartbeat

```json
{
    "public_id": "019e1a2b-...",
    "type": "heartbeat",
    "timestamp": "2026-01-18T12:00:00Z",
    "component": "zmq_broker",
    "sequence": 42,
    "status": "healthy",
    "lag_ms": 5
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

#### Order Events

- `orders.events.{exchange}.{instrument}.submitted` — Order submitted
- `orders.events.{exchange}.{instrument}.accepted` — Order accepted
- `orders.events.{exchange}.{instrument}.rejected` — Order rejected
- `orders.events.{exchange}.{instrument}.executed` — Order executed (fill)
- `orders.events.{exchange}.{instrument}.cancelled` — Order cancelled
- `orders.events.{exchange}.{instrument}.expired` — Order expired
- `orders.events.{exchange}.{instrument}.replaced` — Order replaced

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
    console.log('Received:', message.type, message);
};

// Heartbeat
setInterval(() => {
    ws.send(JSON.stringify({ type: 'ping' }));
}, 30000);
```
