# API

Snapper provides a REST API and WebSocket interface for platform interaction.
The API requires JWT authentication via HTTP-only cookies. Auth bootstrap
endpoints under `/api/auth` are exempt, but other mutating requests
(`POST`) require a valid `X-CSRF-Token` header.

## Authentication

Authentication uses HTTP-only cookies. After login, the server sets
`access_token`, `refresh_token`, and `csrf_token` cookies automatically.

### Roles and Permissions

| Role | Access |
| ---- | ------ |
| `viewer` | Read-only market data, orders, positions, strategies, system status |
| `operator` | Viewer permissions plus trade execution, process management |
| `admin` | Full access including user management and system configuration |

### POST /api/auth/login

Authenticate and create a session. Sets `access_token`, `refresh_token`,
and `csrf_token` cookies on the response.

**Request:**

```http
POST /api/auth/login
Content-Type: application/json

{
    "username": "admin",
    "password": "password123",
    "remember_me": false
}
```

**Response (200):**

```json
{
    "message": "Login successful",
    "expires_in": 900,
    "user": {
        "username": "admin",
        "email": "admin@example.com",
        "role": "admin",
        "is_active": true,
        "created_at": "2026-01-10T08:00:00Z"
    }
}
```

**Cookies set:**

| Cookie | HttpOnly | Secure | SameSite | Path | Max-Age | Description |
| ------ | -------- | ------ | -------- | ---- | ------- | ----------- |
| `access_token` | Yes | Yes (prod) | Lax/Strict | `/` | session | JWT access token (15 min) |
| `refresh_token` | Yes | Yes (prod) | Lax/Strict | `/api/auth` | 7 days | JWT refresh token |
| `csrf_token` | No | Yes (prod) | Lax/Strict | `/` | session | CSRF protection token |

### POST /api/auth/refresh

Refresh session tokens. The `refresh_token` cookie is sent automatically by
the browser. Returns new tokens in cookies plus a WebSocket authentication
token in the response body. This is the only way to obtain a `ws_token`.

**Request:**

```http
POST /api/auth/refresh
```

**Response (200):**

```json
{
    "message": "session refreshed",
    "ws_token": "eyJhbGciOi...",
    "ws_token_exp": "2026-01-18T12:30:00Z",
    "csrf_token": "abc123def456...",
    "user": {
        "username": "admin",
        "email": "admin@example.com",
        "role": "admin",
        "is_active": true,
        "created_at": "2026-01-10T08:00:00Z"
    }
}
```

### POST /api/auth/logout

Logout and invalidate the current session. Clears all authentication cookies
and blacklists tokens.

**Request:**

```http
POST /api/auth/logout
```

**Response (200):**

```json
{
    "message": "Logged out successfully"
}
```

### GET /api/auth/me

Get the currently authenticated user's profile.

**Request:**

```http
GET /api/auth/me
```

**Response (200):**

```json
{
    "username": "admin",
    "email": "admin@example.com",
    "role": "admin",
    "is_active": true,
    "created_at": "2026-01-10T08:00:00Z"
}
```

### CSRF Protection

The `csrf_token` cookie is readable by JavaScript (not HttpOnly). For
authenticated mutating requests outside `/api/auth/login`,
`/api/auth/refresh`, and `/api/auth/logout`, include its value as a header:

```http
X-CSRF-Token: <value from csrf_token cookie>
```

New CSRF tokens are issued on login and refresh. There is no separate
endpoint for obtaining CSRF tokens.

## REST Endpoints

REST endpoints return the same Data schemas used by WebSocket messages
(`OrderData`, `SignalData`, `ExecutionData`, `PositionData`, `CandleData`
from `messaging.schemas.data`). This means the wire format is identical
whether data arrives via REST or the WebSocket feed.

### REST = Bulk WS

REST responses are JSON arrays of payload items. Each item carries its own
per-item provenance (`public_id`, `session_id`, `sequence_id`) — there is
no wrapper-level `public_id` and no custom HTTP headers for provenance.
The shape of each array element is identical to what the WebSocket feed
delivers for the same data type.

### Provenance on Reads vs. Mutations

- **GET reads** — No client provenance is expected. The server records the
  request in the `telemetry` table for observability but does not stamp
  provenance onto the response items beyond what was stored at write time.

- **Mutations (POST)** — All mutations use command-style POST with a
  `PayloadRequest` envelope carrying provenance (`public_id`, `session_id`,
  `sequence_id`, `timestamp`) and domain intent in `payload`. The
  server-side `ClientProvenanceMiddleware` extracts these fields, emits a
  structured info log, and runs a per-session `GapDetector` to warn on
  sequence gaps. Gaps are logged as warnings but never reject requests.

### ClientProvenanceMiddleware

`ClientProvenanceMiddleware` is an ASGI middleware that processes every
mutation request:

1. Intercepts the request body transparently (replays chunks to
   downstream handlers).
2. Extracts `session_id`, `sequence_id`, and `public_id` from the JSON
   body when present.
3. Emits a structured log with `client_session_id`, `client_sequence_id`,
   `client_public_id`, and the request path.
4. Runs a per-session `GapDetector` to detect sequence gaps from the
   client.
5. Records the mutation in the `control` table via a `finally` block,
   so the row is persisted whether the request succeeded or failed.

The control recording is non-blocking: any DB failure is logged and
swallowed so the response already sent to the client is never invalidated.
The control row includes the redacted request payload, HTTP method, path,
outcome (`ok`, `error`, or `exception`), and server-side provenance
(`session_id` and `sequence_id` from the middleware's own
`SequenceTracker`).

All data endpoints support bitemporal querying via the optional `as_of`
parameter (UTC datetime). When provided, the query returns data as it was
known at that point in time. When omitted, the current time is used.

### GET /api/health

Public health check endpoint. No authentication required.

`status` reflects the health of enabled long-running CORE processes:
`"healthy"` when all are running, `"error"` if any are missing. In
`SERVER_API_ONLY` mode the status is always `"healthy"` (no processes
are started by design).

**Response (200):**

```json
{
    "type": "health_check",
    "status": "healthy",
    "timestamp": "2026-01-18T12:00:00Z",
    "version": "0.1.0",
    "connections": {
        "type": "connection_stats",
        "active_connections": 5,
        "zmq_subscribers": 12,
        "subscriber_tasks": 12,
        "active_topics": 8,
        "active_clients": 3
    },
    "topics": {
        "type": "health_topics",
        "available": 7,
        "active": 3
    },
    "gap_detection": {
        "type": "gap_detection_stats",
        "bridge": {
            "type": "gap_stats",
            "gaps_detected": 0,
            "session_resets": 0,
            "duplicates": 0,
            "mid_stream_joins": 0,
            "rejected_unstamped": 0
        },
        "rest_clients": {}
    }
}
```

The `gap_detection` field provides observability into sequence gap detection
across the ZMQ bridge and per-session REST client detectors.

### GET /api/candles

Fetch OHLCV candlestick data. Requires `read:market_data` permission.

**Request:**

```http
GET /api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1h&limit=100
X-CSRF-Token: <csrf_token>
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `instrument` | string | yes | Instrument symbol (e.g., `BTC-USD`) |
| `exchange` | string | yes | Exchange name (`kraken`, `zonda`, `walutomat`, `polygon`) |
| `timeframe` | string | yes | Candle timeframe (e.g., `1m`, `5m`, `15m`, `1h`, `4h`, `1d`) |
| `limit` | int | no | Number of candles, max 1000 (default 100) |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |

Returns 204 No Content if the instrument is not found.

**Response (200):**

```json
[
    {
        "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
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

### GET /api/orders

Fetch orders with optional filtering. Requires `read:orders` permission.

**Request:**

```http
GET /api/orders?symbol=BTC-USD&exchange=kraken&limit=100&offset=0
X-CSRF-Token: <csrf_token>
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `symbol` | string | no | Filter by instrument symbol |
| `exchange` | string | no | Filter by exchange (`paper`, `kraken`, `zonda`, `walutomat`) |
| `limit` | int | no | Number of orders, 1-1000 (default 100) |
| `offset` | int | no | Number of orders to skip (default 0) |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |

**Response (200):**

```json
[
    {
        "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
        "type": "order",
        "timestamp": "2026-01-18T12:00:00Z",
        "instrument": "BTC-USD",
        "exchange": "kraken",
        "client_order_id": "signal-a1b2c3d4",
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

### GET /api/signals

Fetch trading signals with optional filtering. Requires `read:market_data`
permission.

**Request:**

```http
GET /api/signals?instrument=BTC-USD&strategy=rsi_btc_1h&exchange=paper&hours=24&limit=100
X-CSRF-Token: <csrf_token>
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `instrument` | string | no | Filter by instrument symbol |
| `strategy` | string | no | Filter by strategy name |
| `exchange` | string | no | Filter by exchange (`paper`, `kraken`, `zonda`, `walutomat`) |
| `hours` | int | no | Hours of history, max 168 (default 24) |
| `limit` | int | no | Number of signals, max 1000 (default 100) |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |

**Response (200):**

```json
[
    {
        "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
        "type": "signal",
        "timestamp": "2026-01-18T12:00:00Z",
        "instrument": "BTC-USD",
        "exchange": "paper",
        "side": "buy",
        "strength": 0.85,
        "reason": "RSI 28.5 <= 30",
        "strategy_name": "rsi_btc_1h",
        "price": 42000.0,
        "fired_at": "2026-01-18T12:00:00Z"
    }
]
```

### GET /api/executions

Fetch order fills/executions. Requires `read:orders` permission.

**Request:**

```http
GET /api/executions?limit=100
X-CSRF-Token: <csrf_token>
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `limit` | int | no | Number of executions, max 1000 (default 100) |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |

**Response (200):**

```json
[
    {
        "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
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

### GET /api/positions

Fetch current portfolio positions. Requires `read:positions` permission.

**Request:**

```http
GET /api/positions
X-CSRF-Token: <csrf_token>
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |

**Response (200):**

```json
[
    {
        "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
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

### GET /api/exchanges

List distinct exchange names from active symbol aliases. Requires
`read:market_data` permission.

**Request:**

```http
GET /api/exchanges
X-CSRF-Token: <csrf_token>
```

**Response (200):**

```json
["kraken", "polygon", "walutomat", "zonda"]
```

### GET /api/exchanges/{exchange}/instruments

List distinct native symbols available on a given exchange. Requires
`read:market_data` permission.

**Request:**

```http
GET /api/exchanges/kraken/instruments
X-CSRF-Token: <csrf_token>
```

**Response (200):**

```json
["BTC-USD", "ETH-USD", "SOL-USD"]
```

### GET /api/status

System-wide status including trader process, backtests, and active
strategies. Requires `read:system_status` permission.

**Request:**

```http
GET /api/status
X-CSRF-Token: <csrf_token>
```

**Response (200):**

```json
{
    "trader": {
        "status": "running",
        "pid": null,
        "started_at": null,
        "command": null,
        "exit_code": null,
        "error": null
    },
    "backtests": {},
    "strategies": [
        {
            "strategy_name": "rsi_btc_1h",
            "status": "running",
            "details": {},
            "signals_generated": 42,
            "trades_executed": 5,
            "last_signal": "buy",
            "last_signal_time": "2026-01-18T11:45:00Z",
            "pnl": 150.25,
            "pid": 12345,
            "uptime": "2h 15m"
        }
    ]
}
```

### GET /api/ws/stats

WebSocket and ZMQ bridge statistics. Requires `read:system_status`
permission.

**Request:**

```http
GET /api/ws/stats
X-CSRF-Token: <csrf_token>
```

**Response (200):**

```json
{
    "websocket": {
        "active_connections": 5,
        "topic_subscribers": {
            "market.kraken.BTC-USD.candles.1h": 3,
            "signals.paper.BTC-USD.rsi_btc_1h": 2
        },
        "client_count": 5
    },
    "zmq_bridge": {
        "active_topics": 12,
        "subscriber_tasks": 12,
        "available_topics": ["admin", "market", "orders.commands", "orders.events", "signals", "strategy.signals", "system.heartbeats."]
    },
    "connections": {
        "active_connections": 5,
        "zmq_subscribers": 12,
        "subscriber_tasks": 12,
        "active_topics": 8,
        "active_clients": 5
    },
    "topics": {
        "market.kraken.BTC-USD.candles.1h": {
            "active_subscribers": 3,
            "received": 1200,
            "forwarded": 1180,
            "throttled": 20,
            "dropped": 0,
            "timeout": 0,
            "errors": 0,
            "invalid_messages": 0,
            "last_message_ts": 1737208800.0,
            "throttle_ms": 100,
            "pattern": "market."
        }
    },
    "subscriptions": {
        "per_topic": {
            "market.kraken.BTC-USD.candles.1h": 3
        },
        "per_client": {
            "140234567890": ["market.kraken.BTC-USD.candles.1h"]
        }
    },
    "config": {
        "broker_xpub": "tcp://127.0.0.1:7501",
        "heartbeat_interval_ms": 15000
    }
}
```

### GET /api/zmq/health

ZMQ bridge health check. Requires `read:system_status` permission.

**Request:**

```http
GET /api/zmq/health
X-CSRF-Token: <csrf_token>
```

**Response (200):**

```json
{
    "status": "healthy",
    "timestamp": "2026-01-18T12:00:00Z",
    "components": {
        "zmq_context": "ok",
        "websocket_manager": "ok",
        "active_connections": 5
    },
    "config": {
        "available_topics": ["admin", "market", "orders.commands", "orders.events", "signals", "strategy.signals", "system.heartbeats."]
    },
    "connections": {
        "active_connections": 5,
        "zmq_subscribers": 12,
        "subscriber_tasks": 12,
        "active_topics": 8,
        "active_clients": 3
    },
    "message_stats": {},
    "errors": []
}
```

## Process Management

Process endpoints manage background services (feeds, strategies, executors,
brokers). Most require `manage:processes` permission (operator/admin). The
summary endpoint requires only `read:system_status` (viewer+).

### GET /api/processes/available

List registered process templates that can be instantiated. Requires
`manage:processes` permission.

**Response (200):**

```json
{
    "processes": [
        {
            "name": "zmq_broker",
            "class_path": "snapper.messaging.infrastructure.broker.ZmqBrokerProcess",
            "method": "start",
            "description": "ZeroMQ XPUB/XSUB message broker",
            "lifecycle": "long_running",
            "role": "core",
            "tags": ["infrastructure"],
            "parameters_schema": null
        }
    ],
    "count": 1
}
```

### GET /api/processes/configured

List configured process instances with runtime state. Requires
`manage:processes` permission.

**Response (200):**

```json
{
    "processes": [
        {
            "name": "zmq_broker",
            "enabled": true,
            "running": true,
            "mode": "thread",
            "class_path": "snapper.messaging.infrastructure.broker.ZmqBrokerProcess",
            "method": "start",
            "parameters": {},
            "note": null,
            "lifecycle": "long_running",
            "role": "core",
            "tags": ["infrastructure"],
            "parameters_schema": null,
            "is_one_shot": false,
            "active_public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b"
        }
    ],
    "count": 1
}
```

### GET /api/processes/summary

Lightweight process category counts for the overview dashboard. Requires
`read:system_status` permission.

**Response (200):**

```json
{
    "feeds": { "running": 2, "total": 3 },
    "strategies": { "running": 1, "total": 2 },
    "executors": { "running": 1, "total": 1 },
    "brokers": { "running": 1, "total": 1 }
}
```

### POST /api/processes

Create a new process configuration from a registered template. Requires
`manage:processes` permission. Returns 201 on success.

**Request:**

```http
POST /api/processes
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "name": "kraken_feed_btc",
    "template": "kraken_feed_publisher",
    "enabled": true,
    "mode": "thread",
    "parameters": { "symbols": ["BTC-USD"] },
    "note": "Kraken BTC feed"
}
```

**Response (201):**

```json
{
    "status": "created",
    "process": {
        "name": "kraken_feed_btc",
        "template": "kraken_feed_publisher"
    }
}
```

### GET /api/processes/schema/{name}

Get the configuration schema and defaults for a registered process template.
Requires `manage:processes` permission.

**Response (200):**

```json
{
    "name": "kraken_feed_publisher",
    "description": "Kraken market data feed publisher",
    "class_path": "snapper.messaging.publishers.kraken.KrakenMarketDataPublisher",
    "method": "start",
    "default_enabled": true,
    "default_mode": "thread",
    "default_parameters": {},
    "lifecycle": "long_running"
}
```

### POST /api/processes/{name}/start

Start a configured process. Requires `manage:processes` permission.

**Request:**

```http
POST /api/processes/zmq_broker/start
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "mode": "process"
}
```

All fields are optional. `mode` overrides the stored execution mode for this
run only — it does not persist to Settings. Omitting it uses the stored value.

**Response (200):**

```json
{
    "status": "success",
    "name": "zmq_broker",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "message": "Process started"
}
```

### POST /api/processes/{name}/stop

Stop a running process. Requires `manage:processes` permission.

**Request:**

```http
POST /api/processes/zmq_broker/stop
X-CSRF-Token: <csrf_token>
```

**Response (200):**

```json
{
    "status": "success",
    "name": "zmq_broker",
    "message": "Process stopped"
}
```

### GET /api/processes/runs

List historical process runs. Requires `manage:processes` permission.

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `limit` | int | no | Number of runs to return (default 50) |
| `name` | string | no | Filter by process name |

**Response (200):**

```json
{
    "runs": [
        {
            "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
            "process_name": "zmq_broker",
            "status": "succeeded",
            "role": "core",
            "lifecycle": "long_running",
            "parameters": {},
            "result": null,
            "error": null,
            "tags": ["infrastructure"],
            "started_at": "2026-01-18T10:00:00Z",
            "completed_at": "2026-01-18T18:00:00Z"
        }
    ],
    "count": 1
}
```

## Strategies

### GET /api/strategies

List configured strategy processes with lightweight status. Requires
`read:strategies` permission (viewer+).

**Response (200):**

```json
{
    "strategies": [
        {
            "name": "strategy_rsi_btc_1h",
            "running": true,
            "enabled": true,
            "mode": "process"
        }
    ],
    "count": 1
}
```

## Settings

Settings endpoints require `configure:system` permission (admin only).

### GET /api/settings

List all settings, optionally filtered by category.

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `category` | string | no | Filter by setting category |

**Response (200):**

```json
[
    {
        "key": "kraken_api_key",
        "value": "xK9m...",
        "category": "exchanges",
        "description": "Kraken REST API key",
        "updated_at": "2026-01-15T10:00:00Z",
        "updated_by": "admin"
    }
]
```

### GET /api/settings/categories

List distinct setting category names.

**Response (200):**

```json
{
    "categories": ["exchanges", "strategy", "system"]
}
```

### POST /api/settings/{key}/set

Set (create or update) a setting value.

**Request:**

```http
POST /api/settings/kraken_api_key/set
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "payload": {
        "value": "new-api-key-value",
        "category": "exchanges",
        "description": "Kraken REST API key"
    }
}
```

**Payload fields:**

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `value` | string | yes | Setting value |
| `category` | string | no | Setting category (default: `system`) |
| `description` | string | no | Human-readable description |

**Response (200):**

```json
{
    "type": "setting_response",
    "public_id": "<uuid7>",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "payload": {
        "type": "setting_read",
        "key": "kraken_api_key",
        "value": "new-api-key-value",
        "category": "exchanges",
        "description": "Kraken REST API key",
        "updated_at": "2026-01-18T12:00:00Z",
        "updated_by": "admin"
    }
}
```

### POST /api/settings/{key}/remove

Soft-delete a setting by key (sets `known_to` to current time).

**Request:**

```http
POST /api/settings/kraken_api_key/remove
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 2,
    "timestamp": "2026-01-18T12:00:01Z",
    "payload": {}
}
```

**Response (200):**

```json
{
    "type": "message",
    "public_id": "<uuid7>",
    "session_id": "<server-session>",
    "sequence_id": 2,
    "timestamp": "2026-01-18T12:00:01Z",
    "payload": "Setting 'kraken_api_key' deleted successfully"
}
```

## WebSocket

### Connection and Authentication

The WebSocket endpoint is at `/api/ws`. Authentication is message-based,
not query-parameter-based.

**Connection flow:**

1. Client connects to `ws://host:port/api/ws` (or `wss://` over TLS)
2. Server accepts the connection and validates the origin header
3. Server sends `auth_required` message
4. Client sends `authenticate` message with a WebSocket token
5. Server validates the token and sends `auth_ok`
6. Server sends `auth_complete` with available topics for the user's role
7. Client can now subscribe to topics

**Obtaining a WebSocket token:**

Call `POST /api/auth/refresh` (requires a valid `refresh_token` cookie).
The response includes `ws_token` and `ws_token_exp` fields.

### Authentication Messages

**Server sends after connection:**

```json
{
    "type": "auth_required",
    "timeout": 10,
    "timestamp": "2026-01-18T12:00:00Z"
}
```

**Client sends to authenticate:**

```json
{
    "type": "authenticate",
    "ws_token": "eyJhbGciOi...",
    "timestamp": "2026-01-18T12:00:00Z"
}
```

**Server sends on success:**

```json
{
    "type": "auth_ok",
    "exp": "2026-01-18T12:30:00Z",
    "timestamp": "2026-01-18T12:00:00Z"
}
```

**Server sends session info:**

```json
{
    "type": "auth_complete",
    "available_topics": [
        "market",
        "orders.commands",
        "orders.events",
        "signals",
        "strategy.signals",
        "system.heartbeats."
    ],
    "user_role": "operator",
    "session_expires_at": "2026-01-18T12:15:00Z",
    "ws_token_exp": "2026-01-18T12:30:00Z",
    "timestamp": "2026-01-18T12:00:00Z"
}
```

`available_topics` contains allowed topic roots/pattern keys for the user's
role. Concrete subscriptions may use those roots as prefixes or full topic
strings such as `market.kraken.BTC-USD.candles.1h` or
`signals.paper.BTC-USD.rsi_btc_1h`.

**Server sends on failure:**

```json
{
    "type": "auth_failed",
    "reason": "Invalid token",
    "timestamp": "2026-01-18T12:00:00Z"
}
```

### Reauthentication

Before the WebSocket token expires, the server sends a `reauth_required`
message. Clients may also refresh proactively before that deadline. In both
cases they obtain a new `ws_token` via `POST /api/auth/refresh` and send it as
a `reauth` message.

**Server warning:**

```json
{
    "type": "reauth_required",
    "deadline": "2026-01-18T12:29:00Z",
    "timestamp": "2026-01-18T12:28:00Z"
}
```

**Client sends new token:**

```json
{
    "type": "reauth",
    "ws_token": "eyJhbGciOi...(new token)...",
    "timestamp": "2026-01-18T12:28:30Z"
}
```

**Server confirms:**

```json
{
    "type": "reauth_ok",
    "exp": "2026-01-18T13:00:00Z",
    "timestamp": "2026-01-18T12:28:30Z"
}
```

If the client does not reauthenticate in time:

```json
{
    "type": "auth_expired",
    "timestamp": "2026-01-18T12:30:00Z"
}
```

### Subscription Management

**Subscribe to topics:**

```json
{
    "type": "subscribe",
    "topics": ["market.kraken.BTC-USD.candles.1h", "signals.paper.BTC-USD.rsi_btc_1h"]
}
```

**Server confirms subscription:**

```json
{
    "type": "subscription_success",
    "action": "subscribe",
    "status": "subscribed",
    "topics": ["market.kraken.BTC-USD.candles.1h", "signals.paper.BTC-USD.rsi_btc_1h"],
    "denied_topics": [],
    "active_subscriptions": ["market.kraken.BTC-USD.candles.1h", "signals.paper.BTC-USD.rsi_btc_1h"],
    "message": null,
    "timestamp": "2026-01-18T12:00:01Z"
}
```

**Unsubscribe:**

```json
{
    "type": "unsubscribe",
    "topics": ["market.kraken.BTC-USD.candles.1h"]
}
```

**List active subscriptions:**

```json
{
    "type": "get_subscriptions"
}
```

**Response:**

```json
{
    "type": "subscriptions_list",
    "subscriptions": ["signals.paper.BTC-USD.rsi_btc_1h"],
    "available_topics": ["market", "orders.commands", "orders.events", "signals", "strategy.signals", "system.heartbeats."],
    "total_available": 6,
    "timestamp": "2026-01-18T12:00:02Z"
}
```

### Ping/Pong

**Client sends:**

```json
{
    "type": "ping"
}
```

**Server responds:**

```json
{
    "type": "pong",
    "timestamp": "2026-01-18T12:00:00Z",
    "active_connections": 5
}
```

### Server Data Messages

Data messages are flat JSON objects forwarded directly from the ZMQ bus.
There is no `"data"` wrapper. Messages with malformed JSON are dropped by the bridge
before forwarding and are counted in the `invalid_messages` metric for the topic.

All data messages carry `session_id` and `sequence_id` provenance fields (see REST section
above). All WebSocket control messages (auth, subscribe, ping/pong, errors) inherit from
`StrictDataSchema`, so they also carry the same provenance envelope. Clients can
use these fields to detect gaps without server-side replay support.

#### Candle

```json
{
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "type": "candle",
    "timestamp": "2026-01-18T11:00:00Z",
    "session_id": "019e0000-0000-7000-0000-000000000001",
    "sequence_id": 42,
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
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
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

#### Trade

```json
{
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "type": "trade",
    "timestamp": "2026-01-18T12:00:00Z",
    "instrument": "BTC-USD",
    "exchange": "kraken",
    "executed_at": "2026-01-18T12:00:00Z",
    "price": 42000.5,
    "volume": 0.25,
    "side": "buy"
}
```

#### Signal

```json
{
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "type": "signal",
    "timestamp": "2026-01-18T12:00:00Z",
    "session_id": "019e0000-0000-7000-0000-000000000001",
    "sequence_id": 7,
    "instrument": "BTC-USD",
    "exchange": "paper",
    "side": "buy",
    "strength": 0.85,
    "reason": "RSI 28.5 <= 30",
    "price": 42000.0,
    "strategy_name": "rsi_btc_1h",
    "fired_at": "2026-01-18T12:00:00Z"
}
```

#### Execution

```json
{
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
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
```

#### Order

```json
{
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
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
    "reason": null,
    "time_in_force": "GTC",
    "error": null,
    "created_at": "2026-01-18T12:00:00Z",
    "updated_at": null
}
```

#### Heartbeat

```json
{
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "type": "heartbeat",
    "timestamp": "2026-01-18T12:00:00Z",
    "session_id": "019e1a2b-0000-7000-8000-000000000001",
    "sequence_id": 42,
    "component": "zmq_broker",
    "sequence": 42,
    "status": "healthy",
    "lag_ms": 5,
    "meta": {}
}
```

#### Error

```json
{
    "type": "error",
    "message": "Topic 'invalid.topic' does not exist",
    "timestamp": "2026-01-18T12:00:00Z"
}
```

### Available Topic Patterns

Topics use dot-separated hierarchical names. Subscribe using the full topic
string or a prefix to match multiple topics.

#### Market Data

- `market.{exchange}.{instrument}.candles.{timeframe}` -- OHLCV candles
- `market.{exchange}.{instrument}.ticks` -- Price ticks
- `market.{exchange}.{instrument}.trades` -- Individual trades
- `market.paper.{source_exchange}.{instrument}.candles.{timeframe}` -- Paper candles sourced from a real exchange
- `market.paper.{source_exchange}.{instrument}.ticks` -- Paper ticks sourced from a real exchange

#### Signals

- `signals.{exchange}.{instrument}.live` -- Live trading signals
- `signals.paper.{instrument}.{strategy_name}` -- Paper trading signals

#### Order Events

- `orders.events.{exchange}.{instrument}.submitted` -- Order submitted
- `orders.events.{exchange}.{instrument}.accepted` -- Order accepted
- `orders.events.{exchange}.{instrument}.rejected` -- Order rejected
- `orders.events.{exchange}.{instrument}.executed` -- Order fill
- `orders.events.{exchange}.{instrument}.cancelled` -- Order cancelled
- `orders.events.{exchange}.{instrument}.expired` -- Order expired
- `orders.events.{exchange}.{instrument}.replaced` -- Order replaced

#### Order Commands

- `orders.commands.{exchange}.{instrument}.submit` -- Submit order
- `orders.commands.{exchange}.{instrument}.cancel` -- Cancel order
- `orders.commands.{exchange}.{instrument}.replace` -- Replace order

#### System

- `system.heartbeats.{component}.{name}[.{source}]` -- Component heartbeats
- `admin.{resource}` -- Administrative events (admin only)

## Error Handling

| HTTP Status | Description |
| ----------- | ----------- |
| 401 | Missing or invalid authentication |
| 403 | Insufficient permissions or invalid CSRF token |
| 404 | Resource not found |
| 422 | Request validation error |
| 429 | Rate limit exceeded (Retry-After header included) |
| 500 | Internal server error |

## Usage Example (Python)

```python
import httpx

BASE_URL = "http://localhost:8000/api"

async def main():
    async with httpx.AsyncClient() as client:
        login_resp = await client.post(
            f"{BASE_URL}/auth/login",
            json={"username": "admin", "password": "password123"},
        )
        csrf_token = client.cookies.get("csrf_token")

        refresh_resp = await client.post(f"{BASE_URL}/auth/refresh")
        ws_token = refresh_resp.json()["ws_token"]

        candles_resp = await client.get(
            f"{BASE_URL}/candles",
            params={
                "instrument": "BTC-USD",
                "exchange": "kraken",
                "timeframe": "1h",
            },
            headers={"X-CSRF-Token": csrf_token},
        )
        candles = candles_resp.json()
        print(candles)
```

## Usage Example (JavaScript)

This example is intentionally minimal. The shipped frontend client reuses
cached WS tickets, schedules proactive reauthentication before expiry, and
sends heartbeat pings every 5 seconds.

```javascript
async function connect() {
    const refreshResp = await fetch("/api/auth/refresh", {
        method: "POST",
        credentials: "include",
    });
    const { ws_token } = await refreshResp.json();

    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${protocol}//${location.host}/api/ws`);

    ws.onopen = () => {
        console.log("Connected, waiting for auth_required...");
    };

    ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);

        if (msg.type === "auth_required") {
            ws.send(JSON.stringify({
                type: "authenticate",
                ws_token: ws_token,
            }));
        }

        if (msg.type === "auth_complete") {
            console.log("Authenticated. Topics:", msg.available_topics);
            ws.send(JSON.stringify({
                type: "subscribe",
                topics: ["market.kraken.BTC-USD.candles.1h"],
            }));
        }

        if (msg.type === "candle" || msg.type === "tick") {
            console.log("Data:", msg);
        }

        if (msg.type === "reauth_required") {
            refreshAndReauth(ws);
        }
    };

    setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: "ping" }));
        }
    }, 30000);
}
```
