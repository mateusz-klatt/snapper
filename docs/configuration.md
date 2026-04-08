# Configuration

Snapper's configuration system is based on two layers: environment variables
(bootstrap) and database settings.

## Environment Variables

Settings loaded from `.env` file or environment variables.

### Database

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `DB_URL` | `sqlite+aiosqlite:///./data/snapper.db` | SQLAlchemy connection URL |

Database URL examples:

```bash
# SQLite (development default)
DB_URL=sqlite+aiosqlite:///./data/snapper.db

# PostgreSQL (production)
DB_URL=postgresql+asyncpg://user:password@localhost:5432/snapper
```

### Encryption

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `MASTER_PASSWORD` | `snapper_default_master_password_v1` | Password for encrypting settings in database (salt derived automatically) |

**Important**: Change default values in production environment.

### HTTP Server

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `SERVER_HOST` | `127.0.0.1` | Listen address |
| `SERVER_PORT` | `8000` | Server port |
| `SERVER_RELOAD` | `false` | Auto-reload for development |
| `SERVER_PROXY_HEADERS` | `true` | Enable proxy header parsing in uvicorn |
| `SERVER_FORWARDED_ALLOW_IPS` | `127.0.0.1` | Trusted proxy IPs/CIDRs for forwarded headers |
| `SERVER_API_ONLY` | `false` | Skip process autostart; serve API + WS bridge only (for multi-worker or separate engine) |
| `TELEMETRY_RECORDING_ENABLED` | `false` | Record pings, heartbeats, and GET reads to `telemetry` table. High volume — enable for debugging only |

### ZeroMQ

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `ZMQ_BROKER_XSUB` | `tcp://127.0.0.1:7500` | XSUB endpoint (publishers connect here) |
| `ZMQ_BROKER_XPUB` | `tcp://127.0.0.1:7501` | XPUB endpoint (subscribers connect here) |

## Database Settings

Sensitive data is stored encrypted in the `settings` table. Wallet-scoped
exchange credentials (api keys, secrets, PEM material) live in a separate
`wallet_credentials` table — see [Wallet Credentials](#wallet-credentials)
below. Shared market-data provider keys stay in `settings`.

### Market Data API Keys

| Key | Description |
| --- | ----------- |
| `polygon_api_key` | Polygon.io API key (shared market data — not wallet-scoped) |

### Wallet Credentials

Per-exchange trading credentials live in the `wallet_credentials` table
and are loaded by `CredentialResolver` during per-wallet executor
startup. Each row carries:

| Column | Description |
| --- | ----------- |
| `wallet_public_id` | UUID of the owning wallet (`(label, is_paper)` unique in `wallets` table) |
| `exchange` | Exchange identifier (`kraken`, `kraken_futures`, `walutomat`, `zonda`, `paper`) |
| `credential_type` | Envelope shape: `api_key_secret`, `rsa_pem`, `oauth`, or `paper` |
| `encrypted_payload` | Fernet-encrypted JSON envelope (shape depends on `credential_type`) |
| `label` | Human-readable description of the credential row |

Envelope shapes by `credential_type`:

- `api_key_secret` — `{"api_key": "...", "api_secret": "..."}` (Kraken, Zonda, Kraken Futures)
- `rsa_pem` — `{"api_key": "...", "private_key_pem": "..."}` (Walutomat)
- `paper` — `{"initial_balance": "10000.0"}` (paper wallets)

Seed profiles (`dev.toml` / `prod.toml`) are the current mechanism
for populating `wallet_credentials`. A runtime credential management
UI (list / add / rotate / delete per-wallet credentials, with
automatic executor restart on rotation) is planned as part of the
upcoming frontend work — see Phase 0d Section 6.4 of
`proprietary/plans/plan_multi_tenant_foundation.md`. Until the UI
ships, rotation requires editing the seed file and running the
seed command against a clean database (seed is idempotent — it
skips wallets that already exist), or direct SQL surgery on the
encrypted payload.

Seed file structure:

```toml
[[wallets]]
label = "default"
is_paper = false

[[wallets.credentials]]
exchange = "kraken"
credential_type = "api_key_secret"
api_key = "your-kraken-api-key"
api_secret = "your-kraken-api-secret"

[[wallets.credentials]]
exchange = "walutomat"
credential_type = "rsa_pem"
api_key = "your-walutomat-api-key"
private_key_pem_base64 = "your-pem-base64-encoded"
```

Two wallets named `default` — one with `is_paper=true` and one with
`is_paper=false` — coexist via the `(label, is_paper)` unique index.
The seed loader encrypts every payload with the master-password Fernet
key before insert.

### Trading Parameters

| Key | Default | Description |
| --- | ------- | ----------- |
| `instruments` | `{"kraken": ["BTC-USD", ...], ...}` | Instruments per exchange (dict) |
| `timeframes` | `["1m"]` | Candle timeframes (list) |
| `risk_max_leverage` | `1.0` | Maximum leverage |
| `risk_r_per_trade` | `0.005` | Risk per trade (0.5%) |
| `use_durable_commands` | `false` | Enable durable (outbox-driven) trade runtime instead of dual-write |

`use_durable_commands` is read by the trade runtime and executors during
startup. After changing it, restart `snapper trade-zmq` and the relevant
`snapper executor` processes.

Mode summary:

- `false` — dual-write mode. The engine writes durable command rows and still
  publishes directly to ZMQ.
- `true` — outbox-driven durable mode. Commands are published from the
  database outbox and executor `VenueEvent` writes become fail-closed.

### Publisher Micro-Batch

| Key                            | Default | Description                                       |
| ------------------------------ | ------- | ------------------------------------------------- |
| `write_buffer_flush_ms`        | `50`    | Flush age threshold in ms (cached at start)       |
| `write_buffer_candle_max_rows` | `100`   | Candle batch size trigger                         |
| `write_buffer_tick_max_rows`   | `500`   | Tick batch size trigger                           |
| `write_buffer_trade_max_rows`  | `500`   | Trade batch size trigger                          |

These settings are cached when the publisher starts and require a process
restart to take effect.

### Authentication

| Key | Default | Description |
| --- | ------- | ----------- |
| `auth_secret_key` | `change-me-in-production-use-openssl-rand-hex-32` | JWT signing secret |
| `csrf_secret_key` | `change-me-in-production-csrf-key` | CSRF token signing secret |
| `auth_algorithm` | `HS256` | JWT signing algorithm |
| `auth_access_token_expire_minutes` | `15` | Access token lifetime |
| `auth_refresh_token_expire_days` | `7` | Refresh token lifetime |
| `auth_refresh_token_expire_days_extended` | `30` | Extended refresh token lifetime |
| `ws_token_ttl_seconds` | `900` | WebSocket token lifetime |
| `csrf_token_expire_minutes` | `60` | CSRF token lifetime |
| `session_secure` | `false` | Require HTTPS for session cookies |
| `session_same_site` | `lax` | Session cookie SameSite mode |
| `session_domain` | `""` | Session cookie domain override |
| `ui_origin` | `""` | Additional allowed UI origins for CORS and WS origin validation |

## `.env.example` File

```bash
# Database connection
DB_URL=sqlite+aiosqlite:///./data/snapper.db

# Encryption settings for sensitive data (API keys, secrets)
# IMPORTANT: Change this default in production!
MASTER_PASSWORD=snapper_default_master_password_v1

# Server settings
SERVER_HOST=0.0.0.0
SERVER_PORT=8000
SERVER_RELOAD=false
SERVER_API_ONLY=false
SERVER_PROXY_HEADERS=true
SERVER_FORWARDED_ALLOW_IPS=127.0.0.1,172.17.0.1

# ZMQ broker settings
ZMQ_BROKER_XSUB=tcp://127.0.0.1:7500
ZMQ_BROKER_XPUB=tcp://127.0.0.1:7501
```

## Accessing Configuration in Code

### Bootstrap settings (without database)

```python
from snapper.config.settings import get_settings

settings = get_settings()
db_url = settings.db_url
host = settings.server_host
```

### With database access

```python
from snapper.config.settings import get_settings_with_service

settings = get_settings_with_service(settings_service)
polygon_key = settings.polygon_api_key  # Decrypted from database
```

Wallet-scoped exchange credentials do NOT appear on `AppSettings`. They
are loaded by `CredentialResolver` during per-wallet executor startup:

```python
from snapper.config.credentials import CredentialResolver

resolver = CredentialResolver(repository)
envelope = await resolver.get_credentials(
    exchange="kraken",
    wallet_public_id="01975a8b-3c7d-7000-8000-0000000000a1",
)
# envelope == {"api_key": "...", "api_secret": "..."}
```

## Managing Settings via API

### List Settings

```http
GET /api/settings
GET /api/settings?category=exchange
```

### List Categories

```http
GET /api/settings/categories
```

### Set Setting

```http
POST /api/settings/{key}/set
X-CSRF-Token: <csrf_token>
Content-Type: application/json

{
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "payload": {
        "value": "new_value",
        "category": "system",
        "description": "optional description"
    }
}
```

### Remove Setting

```http
POST /api/settings/{key}/remove
X-CSRF-Token: <csrf_token>
Content-Type: application/json

{
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 2,
    "timestamp": "2026-01-18T12:00:01Z",
    "payload": {}
}
```

## Environment Configuration

### Development

```bash
# .env.development
DB_URL=sqlite+aiosqlite:///./data/snapper.db
SERVER_HOST=127.0.0.1
SERVER_PORT=8000
SERVER_RELOAD=true
SERVER_PROXY_HEADERS=true
SERVER_FORWARDED_ALLOW_IPS=127.0.0.1
```

### Production

```bash
# .env.production
DB_URL=postgresql+asyncpg://user:pass@db.example.com/snapper
MASTER_PASSWORD=<strong_random_password>
SERVER_HOST=0.0.0.0
SERVER_PORT=8000
SERVER_RELOAD=false
SERVER_PROXY_HEADERS=true
SERVER_FORWARDED_ALLOW_IPS=127.0.0.1,172.17.0.1
```

### Docker

Variables can be passed via `docker-compose.yml`:

```yaml
services:
  snapper:
    env_file: .env
    environment:
      - DB_URL=postgresql+asyncpg://snapper:password@postgres/snapper
```

## Settings Encryption

Sensitive settings are encrypted with Fernet (AES-128-CBC + HMAC-SHA256):

1.  Salt is derived from `MASTER_PASSWORD` via SHA-256
2.  Encryption key is derived via PBKDF2-HMAC-SHA256 (100,000 iterations)
3.  Values are encrypted with Fernet and stored in database

If `MASTER_PASSWORD` changes, encrypted settings become inaccessible.
Use `snapper settings-rotate-encryption` to rotate passwords safely.

## Configuration Validation

The system validates configuration at startup:

- Database connection check
- ZMQ endpoint verification
- Settings decryption test

Configuration errors are reported through exceptions with clear messages.
