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
# SQLite (default)
DB_URL=sqlite+aiosqlite:///./data/snapper.db

# PostgreSQL
DB_URL=postgresql+asyncpg://user:password@localhost:5432/snapper

# Azure SQL
DB_URL=mssql+pyodbc://user:password@server.database.windows.net/snapper?driver=ODBC+Driver+18+for+SQL+Server
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

### ZeroMQ

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `ZMQ_BROKER_XSUB` | `tcp://127.0.0.1:7500` | XSUB endpoint (publishers connect here) |
| `ZMQ_BROKER_XPUB` | `tcp://127.0.0.1:7501` | XPUB endpoint (subscribers connect here) |

## Database Settings

Sensitive data is stored encrypted in the `settings` table.

### Exchange API Keys

| Key | Description |
| --- | ----------- |
| `kraken_api_key` | Kraken API key |
| `kraken_api_secret` | Kraken API secret |
| `polygon_api_key` | Polygon.io API key |
| `walutomat_api_key` | Walutomat API key |
| `walutomat_private_key` | Walutomat private key |
| `zonda_api_key` | Zonda API key |
| `zonda_api_secret` | Zonda API secret |

### Trading Parameters

| Key | Default | Description |
| --- | ------- | ----------- |
| `trading_instruments` | `["BTC-USD", "ETH-USD"]` | List of trading instruments |
| `default_timeframe` | `1h` | Default candle timeframe |
| `max_position_size` | `1.0` | Maximum position size |
| `risk_per_trade` | `0.02` | Risk per trade (2%) |

### Authentication

| Key | Default | Description |
| --- | ------- | ----------- |
| `access_token_expire_minutes` | `15` | Access token lifetime |
| `refresh_token_expire_days` | `7` | Refresh token lifetime |
| `csrf_token_expire_minutes` | `60` | CSRF token lifetime |

## `.env.example` File

```bash
# =============================================================================
# Snapper Environment Configuration
# =============================================================================

# Database (SQLite default, PostgreSQL optional)
DB_URL=sqlite+aiosqlite:///./data/snapper.db

# Settings Encryption (CHANGE IN PRODUCTION!)
MASTER_PASSWORD=your_secure_master_password

# HTTP Server
SERVER_HOST=127.0.0.1
SERVER_PORT=8000
SERVER_RELOAD=false
SERVER_PROXY_HEADERS=true
SERVER_FORWARDED_ALLOW_IPS=127.0.0.1

# ZeroMQ Broker Endpoints
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
api_key = settings.kraken_api_key  # Decrypted from database
```

## Managing Settings via API

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

### List Settings

```http
GET /api/settings
```

## Environment Configuration

### Development

```bash
# .env.development
DB_URL=sqlite+aiosqlite:///./data/snapper_dev.db
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
