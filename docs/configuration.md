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
| `SERVER_API_ONLY` | `false` | Skip process autostart; serve API + WS bridge only (separate-engine boots). Multi-worker uvicorn (multiple FastAPI processes sharing a broker) is supported — see `docs/architecture.md` Deployment Modes for the AI-review fanout dedup contract under N>1. Set `SNAPPER_COORDINATOR_INSTANCE_ID` + `SNAPPER_COORDINATOR_INSTANCE_COUNT` per worker to enable the shared partitioning. |
| `PROCESS_AUTOSTART_PROFILE` | `all` | Select which enabled process configs this node starts: `all` for single-container/dev, `api` for backend-without-market-publishers, `feed` for the dedicated feed container. |
| `TELEMETRY_RECORDING_ENABLED` | `false` | Record pings, heartbeats, and GET reads to `telemetry` table. High volume — enable for debugging only |

The `Default` column reflects the Pydantic defaults on
`BootstrapSettingsLoader` (`src/snapper/config/bootstrap.py`) for
the server / encryption / ZMQ / coordinator rows. The
observability rows further down (`SYSTEM_METRICS_*`, `RETENTION_*`,
`DB_METRICS_*`, `DB_POOL_*`, `SNAPPER_*_PROBE`) are NOT loaded
through that class.
`application/system_metrics/snapshotter.py` and
`application/db_stats/snapshotter.py` read their env vars directly
via `os.environ`. The retention env vars are read in two places:
`application/retention/scheduler.py` (`RETENTION_INTERVAL_SECONDS`,
`RETENTION_DISABLED`, `RETENTION_OUTPUT_DIR`) and
`application/retention/service.py` (`RETENTION_DRY_RUN`), with
fallback resolvers defined in `application/retention/policies.py`.
The defaults shown for those rows reflect the per-module fallbacks.

The repo-root `.env.example` intentionally ships with Docker-friendly
overrides for two server values: `SERVER_HOST=0.0.0.0` (so the dev
server binds inside containers) and
`SERVER_FORWARDED_ALLOW_IPS=127.0.0.1,172.17.0.1` (so requests
forwarded by the default Docker bridge are accepted). Caveat: the
`make run-server` / `make dev-backend` targets in the Makefile
pass `--host 0.0.0.0` on the command line, which wins over the
`.env` value via Typer's `host or s.server_host` resolution — so
editing `.env` only changes `SERVER_HOST` for direct
`snapper server` invocations (no `--host` flag).
`SERVER_FORWARDED_ALLOW_IPS` is honored on every server-start path
because no CLI flag overrides it; `make migrate-dev` does not bind
either value.

### ZeroMQ

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `ZMQ_BROKER_XSUB` | `tcp://127.0.0.1:7500` | XSUB endpoint (publishers connect here) |
| `ZMQ_BROKER_XPUB` | `tcp://127.0.0.1:7501` | XPUB endpoint (subscribers connect here) |
| `ZMQ_BROKER_BIND_XSUB` | empty | Optional broker bind endpoint for XSUB. Empty means bind `ZMQ_BROKER_XSUB`; cross-container deployments set this to `tcp://0.0.0.0:7500`. |
| `ZMQ_BROKER_BIND_XPUB` | empty | Optional broker bind endpoint for XPUB. Empty means bind `ZMQ_BROKER_XPUB`; cross-container deployments set this to `tcp://0.0.0.0:7501`. |

Bind endpoints exist because ZMQ `bind()` requires a local interface
while cross-container clients connect through the Docker service name.
In the compose split, the backend broker binds `0.0.0.0` and both the
backend and `snapper-feed` connect to `tcp://snapper:7500/7501`.

### Database Engine Pool

These optional variables are read directly by the repository engine
factory and only affect PostgreSQL engines.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `DB_POOL_SIZE` | SQLAlchemy default | Per-process pool size. |
| `DB_MAX_OVERFLOW` | SQLAlchemy default | Per-process overflow connections. |

Set them on the dedicated feed container when publisher subprocesses
would otherwise multiply the default pool toward PostgreSQL
`max_connections`.

### Coordinator Sharding

The trade runtime supports horizontal sharding via these variables.
With `SNAPPER_COORDINATOR_INSTANCE_COUNT > 1` each instance owns a
partition of the active wallets / outbox rows, enabling multi-worker
uvicorn deployments and parallel `trade-zmq` instances.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `SNAPPER_COORDINATOR_INSTANCE_ID` | `0` | Zero-based shard index for this instance |
| `SNAPPER_COORDINATOR_INSTANCE_COUNT` | `1` | Total number of instances in the cluster |
| `SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS` | `1000` | Per-tick upper bound on outbox rows scanned by this shard |

### Observability Pipeline

These variables tune the always-on background pipelines that emit
system-metrics snapshots, run retention archive+purge sweeps, and
sample DB stats. See [observability.md](observability.md) for the
underlying snapshots and the retention window math.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `SYSTEM_METRICS_INTERVAL_SECONDS` | `5` | Cadence for the in-process system-metrics snapshotter |
| `SYSTEM_METRICS_HISTORY_CAP` | `17280` | Maximum number of snapshots retained in memory (≈ 24 h at 5 s cadence) |
| `RETENTION_INTERVAL_SECONDS` | `3600` | Cadence for the retention archive+purge tick |
| `RETENTION_DISABLED` | `false` | Disable the retention loop entirely (still allow on-demand archive via CLI) |
| `RETENTION_DRY_RUN` | `false` | Compute the window and counts but skip writes/purges |
| `RETENTION_OUTPUT_DIR` | `data` | Base directory for archive CSV writes (matches the CLI `--output-dir` default) |
| `DB_METRICS_INTERVAL_SECONDS` | `60` | Cadence for the per-table row-count / index-health sampler |
| `DB_METRICS_DISABLED` | `false` | Disable the DB stats sampler |
| `SNAPPER_TICK_PROBE` | unset | Enable per-stage tick hot-path histograms in publisher logs when truthy (`1`, `true`, `yes`) |
| `SNAPPER_TRADE_PROBE` | unset | Enable per-stage trade hot-path histograms in publisher logs when truthy (`1`, `true`, `yes`) |

The two probe variables flush one log line roughly every 10 seconds
with per-stage `count`, `rate`, `p50`, `p95`, `p99`, and `max`.
Leave them unset outside targeted throughput investigations.

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
| `exchange` | Exchange identifier (`kraken`, `kraken_futures`, `walutomat`, `paper`) |
| `credential_type` | Envelope shape: `api_key_secret`, `rsa_pem`, `oauth`, or `paper` |
| `encrypted_payload` | Fernet-encrypted JSON envelope (shape depends on `credential_type`) |
| `label` | Human-readable description of the credential row |

Envelope shapes by `credential_type`:

- `api_key_secret` — `{"api_key": "...", "api_secret": "..."}` (Kraken, Kraken Futures)
- `rsa_pem` — `{"api_key": "...", "private_key_pem": "..."}` (Walutomat)
- `oauth` — `{"client_id": "...", "client_secret": "...", "refresh_token": "..."}`
- `paper` — `{"initial_balance": "10000.0"}` (paper wallets)

Seed profiles (`dev.toml` / `prod.toml`) bootstrap `wallet_credentials`
on a fresh database. Note: the seed loader supports the
`api_key_secret`, `rsa_pem`, and `paper` envelope shapes — the
`oauth` shape is accepted at the credential-resolver layer but
**not** by the current seed loader, so OAuth credentials must be
inserted through the REST `wallet_credentials` create/rotate routes
(`POST /api/wallets/{wallet_public_id}/credentials`,
`POST /api/wallets/{wallet_public_id}/credentials/{credential_public_id}/rotate`)
rather than via
seed.

The credential management REST surface AND the matching frontend UI
(`frontend/src/features/admin/CredentialManagement/`) are both
shipped. Day-to-day operators use the dashboard's Credential
Management view; headless callers can hit the REST routes directly
(curl / Postman — the snapper-mcp bridge does NOT expose
credential CRUD as MCP tools). Other recovery options
are re-running the seed against a clean DB (seed is idempotent — it
skips wallets that already exist) or direct SQL surgery on the
encrypted payload as a last resort.

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

The trade runtime always uses the durable (outbox-driven) dispatch
path. The engine writes `TradeCommand` rows to the database; the
`OutboxDispatcher` picks them up and publishes to ZMQ. Executor-side
`VenueEvent` writes are fail-closed — a failed persist raises before
the executor acknowledges the venue event.

### Market Persist Policy

Market-data publishers always publish ZMQ frames, but DB persistence is
controlled by these settings so high-volume feeds can be cache-first:

| Key | Description |
| --- | ----------- |
| `market_persist_ticks` | Persistence mode for tick rows |
| `market_persist_trades` | Persistence mode for trade rows |
| `market_persist_candles` | Persistence mode for candle rows |
| `market_persist_extra` | Explicit extra instrument allowlist |
| `market_persist_exclude` | Explicit instrument denylist |

`MarketPersistPolicy` refreshes from settings and admin bus events, then
injects immutable allow/deny snapshots into publishers. Invalid edits are
rejected for that refresh cycle and the previous policy remains active.
Use `/api/market/cache/health` and `/api/market/coverage` to verify the
effective persisted universe.

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

The block below mirrors the variables documented in
`.env.example` at the repo root. The inline comments are
abbreviated here for readability; the canonical version of the
file (with full per-variable rationale and rotation guidance)
lives in `.env.example` itself.

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
PROCESS_AUTOSTART_PROFILE=all
SERVER_PROXY_HEADERS=true
SERVER_FORWARDED_ALLOW_IPS=127.0.0.1,172.17.0.1

# Telemetry recording (pings, heartbeats, GET reads — high volume)
TELEMETRY_RECORDING_ENABLED=false

# ZMQ broker settings
ZMQ_BROKER_XSUB=tcp://127.0.0.1:7500
ZMQ_BROKER_XPUB=tcp://127.0.0.1:7501
# ZMQ_BROKER_BIND_XSUB=tcp://0.0.0.0:7500
# ZMQ_BROKER_BIND_XPUB=tcp://0.0.0.0:7501

# PostgreSQL pool clamps for multi-process feed deployments
# DB_POOL_SIZE=2
# DB_MAX_OVERFLOW=3

# Coordinator sharding (multi-instance trade-zmq / multi-worker uvicorn)
SNAPPER_COORDINATOR_INSTANCE_ID=0
SNAPPER_COORDINATOR_INSTANCE_COUNT=1
SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS=1000

# Observability pipeline (system-metrics snapshotter)
SYSTEM_METRICS_INTERVAL_SECONDS=5
SYSTEM_METRICS_HISTORY_CAP=17280

# Retention policy framework
RETENTION_INTERVAL_SECONDS=3600
RETENTION_DISABLED=false
RETENTION_DRY_RUN=false
RETENTION_OUTPUT_DIR=data

# DB stats sampler (per-table row-count snapshots)
DB_METRICS_INTERVAL_SECONDS=60
DB_METRICS_DISABLED=false

# Publisher hot-path probes, disabled by default
# SNAPPER_TICK_PROBE=1
# SNAPPER_TRADE_PROBE=1
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
- Shared `.env` allowlist validation

The allowlist is the union of `BootstrapSettingsLoader` aliases and
each subsystem's exported `ENV_VARS` set. Unknown keys fail startup
with suggestions, so typos such as `MASTER_PASWORD` do not silently
fall back to defaults. When adding a new env var, register it on the
owning module's `ENV_VARS` or add it as a bootstrap settings field.
