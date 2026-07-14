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
| `SNAPPER_ENV` | `development` | Deployment environment: `development`, `dev`, `test`, `testing`, and `ci` allow local placeholders; `production`, `prod`, and `staging` refuse placeholder secrets at startup/runtime. Any other value fails startup; matching is case-insensitive |
| `MASTER_PASSWORD` | `snapper_default_master_password_v1` | Password for encrypting settings in database (salt derived automatically) |

**Important**: Set `SNAPPER_ENV=production` and change the default
`MASTER_PASSWORD` in production. It is the ONLY secret you provision:
every internal key (settings encryption, JWT auth signing, CSRF token
signing, control-plane command signing) is derived from it with a
distinct purpose tag (`src/snapper/infrastructure/security/kdf.py`), so
production-like modes fail fast only when `MASTER_PASSWORD` still uses
the development placeholder. Changing `MASTER_PASSWORD` rotates every
derived key at the next restart — see the rotation runbook in
`docs/operations.md`.

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
the server / encryption / ZMQ / coordinator / trade-safety rows. The
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
| `ZMQ_BROKER_EMBEDDED` | `true` | When `false`, this node's launcher excludes the `zmq_broker` CORE process (a dedicated `snapper-broker` container owns the bus); the dashboard shows the broker as remotely managed. |
| `STRATEGIES_EMBEDDED` | `true` | When `false`, this node's launcher excludes role-STRATEGY processes and refuses manual local starts (a dedicated `snapper-strategies` container owns them). |
| `STRATEGY_EXTRA_PACKAGES` | empty | Comma-separated extra top-level packages process discovery imports (fail-soft) so out-of-tree strategies (e.g. a mounted proprietary tree on `PYTHONPATH`) register their processes. |

Bind endpoints exist because ZMQ `bind()` requires a local interface
while cross-container clients connect through the Docker service name.
In the compose split, the dedicated `snapper-broker` container binds
`0.0.0.0` (via explicit CLI flags) and the backend, `snapper-feed`, and
`snapper-egress` all connect to `tcp://snapper-broker:7500/7501`; the
backend additionally sets `ZMQ_BROKER_EMBEDDED=false` so its launcher
does not start a duplicate embedded broker.

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
Multi-instance coordinator deployments require PostgreSQL; startup
fails fast on SQLite when `SNAPPER_COORDINATOR_INSTANCE_COUNT > 1`
because SQLite does not enforce `SELECT ... FOR UPDATE` row claims.
Process-registry DB sync at server startup runs only on instance `0`
(or when the count is `1`); other instances skip it with a log line so
concurrent workers do not race on the registry setting rows.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `SNAPPER_COORDINATOR_INSTANCE_ID` | `0` | Zero-based shard index for this instance |
| `SNAPPER_COORDINATOR_INSTANCE_COUNT` | `1` | Total number of instances in the cluster |
| `SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS` | `1000` | Per-tick upper bound on outbox rows scanned by this shard; set to `unbounded`, `none`, or an empty value to disable the cap. Positive integers are accepted. |

### Trade Runtime Safety

These bootstrap variables gate stale trade dispatch and live multi-leg
execution safety.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `TRADE_COMMAND_DISPATCH_TTL_S` | `30.0` | Max age, in seconds, for trade commands. Stale `CREATED` submits expire in the outbox before publishing; executor stale-frame handling verifies venue state before any order placement; the executor's reconciliation sweep auto-rejects an unresolved `DISPATCHED` command on verified venue absence only after `max(120 s, 2 × TTL)`. Values `<= 0` disable the cap and the absence auto-reject. Keep this below the engine's 60 s in-flight valve. |
| `PAIRED_EXECUTION_GUARD_ENABLED` | `false` | Fail-closed live multi-leg strategy gate. With the default, live exchanges refuse multi-leg groups while paper multi-leg remains allowed. Enable only after the paired-execution guard is deployed. |
| `PAIRED_EXECUTION_ASSEMBLY_TIMEOUT_S` | `5.0` | Seconds a paired-execution group waits for all sibling legs to register durable commands before the guard can break the group. |
| `PAIRED_EXECUTION_FILL_TIMEOUT_S` | `30.0` | Seconds an armed paired-execution group waits for fills before the guard treats a lagging leg as breakage and compensates. |

### Observability Pipeline

These variables tune the always-on background pipelines that emit
system-metrics snapshots, run retention archive+purge sweeps, and
sample DB stats. See [observability.md](observability.md) for the
underlying snapshots and the retention window math.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `SYSTEM_METRICS_INTERVAL_SECONDS` | `5` | Cadence for the in-process system-metrics snapshotter |
| `SYSTEM_METRICS_HISTORY_CAP` | `17280` | Maximum number of snapshots retained in memory (≈ 24 h at 5 s cadence) |
| `SYSTEM_METRICS_DISK_FREE_WARN_BYTES` | `21474836480` | Free-byte warning threshold for the sampled data partition |
| `SYSTEM_METRICS_DISK_FREE_CRIT_BYTES` | `10737418240` | Free-byte critical threshold for the sampled data partition |
| `SYSTEM_METRICS_DISK_MOUNT_PATH` | `/` | Mount path sampled for disk-pressure REST metrics and host/disk heartbeats |
| `RETENTION_INTERVAL_SECONDS` | `3600` | Cadence for the retention archive+purge tick |
| `RETENTION_DISABLED` | `false` | Disable the retention loop entirely (still allow on-demand archive via CLI) |
| `RETENTION_DRY_RUN` | `false` | Compute the window and counts but skip writes/purges |
| `RETENTION_OUTPUT_DIR` | `data` | Base directory for archive CSV writes (matches the CLI `--output-dir` default) |
| `DB_METRICS_INTERVAL_SECONDS` | `60` | Cadence for the per-table SCD2 row-count sampler |
| `DB_METRICS_DISABLED` | `false` | Disable the DB stats sampler |
| `MARKET_DATA_WATCHDOG_DISABLED` | `false` | Park the silent-exchange market-data watchdog entirely |
| `MARKET_DATA_WATCHDOG_INTERVAL_SECONDS` | `60` | Poll cadence for the per-exchange candle-freshness check (floor 5) |
| `MARKET_DATA_WATCHDOG_THRESHOLD_SECONDS` | `600` | Whole-exchange silence threshold before the `critical_system_error` alert path fires (floor 120) |
| `MARKET_DATA_WATCHDOG_EXCHANGE_THRESHOLDS` | unset | Per-exchange overrides as `exchange=seconds` CSV; `0` disables one exchange (e.g. `walutomat=1200,kraken_equities=900`) |
| `SNAPPER_TICK_PROBE` | unset | Enable per-stage tick hot-path histograms in publisher logs when truthy (`1`, `true`, `yes`) |
| `SNAPPER_TRADE_PROBE` | unset | Enable per-stage trade hot-path histograms in publisher logs when truthy (`1`, `true`, `yes`) |

The two probe variables flush one log line roughly every 10 seconds
with per-stage `count`, `rate`, `p50`, `p95`, `p99`, and `max`.
Leave them unset outside targeted throughput investigations.

### Indicators

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `USE_TALIB` | `true` | Selects the indicator backend in `src/snapper/indicators/ta_lib_adapter.py` (read directly via `os.getenv` at module import). Values `true`/`1`/`yes` (case-insensitive) use the TA-Lib C library; any other value — or a failed TA-Lib import — falls back to the pure-Python RSI/MACD implementations. Not part of the `.env` allowlist (`KNOWN_ENV_KEYS` in `config/env_contract.py`), so set it as a process environment variable only — adding it to `.env` fails startup validation with `UnknownEnvKeyError`. |

## Database Settings

Sensitive data is stored encrypted in the `settings` table. Wallet-scoped
exchange credentials (api keys, secrets, PEM material) live in a separate
`wallet_credentials` table — see [Wallet Credentials](#wallet-credentials)
below. Shared market-data provider keys stay in `settings`.

### Market Data API Keys

| Key | Description |
| --- | ----------- |
| `polygon_api_key` | Polygon.io API key (shared market data — not wallet-scoped) |

### Push Notifications (APNs)

The notification sidecar reads its APNs credentials and the push-beta
rollout gate from the `settings` table:

| Key | Description |
| --- | ----------- |
| `apns_team_id` | Apple Developer team ID |
| `apns_key_id` | APNs auth key ID |
| `apns_bundle_id` | iOS app bundle identifier (reverse-DNS form) |
| `apns_topic` | APNs topic — normally identical to `apns_bundle_id` for alert pushes |
| `apns_environment` | `sandbox`, `production`, or `sandbox_and_production`; the latter runs both APNs clients and routes by each device's registered environment |
| `apns_private_key_p8_base64` | APNs auth key (`.p8` PKCS#8 PEM) encoded as base64 so it round-trips through the TOML seed |
| `push_beta_config` | JSON `{"enabled": bool, "user_public_ids": [...]}` rollout gate (category `notifications`). Disabled by default — every user receives pushes; when enabled, only allowlisted users do. An absent or malformed value falls back to the disabled default so a misedit can never silence pushes. Managed via [Manage Push-Beta Gate](#manage-push-beta-gate) |

All six `apns_*` keys are required when the notification sidecar
starts: `load_apns_config` fails loud at startup with the full list
of missing keys rather than producing cryptic APNs errors at
first-send time. They are seeded under category `apns` via the
proprietary seed profile (`proprietary/data/seed/{dev,prod}.toml`).

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

Seed profiles are resolved in three tiers:
`data/seed/{profile}.toml`, then `proprietary/data/seed/{profile}.toml`,
then the package-bundled `src/snapper/data/seed/{profile}.toml`. The
bundled `dev.toml` seeds an open-source paper wallet with a paper
credential; maintainers may have a proprietary override with live
wallet credentials. On a fresh database, `db-seed` creates the default
operator, every `[[wallets]]` entry, nested
`[[wallets.credentials]]` rows, each requested live reconciliation-method
config, and the first admin user's primary operator membership. If the
profile has no wallets, the loader falls back to a single `default`
paper wallet with a `10000.0` initial balance. Re-running seed on an
established database does not merge wallets: users are skipped when any
user exists, settings are inserted only when the key is absent, and the
whole operator/wallet bootstrap is skipped when either operators or
wallets already exist.

The seed loader supports the `api_key_secret`, `rsa_pem`, and `paper`
envelope shapes. The `oauth` shape is accepted at the
credential-resolver layer but **not** by the current seed loader, so
OAuth credentials must be inserted through the REST `wallet_credentials`
create/rotate routes
(`POST /api/wallets/{wallet_public_id}/credentials`,
`POST /api/wallets/{wallet_public_id}/credentials/{credential_public_id}/rotate`)
rather than via seed.

The credential management REST surface AND the matching frontend UI
(`frontend/src/features/admin/CredentialManagement/`) are both
shipped. Day-to-day operators use the dashboard's Credential
Management view; headless callers can hit the REST routes directly
(curl / Postman — the snapper-mcp bridge does NOT expose
credential CRUD as MCP tools). Other recovery options
are re-running the seed against a clean DB or direct SQL surgery on
the encrypted payload as a last resort.

Seed file structure:

```toml
[[wallets]]
label = "default"
is_paper = false

[[wallets.credentials]]
exchange = "kraken"
credential_type = "api_key_secret"
reconciliation_method = "unclassified"
api_key = "your-kraken-api-key"
api_secret = "your-kraken-api-secret"

[[wallets.credentials]]
exchange = "walutomat"
credential_type = "rsa_pem"
reconciliation_method = "spot_execution_replay"
api_key = "your-walutomat-api-key"
private_key_pem_base64 = "your-pem-base64-encoded"
```

Two wallets named `default` — one with `is_paper=true` and one with
`is_paper=false` — coexist via the `(label, is_paper)` unique index.
The seed loader encrypts every payload with the master-password Fernet
key before insert. Every seed credential requires
`reconciliation_method`. The accepted values are `futures_position`,
`spot_execution_replay`, `margin_ledger_replay`, and `unclassified`.
A real method is inserted atomically with its credential; `unclassified`
and paper credentials create no method-config row. Concrete adapter policy
allows only `futures_position` for Kraken Futures, the two replay methods
for Kraken Spot, and `spot_execution_replay` for Walutomat. Paper,
market-data-only, unreviewed, and unknown adapters reject every real method.
For an existing deployment, stop executors, migrate through revision `0026`,
classify each live credential through the administrative PUT endpoint, and
then restart. An omitted classification remains fail-closed as
`unclassified`; the migration never infers or backfills a real method.

### Trading Parameters

| Key | Default | Description |
| --- | ------- | ----------- |
| `instruments` | `{"kraken": ["*"], "kraken_futures": ["*"], "kraken_equities": ["*"], "walutomat": ["*"], "polygon": ["*"]}` | Instruments per exchange (`*` = all instruments) |
| `timeframes` | `["1m"]` | Candle timeframes (list) |
| `risk_max_leverage` | `1.0` | Maximum leverage |
| `risk_r_per_trade` | `0.005` | Risk per trade (0.5%) |

Additional runtime settings are also DB-backed and can be managed
through the Settings API/UI:

| Key | Default | Description |
| --- | ------- | ----------- |
| `ai_integration_enabled` | `true` | Feature gate for `/api/mcp` and `/api/ai-delegates/*`; `false` returns `503 {"error_code":"feature_disabled"}` from `/api/mcp` and hides AI integration in the frontend |
| `feed_egress_enabled` | `false` | Route feed publishers through their own process-local egress pools at startup |
| `kraken_equities_realtime_ws_enabled` | `false` | Use Kraken Equities authenticated realtime market-data WS when token mint succeeds; fallback remains the public delayed feed |
| `kraken_equities_realtime_wallet_public_id` | `""` | Optional wallet pin whose `exchange='kraken'` Spot API key/secret mints Kraken Equities realtime WS tokens: a wallet public id used verbatim, or `label:<wallet-label>` resolved at runtime to the single matching live wallet (fails closed to the delayed feed on zero/multiple matches); empty auto-selects an active Kraken Spot `api_key_secret` wallet |
| `egress_pool` | unset | JSON tunnel-pool definition, validated as `EgressPoolConfig` at startup. An absent, malformed, or validation-failing value disables egress routing with a log line. See [snapper-egress.md](snapper-egress.md) |
| `paper_instruments` | `{"kraken": ["BTC-USD", "EUR-USD"], "kraken_futures": [], "walutomat": ["EUR-PLN", "USD-PLN"]}` | Source exchanges and symbols replayed by paper feeds |
| `backfill_days` | `30` | Default historical backfill window |
| `risk_max_drawdown` | `0.15` | Maximum portfolio drawdown threshold |
| `log_level` | `INFO` | Application log level |
| `log_json` | `false` | Emit structured JSON logs |
| `ws_reconnect_max_delay` | `10` | Maximum WebSocket reconnect delay in seconds |
| `rest_retry_max_attempts` | `5` | REST retry attempt cap |
| `rest_retry_backoff_base` | `0.2` | REST exponential-backoff base delay in seconds |
| `rest_retry_max_delay` | `2.0` | REST retry maximum delay in seconds |
| `rest_circuit_failure_threshold` | `5` | Failures before the REST circuit opens |
| `rest_circuit_reset_timeout` | `30` | Seconds before the REST circuit attempts reset |
| `zmq_heartbeat_interval_ms` | `1000` | ZMQ heartbeat interval |
| `recon_balance_threshold` | `1.0` | Absolute balance mismatch threshold for reconciliation warnings |

The trade runtime always uses the durable (outbox-driven) dispatch
path. The engine writes `TradeCommand` rows to the database; the
`OutboxDispatcher` picks them up and publishes to ZMQ. Stale `CREATED`
create/submit commands older than `TRADE_COMMAND_DISPATCH_TTL_S` (see
[Trade Runtime Safety](#trade-runtime-safety)) are CAS-expired to
terminal `EXPIRED` instead of published — cancels and replaces are
exempt — and the engine releases its in-flight intent via a synthetic
expired event. Executor-side `VenueEvent` writes are fail-closed for
pre-acceptance events — a failed persist raises before the executor
acknowledges the venue event; once the venue has accepted an order, a
failed `order_accepted` persist no longer aborts the flow and the
executor's recon loop retries the durable write each cycle.

### Market Persist Policy

Market-data publishers always publish ZMQ frames, but DB persistence is
controlled by these settings so high-volume feeds can be cache-first:

| Key | Description |
| --- | ----------- |
| `market_persist_ticks` | Persistence mode for tick rows |
| `market_persist_trades` | Persistence mode for trade rows |
| `market_persist_candles` | Persistence mode for candle rows |
| `persist_intermediate_candles` | When `false` (default) only the FINAL bar per native window is persisted (the native finalizer eliminates intra-minute SCD2 churn); `true` also persists in-progress `complete=false` bars, superseded by the final. ZMQ publishes every frame regardless. |
| `spot_trade_built_shadow_enabled` | When `false` (default) Kraken Spot trade-built 1m candles are not persisted; `true` writes them to `shadow_candles` only for native-vs-trade-built A/B checks. |
| `spot_candle_source` | Kraken Spot live 1m source. Default `native` keeps venue OHLC unchanged; explicit `trade_built` switches live 1m bars to the trade stream. Read at feed startup: the live subscription source is fixed when the publisher subscribes, so a change takes effect only after a `snapper-feed` restart (a live settings broadcast refreshes the cached value but does not re-subscribe). |
| `trade_built_finalize_grace_seconds` | Seconds to wait after a trade-built minute closes before finalizing it. Default `12` absorbs late Kraken trades. |
| `candle_forward_fill` | When `false` (default) the higher-TF synthesis flush emits only data-backed bars; `true` forward-fills empty higher-TF windows with a flat carried-close bar. Enable ONLY for continuous-corpus venues (24/7 crypto like Kraken spot); leave off for session-based equities where it would manufacture non-trading-day bars. |
| `candle_single_source` | When `false` (default) the `/api/candles` smart route derives `5m/15m/30m` on-read from the 1m cache; `true` serves those frames from the persisted `candles` plane (Phase 3 read-cutover, closing the cache-vs-persisted dual-source hazard). Keep off until the persisted higher-TF plane is populated and `verify-candle-coverage` passes. `1m` stays cache-served and `1h/4h/1d` are already DB-served. |
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

The JWT signing key and the CSRF signing key are NOT settings: both
are derived from `MASTER_PASSWORD` (purpose tags
`snapper-auth-token-signing-v1` / `snapper-csrf-token-signing-v1`).
Legacy `auth_secret_key` / `csrf_secret_key` rows in the settings table
are ignored and may be deleted.

| Key | Default | Description |
| --- | ------- | ----------- |
| `auth_algorithm` | `HS256` | JWT signing algorithm |
| `auth_access_token_expire_minutes` | `15` | Access token lifetime |
| `auth_refresh_token_expire_days` | `7` | Refresh token lifetime |
| `auth_refresh_token_expire_days_extended` | `30` | Reserved extended refresh token lifetime. `remember_me` is accepted by the login schema but is not currently wired through by `/api/auth/login`, so refresh JWTs use `auth_refresh_token_expire_days` and browser refresh cookies use a fixed 7-day Max-Age |
| `ws_token_ttl_seconds` | `900` | WebSocket token lifetime |
| `csrf_token_expire_minutes` | `60` | CSRF token lifetime |
| `session_secure` | `false` | Require HTTPS for session cookies |
| `session_same_site` | `lax` | Session cookie SameSite mode |
| `session_domain` | `""` | Session cookie domain override |
| `ui_origin` | `""` | Additional allowed UI origins for CORS and WS origin validation |

These settings are ordinary tunables with working defaults; none of
them are placeholders and none are refused at any `SNAPPER_ENV` value.
When `SNAPPER_ENV` is `production`, `prod`, or `staging`, the only
placeholder refused at startup is the development default for
`MASTER_PASSWORD` (checked in
`BootstrapSettingsLoader._validate_production_secret_defaults`), from
which the JWT and CSRF signing keys are derived.

## `.env.example` File

The block below mirrors the variables documented in
`.env.example` at the repo root. The inline comments are
abbreviated here for readability; the canonical version of the
file (with full per-variable rationale and rotation guidance)
lives in `.env.example` itself.

```bash
# Database connection
DB_URL=sqlite+aiosqlite:///./data/snapper.db
SNAPPER_ENV=development

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
TRADE_COMMAND_DISPATCH_TTL_S=30.0
# PAIRED_EXECUTION_GUARD_ENABLED=false
# PAIRED_EXECUTION_ASSEMBLY_TIMEOUT_S=5.0
# PAIRED_EXECUTION_FILL_TIMEOUT_S=30.0

# Observability pipeline (system-metrics snapshotter)
SYSTEM_METRICS_INTERVAL_SECONDS=5
SYSTEM_METRICS_HISTORY_CAP=17280
SYSTEM_METRICS_DISK_FREE_WARN_BYTES=21474836480
SYSTEM_METRICS_DISK_FREE_CRIT_BYTES=10737418240
SYSTEM_METRICS_DISK_MOUNT_PATH=/

# Market-data watchdog (silent-exchange alerting)
MARKET_DATA_WATCHDOG_DISABLED=false
MARKET_DATA_WATCHDOG_INTERVAL_SECONDS=60
MARKET_DATA_WATCHDOG_THRESHOLD_SECONDS=600
# MARKET_DATA_WATCHDOG_EXCHANGE_THRESHOLDS=

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
GET /api/settings?category=api
GET /api/settings?category=api&as_of=2026-01-18T12:00:00Z
```

### Public Feature Flags

```http
GET /api/settings/features
```

This route does not require authentication. It returns the public
feature-flag projection used by the frontend, currently
`ai_integration_enabled`.

### List Categories

```http
GET /api/settings/categories
GET /api/settings/categories?as_of=2026-01-18T12:00:00Z
```

### Manage Push-Beta Gate

```http
GET /api/settings/push-beta/users
POST /api/settings/push-beta/users
X-CSRF-Token: <csrf_token>
Content-Type: application/json

{
    "type": "update_push_beta_users_command",
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "payload": {
        "enabled": true,
        "user_public_ids": ["<user-public-id>"]
    }
}
```

The POST body replaces the full allowlist; it is not a merge.

### Set Setting

```http
POST /api/settings/{key}/set
X-CSRF-Token: <csrf_token>
Content-Type: application/json

{
    "type": "setting_update",
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
    "type": "remove_setting_request",
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 2,
    "timestamp": "2026-01-18T12:00:01Z",
    "payload": {}
}
```

## Environment Configuration

The application always reads `.env` (`SettingsConfigDict(env_file=".env")` on `BootstrapSettingsLoader`); switch environments by editing the values in that file — notably `SNAPPER_ENV` — not by renaming it.

### Development

```bash
# .env — development values
DB_URL=sqlite+aiosqlite:///./data/snapper.db
SNAPPER_ENV=development
SERVER_HOST=127.0.0.1
SERVER_PORT=8000
SERVER_RELOAD=true
SERVER_PROXY_HEADERS=true
SERVER_FORWARDED_ALLOW_IPS=127.0.0.1
```

### Production

```bash
# .env — production values
DB_URL=postgresql+asyncpg://user:pass@db.example.com/snapper
SNAPPER_ENV=production
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
Use `snapper settings-rotate-encryption` to re-encrypt encrypted
settings rows before changing it. Wallet credential payloads use the
same master-password Fernet key but are not rewritten by that command;
rotate or recreate wallet credentials separately before changing
`MASTER_PASSWORD`.

## Configuration Validation

The system validates configuration at startup via the shared `.env`
allowlist check. The encrypt/decrypt round-trip is not a startup step;
it runs as part of the `snapper settings-rotate-encryption` command.

The allowlist is the union of `BootstrapSettingsLoader` aliases and
each subsystem's exported `ENV_VARS` set. Unknown keys fail startup
with suggestions, so typos such as `MASTER_PASWORD` do not silently
fall back to defaults. When adding a new env var, register it on the
owning module's `ENV_VARS` or add it as a bootstrap settings field.
Production-like `SNAPPER_ENV` values additionally reject placeholder
secret material so a typo cannot silently sign tokens or encrypt
settings with development defaults.
