# AI Integration

Snapper exposes a **Model Context Protocol (MCP)** endpoint at
`/api/mcp` so Claude Desktop, Cursor, Windsurf, or a plain `curl` client
can authenticate with a long-lived AI delegate access JWT and call a
narrow set of trading + market-data tools. The surface is
vendor-neutral — no Anthropic or OpenAI SDK is bundled in Snapper
core.

This doc covers:

1. [Feature flag](#feature-flag)
2. [Creating an AI delegate](#creating-an-ai-delegate)
3. [Token model](#token-model)
4. [Client configuration examples](#client-configuration-examples)
5. [Available tools](#available-tools)
6. [Safety caps](#safety-caps)
7. [Kill switch + deactivation](#kill-switch--deactivation)
8. [Rate limits](#rate-limits)
9. [Error catalog](#error-catalog)
10. [Security model](#security-model)

---

## Feature flag

The `/api/mcp` sub-app is always mounted and gated by the
`ai_integration_enabled` database setting.

1. The flag **defaults to `true`** — a fresh install exposes the MCP
    endpoint and the AI Integration navigation entry with no manual
    setup. Operators who need to disable the feature flip the
    setting to `false` via the Settings UI (operator+) or directly:

    ```bash
    curl -X POST http://localhost:8000/api/settings/ai_integration_enabled/set \
      -H "Authorization: Bearer <operator-jwt>" \
      -H "Content-Type: application/json" \
      -d '{"session_id":"cli","sequence_id":1,"public_id":"$(uuidgen)","timestamp":"2026-04-20T00:00:00Z","payload":{"value":"false","category":"system"}}'
    ```

2. The frontend reads `GET /api/settings/features` on mount (no auth
    required) and surfaces the "AI Integration" navigation entry
    whenever the flag is on. The MCP endpoint returns
    `503 feature_disabled` only when the flag is explicitly set to
    `false`.

3. Whether the flag is on or off, the MCP endpoint requires every
    request to carry a valid `Authorization: Bearer <jwt>` header.
    Anonymous requests receive `401 missing_bearer_token`.

---

## Creating an AI delegate

An **AI delegate** is a dedicated `AI_DELEGATE`-role user an operator
mints per MCP client. Delegates:

- Cannot log in via the web UI password form (the placeholder password
    hash is opaque to humans).
- Cannot see other delegates, operators, or wallets — they're scoped
    to the owning operator's visible wallet set.
- Carry per-delegate safety caps independent of the operating
    operator's caps.

Create one via `POST /api/ai-delegates`:

```bash
curl -X POST http://localhost:8000/api/ai-delegates \
  -H "Authorization: Bearer <operator-jwt>" \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: <csrf>" \
  -d '{
    "session_id": "cli",
    "sequence_id": 1,
    "public_id": "'$(uuidgen)'",
    "timestamp": "2026-04-20T00:00:00Z",
    "payload": {
      "label": "Claude Desktop",
      "caps": {
        "max_open_orders": 3,
        "max_daily_notional_usd": 1000.0,
        "max_cancels_per_minute": 10
      }
    }
  }'
```

The response is **one-shot**. Copy the tokens out of the HTTP session
immediately — Snapper never re-serves them:

```json
{
  "type": "delegate_created_response",
  "sequence_id": 1,
  "public_id": "<envelope-uuid7>",
  "timestamp": "2026-04-20T00:00:00Z",
  "session_id": "<server-session>",
  "topic": null,
  "payload": {
    "delegate": {
      "public_id": "019da9e...",
      "username": "ai-claudedesktop-a1b2c3",
      "label": "claudedesktop",
      "created_by_user_public_id": "<operator-id>",
      "created_at": "2026-04-20T00:00:00Z",
      "is_active": true,
      "caps": { "max_open_orders": 3, "max_daily_notional_usd": 1000.0, "max_cancels_per_minute": 10 }
    },
    "access_token": "<jwt-with-10y-exp>",
    "expires_in": 315360000
  }
}
```

Other endpoints on `/api/ai-delegates`:

- `GET /api/ai-delegates` — list the caller's delegates (no
    tokens re-served).
- `GET /api/ai-delegates/{id}` — single delegate detail.
- `PATCH /api/ai-delegates/{id}` — update caps (SCD2 close+insert).
    `label`/`username` are immutable post-mint.
- `POST /api/ai-delegates/{id}/deactivate` — kill switch. Publishes
    `admin.user_deactivated` on the bus; every Snapper instance
    disconnects matching WebSocket sessions and evicts the token from
    every LRU within one bus round-trip.

---

## Token model

Each AI delegate mints a single long-lived (~10-year) access JWT.
The same token authenticates both the proxy MCP server and the
optional push-wakeup watch monitor. Revocation works instantly:
`POST /api/ai-delegates/{id}/deactivate` flips
`users.is_active=False`, publishes `admin.user_deactivated` on the
bus, and every Snapper instance evicts the delegate's
`user_active_tokens` row from the verify-cache within one bus
round-trip. The 10-year `exp` is a ceiling, not a commitment.

---

## Client configuration examples

### Claude Code plugin (recommended, since `snapper-mcp` v0.2.1)

In any Claude Code session:

```text
/plugin marketplace add mateusz-klatt/snapper-mcp
/plugin install snapper-mcp@mateusz-klatt-snapper-mcp
/reload-plugins
```

Claude Code prompts for two required values at install time
(per the plugin's `userConfig` schema in `.claude-plugin/plugin.json`):

- **Snapper API URL** -- your backend's `/api/mcp` endpoint. The
  `@mateusz-klatt/snapper-mcp` bridge accepts the value with or without
  a trailing slash and normalizes it before connecting.
- **Access token** -- the `access_token` from the `delegate_created`
  response (or paste from the Settings -> AI Delegates config-snippet
  generator). Delegates are PAT-style: the JWT lifetime is
  ~10 years (`LONG_LIVED_TOKEN_EXPIRE_DAYS = 3650`), so no
  refresh-token rotation is needed -- revocation is done by
  deactivating the delegate in Snapper, which invalidates the
  associated `user_active_tokens` row server-side.

Run `/mcp list` to confirm the `snapper` server is connected. The
plugin pins the runtime to a specific `@mateusz-klatt/snapper-mcp`
version — the manifest hardcodes the exact version string in
`mcpServers.snapper.args` (currently `@0.11.0`), kept in lockstep
with the plugin's own `version` field by the
`integrations/snapper-mcp/test/plugin_manifest.test.ts` parity
test so a bump to either side fails CI until both match. Future
runtime publishes do not silently upgrade existing installs;
`/plugin update snapper-mcp` opts in. Sensitive values land in the
OS keychain (with `~/.claude/.credentials.json` fallback) -- never
in `settings.json` or the plugin manifest.

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "snapper": {
      "command": "npx",
      "args": ["-y", "@mateusz-klatt/snapper-mcp"],
      "env": {
        "SNAPPER_BASE_URL": "https://snapper.example.com/api/mcp",
        "SNAPPER_ACCESS_TOKEN": "<copied-from-create-response>"
      }
    }
  }
}
```

The `@mateusz-klatt/snapper-mcp` npm wrapper presents the access
token as a Bearer header on every request. On 401 the bridge
surfaces the auth failure to the MCP host once per session;
recovery is via recreating the AI delegate in Snapper.

### Cursor

In `~/.cursor/config.json`:

```json
{
  "mcpServers": {
    "snapper": {
      "url": "https://snapper.example.com/api/mcp/",
      "headers": {
        "Authorization": "Bearer <access-token>"
      }
    }
  }
}
```

The delegate access token Cursor sends as a Bearer header is a
long-lived PAT JWT (~10 years), so day-to-day operation does not
require token rotation. Revocation is by deactivating the delegate
in Snapper (which invalidates the underlying `user_active_tokens`
row); recovery is to recreate the delegate and update Cursor's
`Authorization` header with the new token.

### Windsurf

`~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "snapper": {
      "serverUrl": "https://snapper.example.com/api/mcp/",
      "auth": { "type": "bearer", "token": "<access-token>" }
    }
  }
}
```

### curl (diagnostics)

Quick reachability check:

```bash
# Feature flag
curl http://localhost:8000/api/settings/features

# Hello-world MCP initialise
curl -X POST http://localhost:8000/api/mcp/ \
  -H "Authorization: Bearer <access-token>" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"curl","version":"8"}}}'
```

---

## Available tools

Read-only tools surface the delegate's order book, position state,
and venue market data. Write tools (`submit_manual_order`,
`cancel_order`) are gated by per-tool permissions; only
`submit_manual_order` is unconditionally guarded by
`TradingCapsEnforcer.guard` — `cancel_order` invokes the guard
only on the cancel paths that affect open exposure, and
`submit_ai_review_decision` is not wired to
`caps_enforcer_getter` at registration.

- **`list_instruments(exchange: str)`** — returns sorted instrument
    symbols visible to the delegate's operator for the given exchange.
    Read-only; requires `READ_MARKET_DATA` permission (AI_DELEGATE
    role satisfies).

- **`list_orders(wallet_public_id?, status?, exchange?, instrument?,
    limit=50, offset=0)`** — paged read of the delegate's order
    history within accessible wallets. Requires `READ_ORDERS`.
    Surfaces `order_not_found` for inaccessible wallets
    (anti-enumeration).

- **`get_order_status(command_public_id: str)`** — single-order
    lookup keyed by the trade-command public id returned from
    `submit_manual_order`. Requires `READ_ORDERS`.

- **`list_positions(wallet_public_id?, exchange?, instrument?)`** —
    active positions across the caller's accessible wallets. Requires
    `READ_POSITIONS`.

- **`get_position_cycle(cycle_public_id: str)`** — full open→close
    audit trail for a position cycle. Requires `READ_POSITIONS`.

- **`get_ohlcv(exchange, instrument, timeframe, since?, until?,
    limit=200)`** — OHLCV candles for a venue + instrument. Range
    mode (both `since` + `until` ISO 8601 UTC) returns chronological
    candles in the closed window; latest-as-of mode (both omitted)
    returns the most recent `limit` candles in DESC order. Allowed
    timeframes: `1m`, `5m`, `15m`, `1h`, `4h`, `1d`. Requires
    `READ_MARKET_DATA`; market data is public so no wallet scope
    applies.

- **`list_recent_signals(since, instrument?, strategy?, exchange?,
    wallet_public_id?, limit=50)`** — strategy signals fired after
    a watermark, ordered by `fired_at` DESC. `since` is REQUIRED ISO
    8601 UTC. Requires `READ_SIGNALS`; surfaces `signal_not_found`
    on wallet scope violation (anti-enumeration).

- **`submit_manual_order(exchange, instrument, instrument_public_id,
    side, order_type, quantity, wallet_public_id, idempotency_key,
    price?, operator_public_id?, ai_review_public_id?)`** —
    enqueues a trade command under the delegate's user_public_id
    with `source_surface='mcp'` + the caps check from
    `TradingCapsEnforcer.guard`. Fails closed on any cap violation.
    Wraps the REST `create_order` route.

- **`cancel_order(plan_public_id, idempotency_key)`** — cancels an
    active execution plan via the same `PlansCancelService` REST
    goes through. Idempotent: same `idempotency_key` on retry
    returns the current plan state without re-executing.

- **`submit_ai_review_decision(review_id, decision, rationale?)`** —
    REST mirror of the
    `POST /api/ai-reviews/{review_public_id}/decision` route for
    the in-process MCP surface; lets the delegate approve or
    reject a pending CONSULT review. `review_id` is the UUID7 of
    the `ai_reviews` row. Not gated by the caps enforcer (the
    underlying review-decision path does its own SCD2
    close-and-insert + bus fanout).

The tool catalog is discoverable via the MCP `tools/list` JSON-RPC
method:

```bash
curl -X POST http://localhost:8000/api/mcp/ \
  -H "Authorization: Bearer <access-token>" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
```

---

## Safety caps

Every delegate has its own `user_trading_caps` row. All fields are
optional; `null` means "unbounded on this axis".

- `max_order_quantity_per_instrument` — JSON dict `{instrument:
    max_qty}` OR a scalar applied to every instrument.
- `max_open_orders` — all-time count of the delegate's in-flight
    commands (statuses in `created/dispatched/acked/accepted/
    partially_filled`).
- `max_daily_notional_usd` — rolling 24h sum of `submit_quantity *
    submit_price_usd` across non-rejected commands (submit-time
    commitment basis; partial fills don't change accounting).
- `max_cancels_per_minute` — sliding-window cancel rate.

Caps are enforced at trade-command insert sites via the
`TradingCapsEnforcer.guard()` surface — `submit_manual_order`
routes through it unconditionally; `cancel_order` invokes the
guard only on the cancel paths that affect open exposure (the
bare-cancel fast path skips it). The other MCP tools
(`submit_ai_review_decision`, etc.) are not currently wired to
`caps_enforcer_getter` at registration.

Update caps via `PATCH /api/ai-delegates/{id}` — the write is SCD2
close+insert, so cap history is auditable.

---

## Kill switch + deactivation

`POST /api/ai-delegates/{id}/deactivate` runs the same code path as
operator deactivation:

1. SCD2 close+insert on the `users` row flipping `is_active=False`.
2. `TokenManager.revoke_user_sessions` loads every
    `user_active_tokens.jti` for the delegate, flips `revoked_at=NOW()`
    in one SQL UPDATE, and seeds the in-memory JTI blacklist with a
    10-second grace period.
3. `UserService.deactivate_user` publishes `admin.user_deactivated`
    on the bus (sole publisher).
4. Every Snapper instance's `WebSocketAuthManager` subscribes to the
    topic and closes matching WebSocket connections with code `4003`.
5. Every Snapper instance's `TokenManager` subscribes to the topic
    and evicts matching verify-cache entries via
    `invalidate_user_cache`.

Latency: same-instance kill is immediate (JTI blacklist consulted
before LRU in `verify_token_with_db`); cross-instance kill is bounded
by one bus-message round-trip (sub-second on local ZMQ) instead of
the 30-second LRU TTL.

In-flight MCP tool handlers are **not** force-cancelled. The
guarantee is narrowly "subsequent MCP requests are rejected once
the JTI blacklist propagates" — the in-memory blacklist carries
a small grace period (so requests that raced the propagation
window can still complete) before subsequent verifies fail.

---

## Rate limits

REST rate limits apply globally via `RestCallTracker` — observed via
`GET /api/metrics/rest-rate`. MCP-specific rate limits are enforced
by a separate per-principal middleware on `/api/mcp` (default
`60/minute`); the per-delegate `max_cancels_per_minute` cap then
applies on top inside `cancel_order`. Bursts are served best-effort;
sustained abuse over published exchange limits (Walutomat 20 req/s,
Kraken 15 req/s, Polygon 5/min) surfaces as `rate_limited` warnings
at 80/95% utilisation.

---

## Error catalog

MCP responses use standard HTTP status codes + a Snapper-specific
`error_code` in the JSON body. Clients should branch on `error_code`,
not on status text.

| Status | `error_code`               | When it fires                                    | Client action                                         |
| ------ | -------------------------- | ------------------------------------------------ | ----------------------------------------------------- |
| 503    | `feature_disabled`         | Flag explicitly set to false                     | Show "AI Integration disabled" banner                 |
| 429    | `rate_limit_exceeded`      | Per-principal MCP middleware quota exhausted     | Back off and retry after `Retry-After` seconds        |
| 503    | `mcp_unavailable`          | Repository dep unavailable (lifespan not ready)  | Retry with backoff                                    |
| 401    | `missing_bearer_token`     | No `Authorization: Bearer …` header              | Prompt user to authenticate                           |
| 401    | `invalid_bearer_token`     | JWT signature/expiry/blacklist/inventory failure | AI delegates have no refresh token (10-year PAT); deactivate + recreate the delegate in Snapper, then update the client's bearer token. Operator (cookie) sessions can fall back to `POST /api/auth/refresh`. |
| 401    | `user_deactivated`         | Owner account deactivated                        | Prompt re-login; don't auto-refresh                   |
| 401    | Refresh token redeemed     | Replay of a spent refresh JWT                    | Re-login                                              |
| 401    | Account deactivated        | Session cookie flow                              | Re-login                                              |
| 403    | MCP: `wallet_out_of_scope:` *(prefixed message)* / REST: `"Wallet not in accessible set"` *(detail string)* | Tool targets a wallet outside the caller's scope. The two surfaces emit **different** strings: MCP raises `PermissionError(f"wallet_out_of_scope: ...")` lifted by FastMCP into a `ToolError`; REST raises `HTTPException(403, detail="Wallet not in accessible set")` from `server/scoping.py`. Neither path uses a structured `error_code` JSON field | Pick a wallet the caller still has a live grant on    |
| 403    | MCP: `operator_out_of_scope:` *(prefixed message)* / REST: `"Operator not in accessible set"` *(detail string)* | Same shape as the wallet variant — MCP carries the prefixed `PermissionError` message, REST emits its own English detail string. No structured `error_code` field on either surface | Pick an operator from the caller's authenticated set  |

Delegate CRUD:

| Status | Detail                                 | When it fires                                               |
| ------ | -------------------------------------- | ----------------------------------------------------------- |
| 401    | Requires populated `user_public_id`    | Principal has blank `user_public_id` (legacy token rollout) |
| 403    | `require_role(OPERATOR)`               | AI_DELEGATE or VIEWER trying to manage delegates            |
| 404    | `Delegate not found`                   | Unknown ID OR cross-tenant (no existence leak)              |
| 409    | `Could not derive a unique username …` | Label slug collides 8+ times (pathological)                 |
| 422    | `Operator '<id>' is not in …`          | Caller picked `operator_public_id` outside their claim set  |
| 422    | `Caller has no primary operator …`     | No explicit operator and no primary → binding is ambiguous  |

---

## Security model

### Token lifetime

- **Operator** access tokens live **15 minutes** (configurable via
    `auth_access_token_expire_minutes`). **AI delegate** access
    tokens are PAT-style and live for `LONG_LIVED_TOKEN_EXPIRE_DAYS`
    (~10 years); they are revoked by deactivating the delegate
    rather than by short expiry.
- Refresh tokens live **7 days** (30 days with `remember_me=true`).
- Refresh rotation is **atomic**: one DB transaction revokes the old
    refresh JTI AND persists the new access+refresh pair. Replay of
    a spent refresh JWT fails with 401 `Refresh token already
    redeemed` — the transaction rolls back cleanly.

### Inventory-based revocation

`user_active_tokens` is the ground truth for "is this JWT still
valid?". Every mint persists a row; every verify consults it via a
30-second LRU. Deactivation flips `revoked_at=NOW()` in one UPDATE
and fans out a bus event so every cross-instance LRU evicts the
matching entry on receipt.

### No secrets leaked at rest

Delegate password hashes are bcrypt-derived from 32 bytes of
cryptographically random data the operator never sees. An attacker
with DB read rights cannot brute-force the hash.

### Single-publisher invariant

`UserService.deactivate_user` is the SOLE publisher of
`admin.user_deactivated` across the entire process. `TokenManager`
and `WebSocketAuthManager` are pure subscribers — they evict + close
on receipt but never emit the event themselves. This holds for
operator deactivation AND delegate deactivation (the route reuses
`UserService.deactivate_user`).

### MCP transport = Streamable HTTP

The MCP endpoint uses Model Context Protocol's Streamable HTTP
transport. No cookies, no CSRF on MCP calls (bearer header bypass).
Every tool call opens a sub-request inside the same
bearer-authenticated HTTP connection.

---

## Related reading

- `docs/architecture.md` — repository + SCD2 + bus layering.
- `docs/operations.md` — lifespan order, token cleanup loop, shard
    partitioning.
- `docs/api.md` — full REST surface.
