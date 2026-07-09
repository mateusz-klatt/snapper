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
    setup. Admins who need to disable the feature flip the
    setting to `false` via the Settings UI (admin) or directly:

    ```bash
    curl -X POST http://localhost:8000/api/settings/ai_integration_enabled/set \
      -H "Authorization: Bearer <admin-jwt>" \
      -H "Content-Type: application/json" \
      -d '{"session_id":"cli","sequence_id":1,"public_id":"'$(uuidgen)'","timestamp":"2026-04-20T00:00:00Z","payload":{"value":"false","category":"system"}}'
    ```

    Note: the REST settings write is cached verbatim as a string in the
    serving process; the flag change takes effect only after the API
    server restarts (setting values are parsed to booleans when settings
    are loaded at startup).

2. The feature endpoint itself is public, but the frontend route and
    navigation entry are role/permission-gated. After authentication,
    the AI Integration page reads `GET /api/settings/features` and
    renders the enabled or disabled state. The MCP endpoint and
    `/api/ai-delegates/*` return `503 feature_disabled` only when the
    flag is explicitly set to `false`.

3. When the flag is on, the MCP endpoint requires every request to
    carry a valid `Authorization: Bearer <jwt>` header; anonymous
    requests receive `401 missing_bearer_token`. When the flag is off,
    all requests — anonymous or authenticated — short-circuit to
    `503 feature_disabled` before any token verification runs.

---

## Creating an AI delegate

An **AI delegate** is a dedicated `AI_DELEGATE`-role user an operator
mints per MCP client. Delegates:

- Cannot log in via the web UI password form (the placeholder password
    hash is opaque to humans).
- Cannot see other delegates, operators, or wallets — their wallet scope
    comes from the operator they are bound to.
- Carry per-delegate safety caps independent of the operating
    operator's caps.

Each operator may own at most 5 active delegates (deactivated
delegates do not count toward the cap, so rotation is unbounded).

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
      "operator_public_id": "<operator-public-id>",
      "caps": {
        "max_open_orders": 3,
        "max_daily_notional_usd": 1000.0,
        "max_cancels_per_minute": 10
      }
    }
  }'
```

`payload.operator_public_id` is optional. When omitted, Snapper binds the
delegate to the caller's `primary_operator_public_id`; if the caller has
no primary operator, create returns 422 until an explicit operator is
supplied. When supplied by a non-admin caller, the operator must be in
the caller's authenticated operator claims. Admin callers have the admin
operator bypass and may bind explicitly to any operator. The minted
delegate token inherits scope from that bound operator, so later scope
grant changes for that operator control which wallets/instruments the
delegate can act on.

The response is **one-shot**. Copy the token out of the HTTP session
immediately — Snapper never re-serves it:

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
    "access_token": "<jwt-with-90d-exp>",
    "expires_in": 7776000
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
    `admin.user_deactivated` on the bus for immediate fanout; every
    Snapper instance also polls the DB-backed deactivation registry so
    matching WebSocket sessions close and token LRU entries are evicted
    even if the broker is unavailable.

---

## Token model

Each AI delegate mints a single long-lived (~3-month) access JWT.
The same token authenticates both the proxy MCP server and the
optional push-wakeup watch monitor. Revocation is server-side:
`POST /api/ai-delegates/{id}/deactivate` flips
`users.is_active=False`, revokes the delegate's `user_active_tokens`
row, publishes `admin.user_deactivated` on the bus, and each
Snapper instance evicts matching verify-cache entries on receipt
or through the DB-backed fallback scanner. The local JTI blacklist
uses a 10-second grace window for requests that raced the kill
switch. The 90-day `exp` is a ceiling, not a commitment; operators
are expected to rotate delegate tokens on the cadence that fits
their key-management hygiene.

---

## Client configuration examples

### Claude Code plugin (recommended)

In any Claude Code session:

```text
/plugin marketplace add mateusz-klatt/snapper-mcp
/plugin install snapper-mcp@mateusz-klatt-snapper-mcp
/reload-plugins
```

Claude Code prompts for two required values at install time (per the
plugin's `userConfig` schema in
`integrations/snapper-mcp/.claude-plugin/plugin.json`):

- **Snapper API URL** -- your backend's `/api/mcp` endpoint. The
  `@mateusz-klatt/snapper-mcp` bridge accepts the value with or without
  a trailing slash and normalizes it before connecting.
- **Access token** -- the `access_token` from the `delegate_created`
  response (or paste from the Settings -> AI Delegates config-snippet
  generator). Delegates are PAT-style: the JWT lifetime is
  ~3 months (`LONG_LIVED_TOKEN_EXPIRE_DAYS = 90`); operators are
  expected to rotate by minting a fresh delegate via the same flow
  before the existing one expires. Revocation is immediate:
  deactivating the delegate in Snapper invalidates the associated
  `user_active_tokens` row server-side within one bus round-trip.

Run `/mcp list` to confirm the `snapper` server is connected. The
plugin pins the runtime to a specific `@mateusz-klatt/snapper-mcp`
version — the manifest hardcodes the exact version string in
`mcpServers.snapper.args` (currently `@0.12.0`), kept in lockstep
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
long-lived PAT JWT (~3 months), so day-to-day operation does not
require token rotation more often than the 90-day expiry. Revocation
is by deactivating the delegate
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
signals, and venue market data. Write and decision tools
(`submit_manual_order`, `cancel_order`,
`submit_ai_review_decision`) are gated by per-tool permissions.
Only `submit_manual_order` is unconditionally guarded by
`TradingCapsEnforcer.guard`; `cancel_order` requires the caps enforcer
to be initialized, then the cancel service enters the guard only when
emitting a venue cancel command for a plan with a child venue order.
Plans without a child venue order use the bare cancellation transition
and do not enter the guard. `submit_ai_review_decision` is not wired
to `caps_enforcer_getter` at registration.

- **`list_instruments(exchange: str)`** — returns sorted native symbols
    for the exchange inventory. It is not wallet/operator scoped;
    read-only access is gated by `READ_MARKET_DATA` permission
    (AI_DELEGATE role satisfies).

- **`list_orders(wallet_public_id?, status?, exchange?, instrument?,
    limit=50, offset=0)`** — paged read of the delegate's order
    history within accessible wallets. Requires `READ_ORDERS`.
    Surfaces `order_not_found` for inaccessible wallets
    (anti-enumeration).

- **`get_order_status(command_public_id: str)`** — single-order
    lookup keyed by the trade-command public id returned from
    `submit_manual_order`. Requires `READ_ORDERS`; unknown or
    out-of-scope command ids return `order_not_found`
    (anti-enumeration).

- **`list_positions(wallet_public_id?, exchange?, instrument?)`** —
    active positions across the caller's accessible wallets. Requires
    `READ_POSITIONS`; wallet scope violations return
    `position_not_found` (anti-enumeration).

- **`get_position_cycle(cycle_public_id: str)`** — full open→close
    audit trail for a position cycle. Requires `READ_POSITIONS`;
    unknown or out-of-scope cycles return `position_cycle_not_found`
    (anti-enumeration).

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
    side, order_type, quantity, idempotency_key, wallet_public_id?,
    price?, stop_price?, operator_public_id?, ai_review_public_id?)`** —
    enqueues a trade command under the delegate's user_public_id
    with `source_surface='mcp'` + the caps check from
    `TradingCapsEnforcer.guard`. Requires `CREATE_ORDERS`, rejects
    non-admin `operator_public_id` values outside the caller's
    authenticated operator set, rechecks wallet scope against active
    grants on every call, and fails closed on any cap violation.
    Validates order params with the same evaluator rule as REST:
    `price` is required for `limit`/`stop_limit` and `stop_price` is
    required for `stop`/`stop_limit` order types.
    `wallet_public_id` is optional — when omitted, Snapper resolves the
    caller's single accessible live wallet; multiple candidates return
    a structured `wallet_ambiguous` envelope and zero candidates
    `wallet_unresolved` (a blank string returns `invalid_argument`).
    Wraps the REST `create_order` route.

- **`cancel_order(plan_public_id, idempotency_key)`** — cancels an
    active execution plan via the same `PlansCancelService` REST
    goes through. Idempotent: same `idempotency_key` on retry
    returns the current plan state without re-executing. Requires
    `CANCEL_ORDERS`; unknown and out-of-scope plans collapse to
    `order_not_found` (anti-enumeration).

- **`submit_ai_review_decision(review_id, decision, rationale?)`** —
    REST mirror of the
    `POST /api/ai-reviews/{review_public_id}/decision` route for
    the in-process MCP surface; lets the delegate approve or
    reject a pending CONSULT review. `review_id` is the UUID7 of
    the `ai_reviews` row. Requires `CREATE_ORDERS`; the review
    service additionally verifies the caller is a registered AI
    delegate that still holds an active scope grant for the
    review wallet and instrument. Any such delegate may decide —
    the selected delegate is who was consulted, not an exclusive
    decision authority — and a delegate racing an already-resolved
    review receives `review_already_resolved_by_peer`. Not gated
    by the caps enforcer (the underlying review-decision path does
    its own SCD2 close-and-insert + bus fanout).

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

## CONSULT wake path (runbook)

A strategy issues a CONSULT round via the `create_ai_review_and_await`
primitive; the service admits it only when an eligible delegate's
`ai_delegates.last_seen_at` is inside the heartbeat window (default
15s). Liveness is maintained by the delegate's WebSocket session: the
connect handshake bumps `last_seen_at`, and every client ping re-bumps
it (throttled server-side to at most one write per 5s per delegate).
The bundled `HeartbeatConsult` strategy exercises the full loop with
one consult per 1h candle and, on approval, a paper signal at the
configurable `heartbeat_signal_strength` param (default `0.0` =
target-flat, opening no position; a value in `[0.0, 1.0]` opens an
actionable paper long so the signal→order→fill→position plane is
exercised — still paper-only by construction). Its process config must
supply UUID7 `ai_review_user_public_id` and `ai_review_strategy_public_id`
params plus a scoped wallet/operator pair whose grant covers the output
instrument.

Operator steps to arm the wake surface:

1. Mint a delegate (`POST /api/ai-delegates`) and grant it scope on
    the strategy's `(wallet, instrument)` pair.
2. Run `snapper-mcp watch` with the delegate token. The default
    subscription already includes the `ai_reviews.` family; each
    `ai_review.request` frame appears as one JSONL line on stdout.
3. Wire the watch stdout into the monitoring host (for example a
    Claude Code monitor primitive) so a request frame wakes the
    delegate's session. This host-side wiring lives outside this
    repository by design — any host able to read a subprocess's
    stdout can implement it.
4. The woken delegate reads the frame's `review_public_id` and passes
    it as the `review_id` argument of the `submit_ai_review_decision`
    MCP tool before the review deadline (default 25s for
    `HeartbeatConsult`); the strategy resumes with the outcome, and a
    `decision_ack` frame follows on the same `ai_reviews.` family.

After a watch reconnect, `GET /api/ai-reviews/pending` is the
catch-up read for reviews whose fanout window (`fanout_after`,
default creation + 30s) has already opened while the delegate was
offline. Short-deadline rounds such as `HeartbeatConsult` (25s) time
out before that window opens, so a missed heartbeat frame is simply
lost and the next 1h round retries — the catch-up read matters for
strategies configured with deadlines longer than the fanout window.

Operators and admins audit what the AI decided across the whole book
via `GET /api/ai-reviews` (OPERATOR-gated; an ADMIN sees every
operator's reviews, a non-admin OPERATOR is narrowed server-side to its
own operators). It returns `AdminAiReviewListResponse` (`items` +
`count`) with the full per-row outcome — `status`, `decision`,
`rationale`, `resolution_mode`, and the responding delegate — and
accepts optional exact-match `status`, `wallet_public_id`, and
`strategy_public_id` filters plus a `limit` (1-500, default 100),
newest first. Unlike `/pending` it is not keyed by the delegate
identity and returns terminal decided rows, not only pending ones.

---

## Safety caps

Every delegate has its own `user_trading_caps` row. All fields are
optional; `null` means "unbounded on this axis".

- `max_order_quantity_per_instrument` — JSON dict
    `{instrument_public_id: max_qty}` keyed by the Snapper instrument
    UUID7 (NOT the native venue symbol). The delegate API accepts only
    a JSON dict or `null`; a scalar value stored directly on the caps
    row (legacy/manual writes) is applied to every instrument by the
    enforcer, but cannot be set via `POST`/`PATCH /api/ai-delegates`.
- `max_open_orders` — all-time count of the delegate's in-flight
    commands (every non-terminal status: `created/dispatched/
    direct_dispatched/accepted/partially_filled`).
- `max_daily_notional_usd` — rolling 24h sum of `submit_quantity *
    submit_price` per prior non-rejected command (raw submit-time
    price, no USD re-conversion; prior market orders with no submit
    price are skipped with a WARN log), plus the new submission's
    USD-converted notional via the USD price oracle
    (`price_unavailable` caps violation when the oracle is stale or
    missing). Submit-time commitment basis; partial fills don't
    change accounting.
- `max_cancels_per_minute` — sliding-window cancel rate.

Caps are enforced at trade-command insert sites via the
`TradingCapsEnforcer.guard()` surface — `submit_manual_order`
routes through it unconditionally; `cancel_order` requires the caps
enforcer to be initialized and only enters the guard when the cancel
service emits a venue cancel command for a plan with a child venue order
(the bare cancellation transition skips the guard). The other MCP tools
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
    on the bus as the immediate fanout path (sole publisher).
4. Every Snapper instance's `WebSocketAuthManager` subscribes to the
    topic and closes matching WebSocket connections with code `4003`.
5. Every Snapper instance's `TokenManager` subscribes to the topic
    and evicts matching verify-cache entries via
    `invalidate_user_cache`.
6. Every lifespan-wired `WebSocketAuthManager` and `TokenManager`
    also runs a DB-backed fallback scan against the SCD2-active
    `users.is_active` row, so broker outages delay fanout by the
    5-second scan interval (`DEACTIVATION_FALLBACK_SCAN_INTERVAL_S`)
    rather than by token expiry or reconnect.

Latency: the token inventory is revoked before the deactivated-user
bus event is published. Existing positive verify-cache entries are
evicted when each instance receives `admin.user_deactivated`; if the
bus listener is unavailable, the fallback scanner evicts cache entries
and closes matching WebSockets by reading the committed
`users.is_active=False` row. The local JTI blacklist is consulted
before the LRU but has a 10-second grace window, so requests that race
deactivation can still complete.

In-flight MCP tool handlers are **not** force-cancelled. The
guarantee is narrowly "later MCP requests are rejected once the
inventory/cache revocation path has observed the kill switch."

---

## Rate limits

REST rate limits apply globally via `RestCallTracker` — observed via
`GET /api/metrics/rest-rate`. MCP-specific rate limits are enforced
by a separate per-principal middleware on `/api/mcp` (default
`60/minute`); the per-delegate `max_cancels_per_minute` cap then
applies on top inside `cancel_order`. Bursts are served best-effort;
sustained abuse over published exchange limits (Walutomat 20 req/s,
Kraken 15 req/s, Polygon 5/min) surfaces as
`REST utilization …% of limit` log entries — WARNING at 80% and ERROR
at 95% utilisation, rate-limited to one log per exchange per 60 s.

---

## Error catalog

Transport and middleware failures on `/api/mcp` use standard HTTP status
codes plus a Snapper-specific `error_code` in the JSON body. Clients
should branch on `error_code`, not on status text.

Envelope-based MCP tools return a normal MCP `CallToolResult` instead:
the single `TextContent.text` entry contains JSON with `success`,
`error_code`, `message`, and `details`, and `CallToolResult.isError`
mirrors `not success`. A few older tools still return raw dictionaries
on success or surface FastMCP `ToolError`/permission exceptions on
failure; client code should handle both shapes until those tools are
migrated.

| Status | `error_code`               | When it fires                                    | Client action                                         |
| ------ | -------------------------- | ------------------------------------------------ | ----------------------------------------------------- |
| 503    | `feature_disabled`         | Flag explicitly set to false                     | Show "AI Integration disabled" banner                 |
| 429    | `rate_limit_exceeded`      | Per-principal MCP middleware quota exhausted     | Back off and retry after `Retry-After` seconds        |
| 503    | `mcp_unavailable`          | Repository dep unavailable (lifespan not ready)  | Retry with backoff                                    |
| 401    | `missing_bearer_token`     | No `Authorization: Bearer …` header              | Prompt user to authenticate                           |
| 401    | `invalid_bearer_token`     | JWT signature/expiry/blacklist/inventory failure | AI delegates have no refresh token (90-day PAT); deactivate + recreate the delegate in Snapper, then update the client's bearer token. Operator (cookie) sessions can fall back to `POST /api/auth/refresh`. |
| 401    | `user_deactivated`         | Owner account deactivated                        | Prompt re-login; don't auto-refresh                   |
| 401    | Refresh token redeemed     | Replay of a spent refresh JWT                    | Re-login                                              |
| 401    | Authentication required *(detail string, no error_code)* | Session cookie flow — deactivated account, revoked session, or expired token fails DB-backed verify; require_authentication collapses all of these into one generic 401 | Re-login |
| 403    | MCP write helpers: `wallet_out_of_scope:` *(prefixed message)* / REST: `"Wallet not in accessible set"` *(detail string)* | Mutating tool targets a wallet outside the caller's scope. The helper path raises `PermissionError(f"wallet_out_of_scope: ...")` lifted by FastMCP into a `ToolError`; REST raises `HTTPException(403, detail="Wallet not in accessible set")` from `server/scoping.py`. Tool-level read/cancel paths may instead return structured anti-enumeration envelopes such as `order_not_found`, `position_not_found`, or `signal_not_found`. | Pick a wallet the caller still has a live grant on    |
| 403    | MCP write helpers: `operator_out_of_scope:` *(prefixed message)* / REST: `"Operator not in accessible set"` *(detail string)* | Same write-helper shape as the wallet variant. Non-admin callers must pick an operator from their authenticated set; ADMIN bypasses the operator-set check. Read/cancel tools can intentionally collapse out-of-scope and not-found cases into structured not-found envelopes to avoid leaking resource existence. | Pick an operator from the caller's authenticated set unless the caller is ADMIN |

Delegate CRUD:

| Status | Detail                                 | When it fires                                               |
| ------ | -------------------------------------- | ----------------------------------------------------------- |
| 401    | Requires populated `user_public_id`    | Principal has blank `user_public_id` (older token rollout)  |
| 403    | `require_role(OPERATOR)`               | AI_DELEGATE or VIEWER trying to manage delegates            |
| 404    | `Delegate not found`                   | Unknown ID OR cross-tenant (no existence leak)              |
| 409    | `Could not derive a unique username …` | Label slug collides 8+ times (pathological)                 |
| 409    | `Operator … already owns N active AI delegates (limit 5)` | Owner hit the 5-active-delegates-per-operator cap; deactivate an existing delegate before creating another |
| 422    | `Operator '<id>' is not in …`          | Non-admin caller picked `operator_public_id` outside their claim set |
| 422    | `Caller has no primary operator …`     | No explicit operator and no primary → binding is ambiguous  |

---

## Security model

### Token lifetime

- **Operator** access tokens live **15 minutes** (configurable via
    `auth_access_token_expire_minutes`). **AI delegate** access
    tokens are PAT-style and live for `LONG_LIVED_TOKEN_EXPIRE_DAYS`
    (~3 months / 90 days); operators rotate them on the cadence that
    fits their key-management hygiene, and revocation is immediate via
    deactivating the delegate rather than waiting for expiry.
- Refresh tokens on the login/refresh route path currently live
    `auth_refresh_token_expire_days` (default **7 days**). The
    `remember_me` request field is accepted by the schema but is not
    currently threaded into token creation.
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
and `WebSocketAuthManager` are pure subscribers/fallback readers: they
evict + close on receipt or on DB scan, but never emit the event
themselves. This holds for operator deactivation AND delegate
deactivation (the route reuses `UserService.deactivate_user`).

### MCP transport = Streamable HTTP

The MCP endpoint uses Model Context Protocol's Streamable HTTP
transport. No cookies, no CSRF on MCP calls (bearer header bypass).
Every tool call opens a sub-request inside the same
bearer-authenticated HTTP connection.

---

## Related reading

- `docs/architecture.md` — repository + SCD2 + bus layering.
- `docs/operations.md` — multi-instance trade coordinator and
    static-hash shard partitioning.
- `docs/api.md` — full REST surface.
