# AI Integration

Snapper exposes a **Model Context Protocol (MCP)** endpoint at
`/api/mcp` so Claude Desktop, Cursor, Windsurf, or a plain `curl` client
can authenticate with a **scoped bearer token pair** and call a narrow
set of trading + market-data tools. The surface is vendor-neutral — no
Anthropic or OpenAI SDK is bundled in Snapper core. Phase A (this
document) ships the server-side surface; Phase B adds an npm wrapper
for Claude Desktop, Phase C adds a first-class Claude Code channel.

This doc covers:

1. [Feature flag](#feature-flag)
2. [Creating an AI delegate](#creating-an-ai-delegate)
3. [Token types: rotating vs long-lived](#token-types-rotating-vs-long-lived)
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
  "payload": {
    "delegate": {
      "public_id": "019da9e...",
      "username": "ai-claudedesktop-a1b2c3",
      "label": "claudedesktop",
      "created_by_user_public_id": "<operator-id>",
      "created_at": "2026-04-20T00:00:00Z",
      "is_active": true,
      "caps": { "max_open_orders": 3, "max_daily_notional_usd": 1000.0, ... },
      "token_kind": "rotating"
    },
    "access_token": "<jwt>",
    "refresh_token": "<jwt>",
    "expires_in": 900,
    "token_kind": "rotating"
  }
}
```

Other endpoints on `/api/ai-delegates`:

- `GET /api/ai-delegates` — list the caller's delegates (no
    tokens re-served). Each list item carries `token_kind` so the
    UI can render a PAT badge without a follow-up fetch.
- `GET /api/ai-delegates/{id}` — single delegate detail.
- `PATCH /api/ai-delegates/{id}` — update caps (SCD2 close+insert).
    `label`/`username` are immutable post-mint. `token_kind` is
    derived from the live token inventory and cannot be changed
    post-creation — operators who need to switch between rotating
    and long-lived must deactivate + recreate the delegate.
- `POST /api/ai-delegates/{id}/deactivate` — kill switch. Publishes
    `admin.user_deactivated` on the bus; every Snapper instance
    disconnects matching WebSocket sessions and evicts the token from
    every LRU within one bus round-trip. Works identically on
    rotating and long-lived delegates.

---

## Token types: rotating vs long-lived

At delegate-creation time the operator picks one of two token modes
via the `long_lived: bool` field on `DelegateCreateBody` (default
`false`). The choice is permanent for that delegate — rotate by
deactivating + recreating with the other setting.

### Rotating (default)

- 15-minute access-token TTL + 7-day refresh-token TTL (30 days with
    `remember_me`, not exposed to delegate creation today).
- MCP bridge holds both JWTs in-memory; on 401 `invalid_bearer_token`
    the bridge calls `POST /api/auth/refresh?return_tokens=true`
    with the refresh JWT and updates the in-memory pair.
- Single-flight rotation — N concurrent 401s share ONE refresh call.
- **Recommended for** remote / shared-host deployments where the
    short-lived access token cap is a meaningful risk reducer.

### Long-lived PAT (opt-in)

- Single access-token JWT with a ~10-year `exp`, no refresh token.
    Response carries `access_token` + `refresh_token: null` +
    `token_kind: "long_lived"`.
- MCP bridge never calls `/api/auth/refresh`. On 401
    `invalid_bearer_token` the 401 surfaces verbatim to the MCP host
    with a stderr hint pointing the operator at the Snapper UI to
    regenerate the delegate.
- **Recommended for** local `localhost` MCP clients where the
    refresh-token dance adds operator friction without security
    benefit (single-operator, single-machine, trust boundary is the
    machine itself).
- **Requires** `@mateusz-klatt/snapper-mcp` bridge **v0.2.0 or
    newer** — v0.1.0 treated `SNAPPER_REFRESH_TOKEN` as required and
    refuses to start without it.

### Kill switch parity

Both modes revoke via the same path: `POST /api/ai-delegates/{id}/deactivate`
flips `users.is_active=False`, publishes `admin.user_deactivated` on
the bus, and every Snapper instance evicts the delegate's
`user_active_tokens` row(s) from the verify-cache within one
round-trip. The 10-year PAT expiry is a ceiling, not a commitment —
revocation takes effect instantly.

### Example: creating a PAT delegate

```bash
curl -X POST http://localhost:8000/api/ai-delegates \
  -H "Authorization: Bearer <operator-jwt>" \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: <csrf>" \
  -d '{
    "session_id": "cli",
    "sequence_id": 1,
    "public_id": "'$(uuidgen)'",
    "timestamp": "2026-04-24T00:00:00Z",
    "payload": {
      "label": "Local Claude",
      "long_lived": true,
      "caps": { "max_open_orders": 3, "max_daily_notional_usd": 1000.0 }
    }
  }'
```

Response:

```json
{
  "type": "delegate_created_response",
  "payload": {
    "delegate": { "...": "...", "token_kind": "long_lived" },
    "access_token": "<jwt-with-10y-exp>",
    "refresh_token": null,
    "expires_in": 315360000,
    "token_kind": "long_lived"
  }
}
```

Paste only `access_token` as `SNAPPER_ACCESS_TOKEN` into your MCP
client config; leave `SNAPPER_REFRESH_TOKEN` unset. The Settings UI
snippet generator emits the correct env block automatically.

---

## Client configuration examples

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "snapper": {
      "command": "npx",
      "args": ["-y", "snapper-mcp"],
      "env": {
        "SNAPPER_MCP_BASE_URL": "https://snapper.example.com/api/mcp",
        "SNAPPER_ACCESS_TOKEN": "<copied-from-create-response>",
        "SNAPPER_REFRESH_TOKEN": "<copied-from-create-response>"
      }
    }
  }
}
```

The `snapper-mcp` npm wrapper (Phase B) handles access-token refresh
via `POST /api/auth/refresh` on 401 so the agent session doesn't
interrupt mid-conversation.

### Cursor

In `~/.cursor/config.json`:

```json
{
  "mcpServers": {
    "snapper": {
      "url": "https://snapper.example.com/api/mcp",
      "headers": {
        "Authorization": "Bearer <access-token>"
      }
    }
  }
}
```

Cursor currently requires manual token rotation when the access
token expires (15 minutes by default). Use the refresh token via
`POST /api/auth/refresh?return_tokens=true` with
`Authorization: Bearer <refresh-jwt>` to mint a fresh pair.

### Windsurf

`~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "snapper": {
      "serverUrl": "https://snapper.example.com/api/mcp",
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
curl -X POST http://localhost:8000/api/mcp \
  -H "Authorization: Bearer <access-token>" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"curl","version":"8"}}}'
```

---

## Available tools

Phase A ships two tools (expand in Phase B/C):

- **`list_instruments(exchange: str)`** — returns sorted instrument
    symbols visible to the delegate's operator for the given exchange.
    Read-only; requires `READ_MARKET_DATA` permission (AI_DELEGATE
    role satisfies).

- **`submit_manual_order(exchange, instrument, side, quantity,
    price?, mode?)`** — enqueues a trade command under the
    delegate's user_public_id with `source_surface='mcp'` + the
    caps check from `TradingCapsEnforcer.guard`. Fails closed on
    any cap violation.

The tool catalog is discoverable via the MCP `tools/list` JSON-RPC
method:

```bash
curl -X POST http://localhost:8000/api/mcp \
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

Caps are enforced at every trade-command insert site via the
`TradingCapsEnforcer.guard()` surface. MCP `submit_manual_order` is
the only tool in Phase A that triggers them.

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
    on the bus (sole publisher per plan §3.6.1).
4. Every Snapper instance's `WebSocketAuthManager` subscribes to the
    topic and closes matching WebSocket connections with code `4003`.
5. Every Snapper instance's `TokenManager` subscribes to the topic
    and evicts matching verify-cache entries via
    `invalidate_user_cache`.

Latency: same-instance kill is immediate (JTI blacklist consulted
before LRU in `verify_token_with_db`); cross-instance kill is bounded
by one bus-message round-trip (sub-second on local ZMQ) instead of
the 30-second LRU TTL.

In-flight MCP tool handlers are **not** force-cancelled. The plan §2
item 6 guarantee is narrowly "the NEXT MCP request is rejected".

---

## Rate limits

REST rate limits apply globally via `RestCallTracker` — observed via
`GET /api/metrics/rest-rate`. MCP-specific rate limits live on top of
the per-delegate `max_cancels_per_minute` cap. Bursts are served best-
effort; sustained abuse over published exchange limits (Walutomat 20
req/s, Kraken 15 req/s, Polygon 5/min) surfaces as `rate_limited`
warnings at 80/95% utilisation.

---

## Error catalog

MCP responses use standard HTTP status codes + a Snapper-specific
`error_code` in the JSON body. Clients should branch on `error_code`,
not on status text.

| Status | `error_code`               | When it fires                                    | Client action                                         |
| ------ | -------------------------- | ------------------------------------------------ | ----------------------------------------------------- |
| 503    | `feature_disabled`         | Flag off OR settings service not ready           | Show "AI Integration disabled" banner                 |
| 503    | `mcp_unavailable`          | Repository dep unavailable (lifespan not ready)  | Retry with backoff                                    |
| 401    | `missing_bearer_token`     | No `Authorization: Bearer …` header              | Prompt user to authenticate                           |
| 401    | `invalid_bearer_token`     | JWT signature/expiry/blacklist/inventory failure | Call `POST /api/auth/refresh`; fail → re-login        |
| 401    | `user_deactivated`         | Owner account deactivated (plan §2 item 6)       | Prompt re-login; don't auto-refresh                   |
| 401    | Refresh token redeemed     | Replay of a spent refresh JWT                    | Re-login                                              |
| 401    | Account deactivated        | Session cookie flow                              | Re-login                                              |
| 403    | `wallet_out_of_scope`      | Tool targets a wallet outside the caller's scope | Pick a wallet the caller still has a live grant on    |
| 403    | `operator_out_of_scope`    | Tool targets an operator not in the caller's JWT | Pick an operator from the caller's authenticated set  |

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

- Access tokens live **15 minutes** (configurable via
    `auth_access_token_expire_minutes`).
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

### Single-publisher invariant (§3.6.1)

`UserService.deactivate_user` is the SOLE publisher of
`admin.user_deactivated` across the entire process. `TokenManager`
and `WebSocketAuthManager` are pure subscribers — they evict + close
on receipt but never emit the event themselves. This holds for
operator deactivation AND delegate deactivation (the route reuses
`UserService.deactivate_user`).

### MCP transport = Streamable HTTP

Per plan §3.2 the MCP endpoint uses Model Context Protocol's
Streamable HTTP transport. No cookies, no CSRF on MCP calls (bearer
header bypass per plan §3.7 item 4). Every tool call opens a
sub-request inside the same bearer-authenticated HTTP connection.

---

## Related reading

- `plan_ai_integration_phase_a.md` — the shipping plan (multi-model
    APPROVED at R6).
- `docs/architecture.md` — repository + SCD2 + bus layering.
- `docs/operations.md` — lifespan order, token cleanup loop, shard
    partitioning.
- `docs/api.md` — full REST surface.
