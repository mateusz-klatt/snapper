# AI Integration

Snapper exposes a **Model Context Protocol (MCP)** endpoint at
`/api/mcp` so Claude Desktop, Cursor, Windsurf, or a plain `curl` client
can authenticate with a long-lived AI delegate or researcher access JWT
and call a permission-filtered set of trading and market-data tools. The surface is
vendor-neutral — no Anthropic or OpenAI SDK is bundled in Snapper
core.

This doc covers:

1. [Feature flag](#feature-flag)
2. [Creating an AI delegate](#creating-an-ai-delegate)
3. [Creating an AI researcher](#creating-an-ai-researcher)
4. [Token model](#token-model)
5. [Client configuration examples](#client-configuration-examples)
6. [Available tools](#available-tools)
7. [Safety caps](#safety-caps)
8. [Kill switch + deactivation](#kill-switch--deactivation)
9. [Rate limits](#rate-limits)
10. [Error catalog](#error-catalog)
11. [Security model](#security-model)

---

## Feature flag

The `/api/mcp` sub-app is always mounted and gated by the
`ai_integration_enabled` database setting.

1. The flag **defaults to `true`** — a fresh install exposes the MCP
    endpoint and the AI Integration navigation entry with no manual
    setup. A caller with `configure:system` can disable the feature through
    the Settings UI or directly:

    ```bash
    curl -X POST http://localhost:8000/api/settings/ai_integration_enabled/set \
      -H "Authorization: Bearer <settings-manager-jwt>" \
      -H "Content-Type: application/json" \
      -d '{"session_id":"cli","sequence_id":1,"public_id":"'$(uuidgen)'","timestamp":"2026-04-20T00:00:00Z","payload":{"value":"false","category":"system"}}'
    ```

    Note: the REST settings write is cached verbatim as a string in the
    serving process; the flag change takes effect only after the API
    server restarts (setting values are parsed to booleans when settings
    are loaded at startup).

2. The feature endpoint itself is public, but the frontend route and
    navigation entry require `read:ai_integration`. The page derives all
    management controls separately from `manage:ai_integration`, so the
    current `viewer` set can inspect its operator-scoped integration state
    without gaining a mutation. After authentication, the page reads
    `GET /api/settings/features` and renders the enabled or disabled state.
    The MCP endpoint, `/api/ai-delegates/*`, and `/api/ai-researchers`
    return `503 feature_disabled` only when the flag is explicitly set to
    `false`.

3. When the flag is on, the MCP endpoint requires every request to
    carry a valid `Authorization: Bearer <jwt>` header; anonymous
    requests receive `401 missing_bearer_token`. When the flag is off,
    all requests — anonymous or authenticated — short-circuit to
    `503 feature_disabled` before any token verification runs.

Authorization uses each token's effective grant from Snapper's 34-permission
catalogue, not a role hierarchy. Roles are named permission sets. The current
`viewer` set includes `read:ai_integration` and `read:ai_reviews`, uses its
operator memberships for read-only integration visibility, and contains no
AI-integration management permission. A token with `manage:ai_integration`
instead receives the creator-owned delegate view. Only an effective
`impersonate:operator` grant provides global operator scope on surfaces that
support global scoping.

---

## Creating an AI delegate

An **AI delegate** is a dedicated user assigned the `ai_delegate` named
permission set and minted per MCP client by a caller with
`manage:ai_integration`. Delegates:

- Cannot log in via the web UI password form (the placeholder password
    hash is opaque to humans).
- Cannot see other delegates, operators, or wallets — their wallet scope
    comes from the operator they are bound to.
- Carry per-delegate safety caps independent of the operating
    operator's caps.

Each integration owner may own at most 5 active delegates (deactivated
delegates do not count toward the cap, so rotation is unbounded).

Create one via `POST /api/ai-delegates`:

```bash
curl -X POST http://localhost:8000/api/ai-delegates \
  -H "Authorization: Bearer <integration-manager-jwt>" \
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
      "permissions": [
        "read:market_data",
        "read:orders",
        "read:positions",
        "read:strategies",
        "read:signals",
        "read:system_status",
        "read:backtests"
      ],
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
supplied. When the caller's effective grant lacks `impersonate:operator`, a
supplied operator must be in the caller's authenticated operator claims. An
effective grant containing `impersonate:operator` may bind explicitly to any
operator. The minted delegate token inherits scope from that bound operator,
so later scope grant changes for that operator control which wallets and
instruments the delegate can act on.

`payload.permissions` independently downscopes what that credential may do.
The read-only monitoring example above omits order creation, cancellation, and
position management even though the `ai_delegate` named set contains them.
The server enforces the intersection of the named set and token grants and
returns 422 if the request includes a permission outside that set. Omit
`permissions` to retain the complete named-set grant.

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
      "created_by_user_public_id": "<creator-user-id>",
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

- `GET /api/ai-delegates` — requires `read:ai_integration`; a management
    token sees creator-owned delegates, while a read-only token sees delegates
    bound to its operator memberships. Tokens are never re-served.
- `GET /api/ai-delegates/{id}` — requires `read:ai_integration` and applies
    the same creator-owned or membership-scoped view.
- `PATCH /api/ai-delegates/{id}` — update caps (SCD2 close+insert).
    Requires `manage:ai_integration`; `label` and `username` are immutable
    post-mint.
- `POST /api/ai-delegates/{id}/deactivate` — kill switch. Publishes
    `admin.user_deactivated` on the bus for immediate fanout. Requires
    `manage:ai_integration`; every Snapper instance also polls the DB-backed
    deactivation registry so matching WebSocket sessions close and token LRU
    entries are evicted even if the broker is unavailable.

---

## Creating an AI researcher

An **AI researcher** is a dedicated principal assigned the `ai_researcher`
named permission set for contexts that ingest hostile third-party material.
That set is exactly:

- `read:market_data`
- `read:market_views`
- `submit:market_view`

It does not receive signal, order, position, or system-status permissions.
Consequently it cannot subscribe to `ai_reviews.` or call the order-,
position-, signal-, and review-shaped MCP tools. It can subscribe to the
`ai_research.` wake root when its token retains `submit:market_view`. Periodic
wakes arrive on `ai_research.{round_public_id}.request` with an
`ai_research.request` frame carrying `round_public_id` and `trigger`; the round
is durable even when the auxiliary wake is lost. The default
`snapper-mcp watch` topic set includes this research root.

Create one via `POST /api/ai-researchers` with a
`ResearcherCreateRequest` envelope:

```json
{
  "type": "researcher_create_request",
  "sequence_id": 1,
  "public_id": "<client-uuid7>",
  "timestamp": "2026-07-21T00:00:00Z",
  "session_id": "cli",
  "payload": {
    "label": "Macro Research",
    "permissions": ["read:market_data", "submit:market_view"]
  }
}
```

The response returns the researcher projection and its long-lived access token
once. Each owner may have at most two active researchers, independently of the
five-delegate cap. Provisioning persists only `users` and
`user_active_tokens` rows: no `user_trading_caps`, operator membership, or
`ai_delegates` row is created, so researchers never enter consult admission
accounting.

---

## Token model

Each AI delegate or researcher mints a single long-lived (~3-month) access JWT.
The same token authenticates both the proxy MCP server and the
optional push-wakeup watch monitor. Delegate revocation is server-side:
`POST /api/ai-delegates/{id}/deactivate` flips
`users.is_active=False`, revokes the delegate's `user_active_tokens`
row, publishes `admin.user_deactivated` on the bus, and each
Snapper instance evicts matching verify-cache entries on receipt
or through the DB-backed fallback scanner. The local JTI blacklist
uses a 10-second grace window for requests that raced the kill
switch. The 90-day `exp` is a ceiling, not a commitment; integration owners
are expected to rotate delegate tokens on the cadence that fits
their key-management hygiene.

Researcher tokens use the same active-token inventory and shared user
deactivation enforcement. The dedicated researcher surface currently provisions
principals only; a caller with `manage:users` uses the standard user-deactivation
route to revoke one.

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
`submit_ai_review_decision`, `submit_market_view`) are gated by
per-tool permissions.
Only `submit_manual_order` is unconditionally guarded by
`TradingCapsEnforcer.guard`; `cancel_order` requires the caps enforcer
to be initialized, then the cancel service enters the guard only when
emitting a venue cancel command for a plan with a child venue order.
Plans without a child venue order use the bare cancellation transition
and do not enter the guard. `submit_ai_review_decision` is not wired
to `caps_enforcer_getter` at registration.

- **`list_instruments(exchange: str)`** — returns sorted native symbols
    for the exchange inventory. It is not wallet/operator scoped;
    read-only access is gated by `READ_MARKET_DATA`; the current
    `ai_delegate` named set contains this permission.

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

- **`list_venue_account_states(wallet_public_id?, exchange?)`** —
    truthful venue account states (balances + open positions per
    wallet/exchange/mode) across the caller's accessible wallets, each
    mapped through the same fail-closed read surface as REST and carrying an
    always-present strict `reconciliation` object. Requires
    `READ_ACCOUNT_STATE`; the current `ai_delegate` named set does **not**
    contain this permission, so a delegate call returns `permission_denied`.
    Wallet scope violations return `account_state_not_found`
    (anti-enumeration). Consumers must trust the account's derived
    `effective_status` and its reconciliation object's independently derived
    `effective_status` / `is_authoritative` fields, not raw stored statuses.
    Reconciliation is authoritative only for a fresh, fully revalidated
    current `matched` or `mismatched` verdict; stale evidence and open drift
    episodes remain visible, while corrupt evidence is cleared.

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

- **`get_latest_research()`** — returns the complete current market-view
    artifact, including its ordered source citations, at the server clock.
    Requires `READ_MARKET_VIEWS`. When no causally eligible, unexpired view
    exists, the successful envelope carries `details.market_view = null`.

- **`get_ai_review_aftermath(review_public_id: str)`** — read-only
    projection for a terminal CONSULT round. Returns the complete persisted
    review plus orders, executions/fills, position-cycle transitions, and
    current position snapshots for the review's wallet and instrument over
    the inclusive `[created_at, as_of]` window. Requires `READ_SIGNALS`, a
    registered AI delegate, and a currently active grant for that exact
    wallet and instrument. Unknown and out-of-scope reviews both return
    `review_not_found`; pending and fanout-dispatched rows return
    `review_not_terminal`. Executions include stable order, instrument, mode,
    and scope-sequence lineage even when the order itself predates the window.
    The tool never changes review state or emits an audit event.

- **`submit_market_view(research_round_public_id, payload)`** — validates a
    strict `SubmittedMarketView` JSON object and completes the target pending
    research round atomically. Requires `SUBMIT_MARKET_VIEW`. Sources are
    submitted inline as `{url, title, retrieved_at}` objects; identity,
    trigger, status, and `submitted_at` are server-owned. Success returns
    `market_view_public_id`. A missing or no-longer-pending round returns
    `research_round_not_pending`; invalid source/rationale defenses return
    `invalid_market_view`.

- **`submit_manual_order(exchange, instrument, instrument_public_id,
    side, order_type, quantity, idempotency_key, wallet_public_id?,
    price?, stop_price?, operator_public_id?, ai_review_public_id?)`** —
    enqueues a trade command under the delegate's user_public_id
    with `source_surface='mcp'` + the caps check from
    `TradingCapsEnforcer.guard`. Requires `CREATE_ORDERS`, rejects
    out-of-membership `operator_public_id` values when the caller's effective
    grant lacks `impersonate:operator`, rechecks wallet scope against active
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
    REST counterpart to the
    `POST /api/ai-reviews/{review_public_id}/decision` route for
    the in-process MCP surface; lets the delegate approve or
    reject a pending CONSULT review. `review_id` is the UUID7 of
    the `ai_reviews` row. Requires `submit:ai_review_decision`. Tool-catalog
    visibility, the tool's call gate, and the REST route all answer to one
    shared capability projection, so a token that can see the tool can also
    call it and behaves identically on either transport. A legacy branch also
    admits decision-capable tokens inside the pre-versioning window — a
    permission-scope version that is absent (the production delegate token
    predates scope versioning entirely) or a literal `1` — provided the role
    ceiling grants the decision permission, carries no `manage:users`, and the
    token retained `create:orders`. Order-creation authority alone reaches the
    decision write on neither surface: a create-only `operator` token is denied
    at any version, and a create-only `admin` token is denied even with an
    absent version. The review service also
    verifies the caller has an active operational delegate lifecycle identity
    and scope grant for the review wallet and instrument. Any such delegate
    may decide — the selected delegate is who was consulted, not an exclusive
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

Every `HeartbeatConsult` envelope also attempts to include a `macro`
cross-asset snapshot built at consult time from complete persisted 1m
`kraken_equities` candles. `macro_contract_symbol` selects the quarterly
CME equity-index contract and defaults to `MNQU6-CME` (the September 2026
Micro Nasdaq-100 future); rotate this parameter as part of the operator's
quarterly contract-roll runbook. `macro_stale_after_minutes` defaults to
`10.0`. The snapshot carries `symbol`, `as_of`, server-computed
`age_minutes`, `session`, `change_1h_pct`,
`change_since_session_open_pct`, and `realized_vol_24h_pct`. The change
fields use the latest complete print against the print at least one hour
earlier and the current CME session's opening price, while realized
volatility is the population standard deviation of one-minute simple
returns from the 24 wall-clock hours ending at the last print.

Session state reuses the shared CME calendar. A fresh open-session feed
reports `session: "open"`. If CME is open but the newest candle is older
than the configured threshold, it reports `session: "halted"`, adds the
presence-only `stale: true` key, and sets all three derived values to
`null`. Scheduled daily, weekend, and holiday closures instead report
`session: "closed"` without a `stale` key: the last print remains the
calculation anchor and its honest age and available values remain in the
envelope. No proxy series is substituted during a closure. Macro and
traded-market snapshot failures are independent and fail-soft, so either
section may be omitted without preventing the consult.

Integration-owner steps to arm the wake surface:

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
strategies configured with deadlines longer than the fanout window. The route
requires `READ_SIGNALS` plus the caller's active operational
`delegate_public_id`; the second condition is lifecycle state rather than a
role gate.

For a round that already became terminal while the delegate was away,
call `get_ai_review_aftermath(review_public_id)` or its REST mirror,
`GET /api/ai-reviews/{review_public_id}/aftermath`. The returned `as_of`
anchors every temporal row in the projection, while `window_started_at`
is the review's persisted `created_at`. This is retrospective evidence only:
it does not reopen the review and grants no order authority.

Callers with `read:ai_reviews`, including the current `viewer`, `operator`,
and `admin` sets, audit what the AI decided through
`GET /api/ai-reviews`. A caller with effective `impersonate:operator` sees
every operator's reviews; every other reader is narrowed server-side to its
explicit operator memberships. The route returns
`AdminAiReviewListResponse` (`items` + `count`) with the full per-row outcome:
`status`, `decision`, `rationale`, `resolution_mode`, and the responding
delegate. It accepts optional exact-match `status`, `wallet_public_id`, and
`strategy_public_id` filters plus a `limit` (1-500, default 100), newest first.
Unlike `/pending`, it is not keyed by the delegate identity and returns
terminal decided rows rather than only pending ones.

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
    submit_price` per prior non-rejected LIVE command (`mode='paper'`
    history is excluded: paper commands carry a simulator reference
    price and simulated notional must not consume the live allowance;
    raw submit-time price, no USD re-conversion; prior live market
    orders with no submit price are skipped with a WARN log), plus the
    new submission's USD-converted notional via the USD price oracle
    (`price_unavailable` caps violation when the oracle is stale or
    missing) — the current submission is evaluated regardless of mode.
    Submit-time commitment basis; partial fills don't change
    accounting.
- `max_cancels_per_minute` — sliding-window cancel rate.

Caps are enforced at trade-command insert sites via the
`TradingCapsEnforcer.guard()` surface — `submit_manual_order`
routes through it unconditionally; `cancel_order` requires the caps
enforcer to be initialized and only enters the guard when the cancel
service emits a venue cancel command for a plan with a child venue order
(the bare cancellation transition skips the guard). The other MCP tools
(`submit_ai_review_decision`, etc.) are not currently wired to
`caps_enforcer_getter` at registration.

Update caps via `PATCH /api/ai-delegates/{id}` with
`manage:ai_integration`. The write is SCD2 close+insert, so cap history is
auditable.

---

## Kill switch + deactivation

`POST /api/ai-delegates/{id}/deactivate` runs the same code path as
the shared user-deactivation flow:

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
| 401    | `invalid_bearer_token`     | JWT signature/expiry/blacklist/inventory failure | Long-lived AI-principal tokens have no refresh token; deactivate and recreate the principal, then update the client's bearer token. Interactive sessions can use `POST /api/auth/refresh`. |
| 401    | `user_deactivated`         | Owner account deactivated                        | Prompt re-login; don't auto-refresh                   |
| 401    | Refresh token redeemed     | Replay of a spent refresh JWT                    | Re-login                                              |
| 401    | Authentication required *(detail string, no error_code)* | Session cookie flow — deactivated account, revoked session, or expired token fails DB-backed verify; require_authentication collapses all of these into one generic 401 | Re-login |
| 403    | MCP write helpers: `wallet_out_of_scope:` *(prefixed message)* / REST: `"Wallet not in accessible set"` *(detail string)* | Mutating tool targets a wallet outside the caller's scope. The helper path raises `PermissionError(f"wallet_out_of_scope: ...")` lifted by FastMCP into a `ToolError`; REST raises `HTTPException(403, detail="Wallet not in accessible set")` from `server/scoping.py`. Tool-level read/cancel paths may instead return structured anti-enumeration envelopes such as `order_not_found`, `position_not_found`, or `signal_not_found`. | Pick a wallet the caller still has a live grant on    |
| 403    | MCP write helpers: `operator_out_of_scope:` *(prefixed message)* / REST: `"Operator not in accessible set"` *(detail string)* | Same write-helper shape as the wallet variant. Callers lacking effective `impersonate:operator` must pick an operator from their authenticated set; an effective grant containing that permission has global operator scope. Read/cancel tools can intentionally collapse out-of-scope and not-found cases into structured not-found envelopes to avoid leaking resource existence. | Pick an operator from the caller's authenticated set unless its effective grant contains `impersonate:operator` |

Delegate CRUD:

| Status | Detail                                 | When it fires                                               |
| ------ | -------------------------------------- | ----------------------------------------------------------- |
| 401    | Requires populated `user_public_id`    | Principal has blank `user_public_id` (older token rollout)  |
| 403    | `Permission 'read:ai_integration' required` | Token lacks delegate list/detail visibility |
| 403    | `Permission 'manage:ai_integration' required` | Token attempts delegate or researcher creation, delegate update, or delegate deactivation without the management permission |
| 404    | `Delegate not found`                   | Unknown ID OR cross-tenant (no existence leak)              |
| 409    | `Could not derive a unique username …` | Label slug collides 8+ times (pathological)                 |
| 409    | `Operator … already owns N active AI delegates (limit 5)` | Owner hit the 5-active-delegates-per-operator cap; deactivate an existing delegate before creating another |
| 422    | `Operator '<id>' is not in …`          | Caller without effective `impersonate:operator` picked `operator_public_id` outside its claim set |
| 422    | `Caller has no primary operator …`     | No explicit operator and no primary → binding is ambiguous  |

---

## Security model

### Token lifetime

- Interactive session access tokens live **15 minutes** (configurable via
    `auth_access_token_expire_minutes`). AI delegate and researcher access
    tokens are PAT-style and live for `LONG_LIVED_TOKEN_EXPIRE_DAYS`
    (about 3 months / 90 days); integration owners rotate them on the cadence
    that fits their key-management hygiene, and revocation is immediate through
    principal deactivation rather than waiting for expiry.
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
cryptographically random data the integration owner never sees. An attacker
with DB read rights cannot brute-force the hash.

### Single-publisher invariant

`UserService.deactivate_user` is the SOLE publisher of
`admin.user_deactivated` across the entire process. `TokenManager`
and `WebSocketAuthManager` are pure subscribers/fallback readers: they
evict + close on receipt or on DB scan, but never emit the event
themselves. This holds for interactive-user deactivation and delegate
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
