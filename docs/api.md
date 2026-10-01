# API

Snapper provides a REST API and WebSocket interface for platform interaction.
The API accepts JWT authentication either through HTTP-only cookies or an
`Authorization: Bearer <jwt>` header. When both are present, Bearer auth
wins. Cookie-authenticated mutating requests that use the CSRF guard
require a valid `X-CSRF-Token` header; Bearer-authenticated requests
skip CSRF because they are not ambient browser credentials. The auth
token lifecycle routes (`/api/auth/login`, `/api/auth/refresh`,
`/api/auth/logout`, `/api/auth/ws_token`) intentionally omit the CSRF
guard.

## Authentication

Browser sessions usually use HTTP-only cookies. After login, the server
sets `access_token`, `refresh_token`, and `csrf_token` cookies
automatically. API clients and MCP/AI delegates may send the access JWT
as a Bearer token instead.

### Roles and Permissions

Every capability decision is based on effective permissions. A role is only a
stable name for a base permission set, plus a user-management value and display
label; it is not an authorization hierarchy.

| Named set | Count | Access summary |
| --------- | ----: | -------------- |
| `ai_researcher` | 3 | Research-only access: read market data and market views, then submit market views. It has no signal, order, position, process, or health access. |
| `ai_reviewer` | 11 | Read market data, market views, orders, positions, strategies, signals, system status, and backtests; create backtest comparisons; control runtime diagnostics; and see the MCP AI-review decision tool in its catalog. It has no order-execution or position-management permission, so current invocation of that tool remains unavailable. |
| `ai_delegate` | 14 | Scoped MCP and AI-review automation: the reviewer set plus scoped order creation, cancellation, and position management. It cannot manage users, settings, processes, AI integration, credentials, scope grants, or paired execution, and cannot subscribe to `alerts.` WebSocket topics. |
| `viewer` | 14 | Full read-only operator visibility: account state; operator-scoped wallets and portfolio; market data and views; orders and positions; signals and AI-review decisions; P&L timeline and attribution; backtests; strategies; process state; health; AI integration; and notifications. It cannot trade, manage positions, create backtest comparisons, control runtime diagnostics, configure or control strategies and processes, manage AI integration, or perform administrative mutations. Self-service notification-device management remains available. |
| `operator` | 26 | The complete viewer read set plus trade execution, position management, strategy configuration and lifecycle control, non-strategy process management, AI-integration management, runtime diagnostics, backtest comparisons and run management, and paired-execution terminalization. |
| `admin` | 34 | Every permission in the current `Permission` catalog, including user and system configuration, wallet credentials, scope grants, and operator impersonation. The mapping is defined as the complete catalog, so newly introduced permissions are included automatically. |

Wallet and portfolio reads use the union of active scope grants from the
caller's operator memberships and its active personal `wallet_user_read_grants`.
A caller with no operator membership can therefore read a personally granted
wallet. Personal read grants never authorize orders, cancellation, or plan
actions, which use the operator scope-grant plane. Runtime scope
decisions consume effective `impersonate:operator`; the current `admin` set is
the only set containing it. This structural permission is non-downscopable, so
an explicitly narrowed admin token retains the historical system-wide operator
and wallet scope.

JWT access is the intersection of the named role set and the token's optional
`permissions` claim, plus any non-downscopable structural grant in that named
set. The named set remains an immutable ceiling: a token may remove ordinary
permissions but cannot add one outside that set or remove
`impersonate:operator`. Tokens minted without an explicit scope retain the
complete named set. For compatibility, an older access token with no
`permissions` claim also receives the complete named set until it expires.
Authenticated profile responses expose the result as `effective_permissions`;
clients use that field for capability decisions.

Version-two explicit scopes are migrated by capability equivalence. An exact
full named-set scope adopts the current named set. For a narrowed `operator` or
`admin` scope, retained `read:signals` adds `read:ai_reviews`, while retained
`manage:processes` adds the process and AI-integration read and management
permissions plus the strategy configure/start/stop permissions that replace
its historical controls. For every named set, retained `read:backtests` adds
`create:backtest_comparisons`, and retained `read:system_status` adds
`manage:runtime_diagnostics`, only when that named set contains the replacement
permission. The `viewer` set contains none of those mutation permissions, and
an intentionally narrowed version-two viewer scope does not inherit the new
read permissions.

### POST /api/auth/login

Authenticate and create a session. Sets `access_token`, `refresh_token`,
and `csrf_token` cookies on the response.

**Request:** the body is a `LoginRequest` envelope (`PayloadRequest`)
wrapping a `LoginBody` payload — the client stamps its own
provenance on the outer envelope.

```http
POST /api/auth/login
Content-Type: application/json

{
    "type": "login_request",
    "sequence_id": 1,
    "public_id": "<client-uuid7>",
    "timestamp": "2026-01-18T12:00:00Z",
    "session_id": "<client-session>",
    "topic": null,
    "payload": {
        "username": "admin",
        "password": "password123",
        "remember_me": false,
        "permissions": [
            "read:market_data",
            "read:orders",
            "read:positions"
        ]
    }
}
```

**Response (200):**

```json
{
    "type": "login_response",
    "sequence_id": 1,
    "public_id": "<uuid7>",
    "timestamp": "2026-01-18T12:00:00Z",
    "session_id": "<server-session>",
    "topic": null,
    "payload": {
        "type": "login",
        "sequence_id": 1,
        "public_id": "<uuid7>",
        "timestamp": "2026-01-18T12:00:00Z",
        "session_id": "<server-session>",
        "topic": null,
        "message": "Login successful",
        "expires_in": 900,
        "user": {
            "type": "user_profile",
            "sequence_id": 1,
            "public_id": "<uuid7>",
            "timestamp": "2026-01-18T12:00:00Z",
            "session_id": "<server-session>",
            "topic": null,
            "username": "admin",
            "email": "admin@example.com",
            "role": "admin",
            "is_active": true,
            "created_at": "2026-01-10T08:00:00Z",
            "operator_public_ids": [],
            "primary_operator_public_id": null,
            "active_wallet_public_id": null,
            "default_language": null,
            "effective_permissions": [
                "impersonate:operator",
                "read:market_data",
                "read:orders",
                "read:positions"
            ],
            "delegate_public_id": null
        },
        "access_token": null,
        "refresh_token": null
    }
}
```

The login handler returns the raw `UserProfile` from
`authenticate_user()` and overlays `principal.active_wallet_public_id`
onto the returned `user` via `model_copy`. On the initial login
path the freshly-built principal has no wallet selected yet, so
`active_wallet_public_id` lands as `null`; the operator-membership
fields are not enriched on this surface either
(`operator_public_ids` = `[]`, `primary_operator_public_id` =
`null`). Call `GET /api/auth/me` after login to get a fully-enriched
profile with operator memberships resolved.

The outer envelope (`LoginResponse`) and the inner `payload` (`LoginData`)
both carry the standard provenance fields (`type`, `sequence_id`, `public_id`,
`timestamp`, `session_id`, `topic`). `access_token` and `refresh_token` are
`null` in the cookie flow; they are populated only when the caller passes
`?return_tokens=true` for headless integrations.

`payload.permissions` is optional. When supplied, every value must already
belong to the authenticated user's named role set; otherwise login returns 422
rather than silently dropping the invalid permission. Omit the field for the
complete named-set behavior. A refresh preserves the selected scope,
including an intentionally empty list, and cannot re-broaden it.

`remember_me` is currently accepted for compatibility but is not wired into
`/api/auth/login`: refresh JWTs use `auth_refresh_token_expire_days`, and
browser refresh cookies use a fixed 7-day `Max-Age`.
`auth_refresh_token_expire_days_extended` is reserved until login passes the
flag through to token creation.

**Cookies set:**

| Cookie | HttpOnly | Secure | SameSite | Path | Max-Age | Description |
| ------ | -------- | ------ | -------- | ---- | ------- | ----------- |
| `access_token` | Yes | Yes (prod) | Lax/Strict | `/` | session | JWT access token (15 min) |
| `refresh_token` | Yes | Yes (prod) | Lax/Strict | `/api/auth` | 7 days | JWT refresh token |
| `csrf_token` | No | Yes (prod) | Lax/Strict | `/` | session | CSRF protection token |

### POST /api/auth/refresh

Refresh session tokens. The `refresh_token` cookie is sent automatically by
the browser. Returns new tokens in cookies plus a WebSocket authentication
token in the response body. Long-running clients that already hold a valid
access token and only need a fresh `ws_token` should call
[`POST /api/auth/ws_token`](#post-apiauthws_token) instead — that route does
not rotate the refresh-token pair.

**Request:**

```http
POST /api/auth/refresh
```

**Response (200):**

```json
{
    "type": "refresh_response",
    "sequence_id": 2,
    "public_id": "<uuid7>",
    "timestamp": "2026-01-18T12:00:00Z",
    "session_id": "<server-session>",
    "topic": null,
    "payload": {
        "type": "refresh",
        "sequence_id": 2,
        "public_id": "<uuid7>",
        "timestamp": "2026-01-18T12:00:00Z",
        "session_id": "<server-session>",
        "topic": null,
        "message": "session refreshed",
        "ws_token": "eyJhbGciOi...",
        "ws_token_exp": "2026-01-18T12:30:00Z",
        "csrf_token": "abc123def456...",
        "user": {
            "type": "user_profile",
            "sequence_id": 2,
            "public_id": "<uuid7>",
            "timestamp": "2026-01-18T12:00:00Z",
            "session_id": "<server-session>",
            "topic": null,
            "username": "admin",
            "email": "admin@example.com",
            "role": "admin",
            "is_active": true,
            "created_at": "2026-01-10T08:00:00Z",
            "operator_public_ids": [],
            "primary_operator_public_id": null,
            "active_wallet_public_id": null,
            "default_language": null,
            "effective_permissions": [
                "impersonate:operator",
                "read:market_data",
                "read:orders",
                "read:positions"
            ],
            "delegate_public_id": null
        },
        "access_token": null,
        "refresh_token": null
    }
}
```

The refresh handler returns the `UserProfile` from
`get_user_by_id()` and overlays the principal's
`active_wallet_public_id` onto it — that value is seeded from
`token_data.active_wallet_public_id` (and may be swapped via an
optional `RefreshTokenPayload` wallet hint), so the refresh
response generally carries the active wallet (it is only `null`
when the refresh JWT itself carries no wallet claim and no hint
is supplied). Operator memberships are still NOT resolved here
(`operator_public_ids` = `[]`, `primary_operator_public_id` =
`null`); use `GET /api/auth/me` for the enriched view.

`access_token` and `refresh_token` are populated only when
`?return_tokens=true` is set (headless integrations); the cookie flow
leaves them `null` and writes the rotated JWTs as `Set-Cookie` headers.
The `RefreshTokenRequest` body (optional) carries
`active_wallet_public_id` / `clear_active_wallet` to atomically swap
the caller's active wallet during the rotation; an
`Authorization: Bearer <refresh-jwt>` header is read first, with the
`refresh_token` cookie used as fallback for browser callers.

### POST /api/auth/ws_token

Mint a one-shot WebSocket authentication token without rotating the
caller's refresh-token pair. Authenticates via the access bearer
(header or cookie) and returns a fresh `ws_token` bound to the
access JWT's session. Per-source-IP rate-limited so a reconnect
storm cannot exhaust the token store.

Use this route when a long-running client (e.g. a push-wakeup
monitor) needs to mint successive `ws_token`s on its own cadence
without rotating the refresh-token pair shared with sibling
processes.

**Request:**

```http
POST /api/auth/ws_token
Authorization: Bearer <access_token>
```

**Response (200):**

```json
{
    "type": "ws_token_response",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:15:00Z",
    "topic": null,
    "payload": {
        "type": "ws_token",
        "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5c",
        "session_id": "<server-session>",
        "sequence_id": 1,
        "timestamp": "2026-01-18T12:15:00Z",
        "topic": null,
        "message": "ws_token issued",
        "ws_token": "eyJhbGciOi...",
        "ws_token_exp": "2026-01-18T12:30:00Z",
        "expires_in": 900
    }
}
```

`expires_in` mirrors `ws_token_exp` as relative seconds for clients
that prefer relative-deadline math.

**Errors:**

- `401 Unauthorized` — access bearer is absent, expired, or invalid.
- `429 Too Many Requests` — per-source-IP minute budget exhausted.

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
    "type": "message",
    "public_id": "<uuid7>",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": "Logged out successfully"
}
```

### GET /api/auth/me

Get the currently authenticated user's profile.

**Request:**

```http
GET /api/auth/me
```

**Response (200):**

Handler returns a `UserResponse` envelope (`PayloadResponse[user_response, UserProfile]`)
wrapping the full `UserProfile` payload, which carries the multi-tenant trio:

```json
{
    "type": "user_response",
    "sequence_id": 3,
    "public_id": "<uuid7>",
    "timestamp": "2026-01-18T12:00:00Z",
    "session_id": "<server-session>",
    "topic": null,
    "payload": {
        "type": "user_profile",
        "sequence_id": 3,
        "public_id": "<uuid7>",
        "timestamp": "2026-01-18T12:00:00Z",
        "session_id": "<server-session>",
        "topic": null,
        "username": "admin",
        "email": "admin@example.com",
        "role": "admin",
        "is_active": true,
        "created_at": "2026-01-10T08:00:00Z",
        "operator_public_ids": ["019d6ca4-..."],
        "primary_operator_public_id": "019d6ca4-...",
        "active_wallet_public_id": "019d7e9a-...",
        "default_language": "pl",
        "effective_permissions": [
            "impersonate:operator",
            "read:market_data",
            "read:orders",
            "read:positions"
        ],
        "delegate_public_id": null
    }
}
```

Only `/api/auth/me` resolves the `operator_public_ids` and
`primary_operator_public_id` membership fields. A caller with effective
`impersonate:operator` receives every active operator; all other callers,
including the current `operator` and `viewer` sets, receive only their
explicit memberships. `active_wallet_public_id` reflects the active wallet
selection. These fields power the frontend OperatorPicker without a second
round trip. `effective_permissions` is the role-bounded token grant that
clients use for capability checks, and `delegate_public_id` identifies an
operational AI-delegate lifecycle identity when one exists.

The login and refresh responses return the `UserProfile` from
`authenticate_user()` / `get_user_by_id()` with the active wallet overlaid
from the request principal. Operator memberships are not resolved on those
two surfaces (`operator_public_ids` = `[]`,
`primary_operator_public_id` = `null`). Active-wallet semantics differ
between the two: login lands `active_wallet_public_id = null` because no
wallet is selected at first authentication; refresh propagates whatever
wallet the refresh JWT carries or the request hint. Clients should call
`/api/auth/me` to get a fully resolved profile.

### POST /api/auth/me/update

Update the authenticated caller's self-service preferences. Currently
exposes `default_language` only; additional preference fields may
be added later without changing the endpoint contract. Mirrors the
admin `POST /api/auth/users/{user_id}/update` shape (codebase convention:
`POST + verb`, no REST `PATCH`).

**Request:**

```http
POST /api/auth/me/update
Content-Type: application/json
X-CSRF-Token: <token>
```

```json
{
    "type": "update_auth_me_request",
    "payload": {
        "default_language": "pl"
    }
}
```

`default_language` is validated against the union of supported
client codes (iOS-canonical + frontend-canonical forms accepted —
e.g. `"zh-Hans"`, `"zh"`, `"pt-BR"`, `"pt"`, `"nb"`, `"no"`).
`null` clears the preference.

**Response (200):**

Returns the same `UserResponse` envelope shape as `GET /api/auth/me`,
with the updated `default_language` echoed in `payload`.

**Errors:**

- `404` — caller's user row was not found (rare; only surfaces if
  an admin concurrently deactivates the user between auth-dep
  resolution and the update commit).
- `422` — `default_language` not in the supported-language allowlist.

### GET /api/auth/users

List users. Requires `manage:users`.

**Query parameters:**

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `include_inactive` | bool | Include deactivated users (default `false`) |
| `as_of` | datetime | Optional point-in-time query timestamp |

Returns `UserListResponse` with `payload` and `count`.

### Desk membership management

Desk membership management is available to callers with the effective
`manage:desk_memberships` permission. A non-global caller must also have a
target-desk claim in the authenticated token and a currently active membership
in that desk; the live membership is re-read from the database for each
operation. The intersection keeps the token as a ceiling, so a desk attached
after login cannot be managed until explicit login. A caller with effective
`impersonate:operator` in both the token and the current database role is the
explicit global exception. The current named sets grant desk management to
`operator` and `admin`, while `viewer` does not hold it.

The first CRUD slice may attach or detach active human `viewer` users only.
AI-delegate membership remains owned by delegate creation and deactivation,
and human `operator` / `admin` memberships are visible in the directory but
cannot be mutated through these endpoints.

#### GET /api/auth/desks/{operator_public_id}/members

List active human members of one desk. The response is a `UserListResponse`,
ordered by username. Its `UserProfile` entries are deliberately desk-scoped:

- `operator_public_ids` contains only the requested desk;
- `primary_operator_public_id` contains that desk only when this membership is
  the user's primary membership, otherwise it is `null`;
- `email` is `null`, and memberships in other desks are never disclosed.

The optional `as_of` query returns the membership directory at that UTC
timestamp for time-travel audit. The caller's permission and target-desk
membership are still revalidated against the live database state.

This endpoint does not require `manage:users`. It therefore gives an operator
the directory needed to manage its own desk without exposing the global user
directory.

#### POST /api/auth/desks/{operator_public_id}/members/{username}

Attach the active human `viewer` identified by the exact username to the desk.
The final path parameter uses the path converter, so clients must percent-encode
the username and names containing `/` remain addressable.
The operation is idempotent: repeating an active pair returns `200` without
creating another membership. The first membership becomes primary; later
memberships are non-primary. Cookie-authenticated requests require CSRF.

Attachment deliberately takes effect on the target's next login. Existing
access tokens and WebSocket principals are not widened in place, and there is
no `admin.membership_granted` event. Established deployments must use this
explicit endpoint and then have the target log in again; local seed or demo
commands do not attach production users. Refresh rotation preserves this
boundary: it intersects desk IDs and membership generations with the signed
session, and a database role change requires explicit authentication instead
of upgrading the existing refresh session.

The response is a `MessageResponse` confirming that the user is attached.

#### DELETE /api/auth/desks/{operator_public_id}/members/{username}

Detach an active human `viewer` from the desk. The operation is idempotent: an
already-absent pair still returns `200` and does not emit another revocation
event. If the removed membership was primary, the oldest surviving membership
by `(timestamp, public_id)` is promoted deterministically; removing the last
membership leaves the user with no primary desk. Cookie-authenticated requests
require CSRF.

For a real detachment, every active access and refresh session belonging to the
target is revoked immediately and positive token-verification caches are
invalidated before the authority reduction is committed. After commit the
service emits the backend-internal `admin.membership_revoked` event. Auth
listeners invalidate cross-instance token caches, retire the target's active
WebSocket subscriptions, and close all of its sockets with code `4003`.
Database-backed membership checks provide the broker-outage fallback. The
target must log in again to receive credentials containing only its surviving
desk memberships.

The endpoints use the following error classes:

- `403` — missing `manage:desk_memberships`, or a non-global caller is not a
  current member of the target desk;
- `404` — the active target desk does not exist; mutations also return it when
  the active target user does not exist;
- `422` — an attach or detach target is not a human `viewer`, including an AI
  delegate.

### POST /api/auth/users

Create a user. Requires `manage:users` and CSRF for cookie auth. Body is
`CreateUserRequest` wrapping username, password, optional email, role,
and `is_active`.

### POST /api/auth/users/{user_id}/update

Update email, role, and active state for an existing user. Requires
`manage:users` and CSRF for cookie auth. Returns `UserResponse`.

### POST /api/auth/users/{user_id}/deactivate

Deactivate a user through the canonical kill-switch flow. Requires
`manage:users` and CSRF for cookie auth. The path segment is resolved as
the target username; self-deactivation returns `400`, unknown active
users return `404`. The service also revokes active sessions and emits
`admin.user_deactivated` for immediate verify-cache eviction and
WebSocket close fanout; auth listeners also poll the committed
`users.is_active=False` row as broker-outage fallback.

### POST /api/auth/users/{user_id}/change-password

Change a password. Users may change their own password; a caller with
`manage:users` may change another user's password. Requires CSRF for cookie
auth and is account-rate limited. Body is `ChangePasswordRequest` with current
and new password.

### POST /api/auth/users/{user_id}/admin-reset-password

Admin password reset without the current password. Requires
`manage:users`, CSRF for cookie auth, and is account-rate limited. Body
is `AdminResetPasswordRequest`.

### CSRF Protection

The `csrf_token` cookie is readable by JavaScript (not HttpOnly). For
cookie-authenticated mutating requests outside `/api/auth/login`,
`/api/auth/refresh`, `/api/auth/logout`, and
`/api/auth/ws_token`, include its value as a header:

```http
X-CSRF-Token: <value from csrf_token cookie>
```

New CSRF tokens are issued on login and refresh. There is no separate
endpoint for obtaining CSRF tokens.

Bearer-authenticated requests do not require `X-CSRF-Token`. This is
intentional: the request is authorized by the explicit
`Authorization: Bearer ...` header rather than an ambient browser cookie.

## REST Endpoints

REST endpoints return the same Data schemas used by WebSocket messages
(`OrderData`, `SignalData`, `ExecutionData`, `PositionData`, `CandleData`
from `messaging.schemas.data`). This means the wire format is identical
whether data arrives via REST or the WebSocket feed.

### REST envelopes — `PayloadResponse` / `PayloadListResponse`

REST responses are JSON objects with a typed envelope plus a
`payload`. Two shapes ship from `src/snapper/api/schemas/base.py`:

- `PayloadResponse[T, P]` — singleton response, fields:
  `type` (Literal discriminator), envelope provenance
  (`public_id`, `session_id`, `sequence_id`, `timestamp`, `topic`)
  and `payload: P`.
- `PayloadListResponse[T, P]` — list response, same envelope plus
  `payload: list[P]` and `count: int` (always equal to
  `len(payload)`).

Per-item provenance is preserved on items that originate from a
bitemporal DB projection (the item carries its own
`public_id`/`session_id`/`sequence_id`); minted results share the
envelope's provenance. Each item's data-type shape is identical to
what the WebSocket feed delivers for the same domain object — only
the wrapping differs.

OpenAPI request-body schemas for routes that use the `json_body`
dependency are patched into FastAPI's generated OpenAPI from
`openapi_extra` metadata. Optional request bodies, such as refresh and
delegate deactivate, stay optional in the generated schema. The mounted
`/api/mcp` Streamable HTTP sub-app is intentionally outside FastAPI's
OpenAPI route list; its tool contract is documented in
[ai-integration.md](ai-integration.md).

### Provenance on Reads vs. Mutations

- **GET reads** — No client provenance is expected. When telemetry recording
  is enabled (disabled by default), the server records the request in the
  `telemetry` table for observability; otherwise no provenance is recorded
  for reads. Either way, it does not stamp provenance onto the response items
  beyond what was stored at write time.

- **Mutations (POST/PATCH/DELETE)** — Most mutation bodies use
  command-style `PayloadRequest` envelopes carrying provenance
  (`public_id`, `session_id`, `sequence_id`, `timestamp`) and domain
  intent in `payload`; most write routes follow the `POST + verb`
  convention, with a small number of PATCH surfaces (AI delegate caps,
  device prefs, alert defaults) using the same envelope. Body-less or
  query-only mutations — for example
  `DELETE /api/devices/{device_public_id}`,
  `POST /api/processes/{name}/stop`,
  `POST /api/paired-execution/groups/{group_public_id}/terminalize`, and
  the tracemalloc start/stop routes — carry no client envelope; their
  control rows record server-side provenance with no extracted payload.
  The server-side `ClientProvenanceMiddleware` extracts envelope fields
  when a request body is present, emits a structured info log, and runs
  a per-session `GapDetector` to warn on sequence gaps. Gaps are logged
  as warnings but never reject requests.

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
`SERVER_API_ONLY` mode the status is `"healthy"` (no processes are
started by design) unless a manually started process has parked after
exhausting its restart budget, in which case `"error"` is reported.

**Response (200):**

```json
{
    "type": "health_check_response",
    "sequence_id": 1,
    "public_id": "<uuid7>",
    "timestamp": "2026-01-18T12:00:00Z",
    "session_id": "<server-session>",
    "topic": null,
    "payload": {
        "type": "health_check",
        "sequence_id": 1,
        "public_id": "<uuid7>",
        "timestamp": "2026-01-18T12:00:00Z",
        "session_id": "<server-session>",
        "topic": null,
        "status": "healthy",
        "version": "0.1.0",
        "connections": {
            "active_connections": 5,
            "zmq_subscribers": 12,
            "subscriber_tasks": 12,
            "active_topics": 8,
            "active_clients": 3
        },
        "topics": {
            "active": 8
        },
        "gap_detection": {
            "bridge": {
                "gaps_detected": 0,
                "session_resets": 0,
                "duplicates": 0,
                "mid_stream_joins": 0,
                "rejected_unstamped": 0
            },
            "rest_clients": {}
        }
    }
}
```

The outer `health_check_response` envelope carries server-side
provenance; the inner `health_check` payload holds the runtime
snapshot. `gap_detection` provides observability into sequence gap
detection across the ZMQ bridge and per-session REST client detectors.

### GET /api/health/egress

Egress route snapshot. Requires `read:system_status`
permission and uses the same CSRF guard as the detailed monitoring
health routes.

The endpoint returns the API process's local egress snapshot merged with
the latest `system.egress.snapshot` frames from other pool-bearing
processes such as `snapper-feed`. Missing remote snapshots are not an
error: the payload still includes the API process. Stale remote
snapshots remain visible and are flagged in `containers`.

**Response (200):**

```json
{
    "type": "egress_health_response",
    "sequence_id": 1,
    "public_id": "<uuid7>",
    "timestamp": "2026-01-18T12:00:00Z",
    "session_id": "<server-session>",
    "topic": null,
    "payload": {
        "type": "egress_health",
        "sequence_id": 1,
        "public_id": "<uuid7>",
        "timestamp": "2026-01-18T12:00:00Z",
        "session_id": "<server-session>",
        "topic": null,
        "enabled": true,
        "on_all_quarantined": "wait",
        "private_fallback_route_id": "pl",
        "private_on_fallback": false,
        "containers": [
            {
                "container": "api:coord-0@snapper",
                "last_seen_age_seconds": 0.0,
                "stale": false,
                "route_count": 1
            },
            {
                "container": "publisher:kraken@snapper-feed",
                "last_seen_age_seconds": 1.4,
                "stale": false,
                "route_count": 1
            }
        ],
        "routes": [
            {
                "id": "default",
                "kind": "direct",
                "proxy_url": null,
                "region": "host",
                "exit_ip": "198.51.100.11",
                "provider": "isp",
                "priority": 100,
                "allowed_exchanges": [],
                "enabled": true,
                "quarantined": false,
                "quarantine_seconds_remaining": null,
                "in_use_count": 2,
                "active_reservations": [
                    {
                        "exchange": "kraken",
                        "traffic_class": "private",
                        "container": "api:coord-0@snapper"
                    },
                    {
                        "exchange": "kraken",
                        "traffic_class": "public",
                        "container": "publisher:kraken@snapper-feed"
                    }
                ],
                "connections": [
                    {
                        "host": "ws-auth.kraken.com",
                        "kind": "ws",
                        "exchange": "kraken",
                        "traffic_class": "private",
                        "container": "api:coord-0@snapper",
                        "count": 1,
                        "last_seen_at": null
                    },
                    {
                        "host": "api.kraken.com",
                        "kind": "rest",
                        "exchange": "kraken",
                        "traffic_class": "public",
                        "container": "publisher:kraken@snapper-feed",
                        "count": 0,
                        "last_seen_at": "2026-01-18T12:00:01Z"
                    }
                ],
                "transfer": null
            }
        ]
    }
}
```

`region`, `exit_ip`, and `provider` are optional operator metadata copied
from the `egress_pool.routes[]` entry. `proxy_url` is present so the API
can join sidecar transfer samples to SOCKS5 routes by listener port.
`active_reservations` lists the unique `(container, exchange,
traffic_class)` tuples currently reserved on the route. `connections`
lists target hostnames by `(container, host, kind, exchange,
traffic_class)`: WebSocket rows carry currently open counts, while REST
rows retain the capped last-seen host with count zero after the short
reservation releases. Only hostnames are reported; URL paths, queries,
headers, bodies, and credentials are never included.

SOCKS5 routes may include `transfer` when the API has a matching
`system.egress.transfer` sample from `snapper-egress`. The nested object
contains `interface`, `socks5_listen_port`, cumulative `rx_bytes` and
`tx_bytes`, current `rx_rate_bytes_per_second` and
`tx_rate_bytes_per_second` when a reliable delta exists,
`latest_handshake_at`, `counter_reset`, `sampled_at`,
`sample_age_seconds`, and `stale`. Direct routes, missing samples, and
ambiguous port joins return `transfer: null`.

`containers` lists each reporting process, the age of its latest snapshot
as observed by the API process, whether that snapshot is stale, and how
many routes it reported. `private_on_fallback` is true when any private
reservation is active on a non-direct route.

### GET /api/candles

Fetch OHLCV candlestick data. Requires `read:market_data` permission.

**Request:**

```http
GET /api/candles?instrument=BTC-USD&exchange=kraken&timeframe=1h&limit=100
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `instrument` | string | yes | Instrument symbol (e.g., `BTC-USD`) |
| `exchange` | string | yes | Exchange name (`kraken`, `kraken_futures`, `kraken_equities`, `walutomat`, `polygon`) |
| `timeframe` | string | yes | Candle timeframe (e.g., `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`) |
| `limit` | int | no | Number of candles, max 1000 (default 100) |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |
| `start` | datetime | no | Market-time window start (`open_at`, UTC); pair with `end` |
| `end` | datetime | no | Market-time window end (`open_at`, UTC); pair with `start` |

Returns `200 OK` with an empty `payload` array when no candles match
the query (unknown instrument, no warm-cache rows, etc.) — the
`CandleListResponse` envelope is the canonical shape for empty
results too.

When both `start` and `end` are supplied the route reads a market-time
range by `open_at` (ascending, capped at `limit`), bypassing the cache and
the `as_of` write-time routing. This navigates the full persisted history —
including bulk-backfilled corpora whose database write time is unrelated to
their market time — and powers the market view's time-travel scrubber.
Supplying only one of the pair, or `start >= end`, returns `400`.

**Response (200):**

```json
{
    "type": "candle_list",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": [
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
            "trades": 5678,
            "complete": true
        }
    ],
    "count": 1
}
```

The `complete` flag marks a provisional intra-minute update of a
still-forming bar when `false`, and the final bar for its window when
`true` (default `true`).

### GET /api/candles/db

Explicit DB-only candle read for tokens with `read:market_data`. Parameters and response
shape match `GET /api/candles` except the `start`/`end` market-time
window pair is not accepted; the route bypasses the in-process market
cache unconditionally so incident response can verify persisted
`candles` rows directly.

### GET /api/candles/cache

Explicit cache-only candle diagnostic read. Parameters match
`GET /api/candles` except `as_of` and the `start`/`end` window pair are
not accepted, and `timeframe` is optional (default `1m`). For cache-eligible
timeframes (`1m`, `5m`, `15m`, `30m`) the route returns cache-shaped
payload data with diagnostic fields such as `is_warm`, `source`, and
`sample_count`; cold cache returns an empty payload with `is_warm=false`
instead of silently falling back to DB. Long frames (`1h`, `4h`, `1d`)
fall through to persisted rows because the cache does not serve them.

### GET /api/orders

Fetch orders with optional filtering. Requires `read:orders` permission.

**Request:**

```http
GET /api/orders?symbol=BTC-USD&exchange=kraken&limit=100&offset=0
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `symbol` | string | no | Filter by instrument symbol |
| `exchange` | string | no | Filter by exchange (`paper`, `kraken`, `kraken_futures`, `walutomat`) |
| `limit` | int | no | Number of orders, 1-1000 (default 100) |
| `offset` | int | no | Number of orders to skip (default 0) |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |
| `operator_public_id` | string | no | Scope to a single operator (403 if foreign) |
| `wallet_public_id` | string | no | Scope to a single wallet (403 if inaccessible) |

**Response (200):**

```json
{
    "type": "order_list",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": [
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
            "mode": "live",
            "error": null
        }
    ],
    "count": 1
}
```

### POST /api/orders

Create a manual order via a `manual_once` execution plan. Requires
`create:orders` permission.

**Request:**

```http
POST /api/orders
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "type": "create_order_command",
    "session_id": "ui",
    "sequence_id": 0,
    "public_id": "<uuid7>",
    "timestamp": "2026-04-10T12:00:00Z",
    "payload": {
        "instrument": "BTC-USD",
        "instrument_public_id": "<uuid>",
        "exchange": "kraken",
        "mode": "live",
        "side": "buy",
        "order_type": "limit",
        "quantity": 0.5,
        "price": 50000.0,
        "time_in_force": "GTC",
        "post_only": false,
        "leverage": null,
        "reduce_only": false,
        "wallet_public_id": "<uuid>",
        "idempotency_key": "<uuid7>",
        "ai_review_public_id": null
    }
}
```

**Payload fields:**

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `instrument` | string | yes | Native symbol (e.g., BTC-USD) |
| `instrument_public_id` | string | yes | Required by the request schema for client-side consistency; the server resolves the persisted instrument UUID from `instrument` + `exchange`, so this field is not authoritative |
| `exchange` | string | yes | Exchange name |
| `mode` | string | no | `live` (default) or `paper` |
| `side` | string | yes | `buy` or `sell` |
| `order_type` | string | yes | `market`, `limit`, `stop`, `stop_limit` |
| `quantity` | float | yes | Order size (must be > 0) |
| `price` | float | cond | Required for `limit` and `stop_limit` |
| `stop_price` | float | cond | Required for `stop` and `stop_limit` |
| `time_in_force` | string | no | Time-in-force policy (`GTC` default) |
| `post_only` | boolean | no | Maker-only order flag (`false` default) |
| `leverage` | int | no | Optional leverage multiplier |
| `reduce_only` | boolean | no | Reduce-only flag for closing exposure (`false` default) |
| `wallet_public_id` | string | no | Target wallet UUID; when omitted, the server resolves the caller's single accessible wallet for the requested mode |
| `operator_public_id` | string | no | Operator identity |
| `idempotency_key` | string | no | Dedup key (409 on duplicate) |
| `ai_review_public_id` | string | no | Approved `ai_reviews` row cited for AI-mediated manual orders |

**Response (200):** `ExecutionPlanResponse` envelope with plan details.

**Errors:** 403 (disabled/forbidden), 409 (idempotency conflict), 422 (validation).

### POST /api/orders/{plan_public_id}/cancel

Cancel an active execution plan by its plan public id. Requires
`cancel:orders` permission and caller access to the plan's wallet.

The route transitions the plan to `cancel_requested`, hydrates the
venue-assigned `exchange_order_id` from the active `orders` row if
available, and inserts a `cancel` `TradeCommand` so the outbox
dispatcher can publish `OrderCancelData` on the
`orders.commands.{ex}.{instr}.cancel` topic. If the cancel
`TradeCommand` insert fails the plan is transitioned to `failed` with
`last_error`, and HTTP 500 is returned — `PlanExecutorService` will
re-emit the cancel on the next startup (idempotent via
`has_pending_cancel_command`).

**Request:**

```http
POST /api/orders/<plan_public_id>/cancel
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "type": "cancel_order_command",
    "session_id": "ui",
    "sequence_id": 0,
    "public_id": "<uuid7>",
    "timestamp": "2026-04-10T12:01:00Z",
    "payload": {
        "reason": "changed mind"
    }
}
```

**Response (200):** `ExecutionPlanResponse` with status `cancel_requested`.

**Errors:** 403 (wallet not accessible), 404 (not found), 409 (already
terminal or concurrent change), 422 (caps violation), 500 (cancel command
insert failed).

### POST /api/orders/by-client-order-id/{client_order_id}/cancel

UI convenience route that cancels by the child order's
`client_order_id` (the value displayed on the Orders table). Resolves
the owning plan via the `trade_commands` table and delegates to the
shared cancel flow described above.

Requires `cancel:orders` permission. Same response shape and error
codes as the plan-id route, plus 404 when no plan-linked
`trade_commands` row is found for the given `client_order_id`.

**Request:**

```http
POST /api/orders/by-client-order-id/<client_order_id>/cancel
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "type": "cancel_order_command",
    "session_id": "ui",
    "sequence_id": 0,
    "public_id": "<uuid7>",
    "timestamp": "2026-04-11T12:00:00Z",
    "payload": {
        "reason": "cancelled from Orders table"
    }
}
```

**Response (200):** `ExecutionPlanResponse` with status `cancel_requested`.

**Errors:** 403 (wallet not accessible), 404 (no linked plan),
409 (already terminal), 422 (caps violation), 500 (cancel command insert
failed).

### GET /api/instrument-capabilities

Fetch instrument order capability matrix. Requires `read:market_data`.

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `exchange` | string | no | Filter by exchange |
| `instrument_public_id` | string | no | Filter by instrument |
| `as_of` | datetime | no | Point-in-time query |

**Response (200):** `InstrumentCapabilityListResponse` with capability flags.

### GET /api/venue-fee-schedules

Fetch venue fee schedules. Requires `read:market_data`.

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `exchange` | string | no | Filter by exchange |
| `as_of` | datetime | no | Point-in-time query |

**Response (200):** `VenueFeeScheduleListResponse` with fee tiers.

### GET /api/signals

Fetch trading signals with optional filtering. Requires `read:signals`
permission.

**Request:**

```http
GET /api/signals?instrument=BTC-USD&strategy=rsi_btc_1h&exchange=paper&hours=24&limit=100
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `instrument` | string | no | Filter by instrument symbol |
| `strategy` | string | no | Filter by strategy name |
| `exchange` | string | no | Filter by exchange (`paper`, `kraken`, `kraken_futures`, `walutomat`) |
| `hours` | int | no | Hours of history, max 168 (default 24) |
| `limit` | int | no | Number of signals, max 1000 (default 100) |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |
| `operator_public_id` | string | no | Scope to a single operator (403 if foreign) |
| `wallet_public_id` | string | no | Scope to a single wallet (403 if inaccessible) |

**Response (200):**

```json
{
    "type": "signal_list",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": [
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
    ],
    "count": 1
}
```

### GET /api/executions

Fetch order fills/executions. Requires `read:orders` permission.

**Request:**

```http
GET /api/executions?limit=100
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `limit` | int | no | Number of executions, max 1000 (default 100) |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |
| `operator_public_id` | string | no | Scope to a single operator (403 if foreign) |
| `wallet_public_id` | string | no | Scope to a single wallet (403 if inaccessible) |

**Response (200):**

```json
{
    "type": "execution_list",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:01:00Z",
    "topic": null,
    "payload": [
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
            "last_size": 0.1,
            "last_price": 42000.0,
            "fee": 0.001,
            "fee_asset": "USD",
            "status": "filled",
            "executed_at": "2026-01-18T12:00:59Z"
        }
    ],
    "count": 1
}
```

### GET /api/positions

Fetch current portfolio positions. Requires `read:positions` permission.

**Request:**

```http
GET /api/positions
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `as_of` | datetime | no | Point-in-time query, UTC (default: current time) |
| `operator_public_id` | string | no | Scope to a single operator (403 if foreign) |
| `wallet_public_id` | string | no | Scope to a single wallet (403 if inaccessible) |

**Response (200):**

```json
{
    "type": "position_list",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": [
        {
            "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
            "type": "position",
            "timestamp": "2026-01-18T12:00:00Z",
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "quantity": 0.5,
            "average_price": 41500.0,
            "unrealized_pnl": 250.0,
            "realized_pnl": 100.0,
            "mark_price": 41650.0,
            "marked_at": "2026-01-18T11:59:30Z",
            "source_venue_event_id": 4711,
            "mode": "live",
            "position_cycle_public_id": "019e1a2b-4d5e-7f6a-8b9c-0d1e2f3a4b5c"
        }
    ],
    "count": 1
}
```

The `position_cycle_public_id` field is `null` when no open position cycle exists
for the position (e.g. flat positions or positions without cycle tracking).

Truthful-valuation semantics: economic rows are written by the trader's
position projection and NULLs are honest, never zero-coerced. Current reads
overlay the stored event-time mark with the newest durable tick for the exact
valuation instrument when it is no more than ten minutes old. Missing, stale,
unmapped-paper, non-positive, and non-finite tick evidence returns
`mark_price`, `marked_at`, and `unrealized_pnl` as `null`. Historical `as_of`
reads retain the projection's event-time mark. `average_price`
is `null` when an aggregate of opposing paper strategy shards has no single
truthful entry (or a component entry is unknown). `mark_price` / `marked_at`
on historical rows echo the active market snapshot captured by the triggering
projection event; both are `null` together with `unrealized_pnl` when no usable
mark exists.
`source_venue_event_id` is the maximum durable venue-event watermark consumed
into the row's state (a recovery watermark, not the exact causal fill), `null`
when unknown.

### GET /api/portfolio/accounts

Fetch truthful venue account state and portfolio-reconciliation truth per
(wallet, exchange, mode). The venue observation remains distinct from the
fill-derived position projection, while the nested reconciliation view compares
the two truth planes. Requires `read:account_state`; the current `ai_delegate`
named set does not contain that permission. There is no `as_of` parameter: an
account state has no truthful historical projection, only its current
observation.

Live clients subscribe to `portfolio.accounts.`. Executors publish a thin
`account_state_changed_event` after snapshot and reconciliation commits; the
event is an invalidation signal, not an account payload. Clients refetch this
REST endpoint to rebuild the read-time fail-closed view and retain a 60-second
safety-net poll for missed frames. The bridge enforces the same accessible-wallet
scope per frame, with system-wide scope for callers holding effective
`impersonate:operator` and fail-closed malformed-frame handling.

**Request:**

```http
GET /api/portfolio/accounts
```

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `operator_public_id` | string | no | Scope to a single operator (403 if foreign) |
| `wallet_public_id` | string | no | Scope to a single wallet (403 if inaccessible) |

**Response (200):**

```json
{
    "type": "portfolio_account_state_list",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": [
        {
            "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
            "type": "portfolio_account_state",
            "timestamp": "2026-01-18T12:00:00Z",
            "wallet_public_id": "019e1a2b-1111-7000-8000-000000000001",
            "exchange": "kraken",
            "mode": "live",
            "sync_status": "observed",
            "effective_status": "observed",
            "is_authoritative": true,
            "balance_status": "observed",
            "position_status": "observed",
            "valuation_status": "native_only",
            "balances": [
                {
                    "currency": "USD",
                    "total": 10000.0,
                    "free": 9000.0,
                    "used": 1000.0
                }
            ],
            "open_positions": [
                {
                    "symbol": "PF_XBTUSD",
                    "side": "buy",
                    "size": 0.5,
                    "entry_price": 41500.0,
                    "mark_price": 41650.0,
                    "unrealized_pnl": 75.0,
                    "unrealized_funding": -1.25,
                    "timestamp": "2026-01-18T11:59:30Z"
                }
            ],
            "balance_observed_at": "2026-01-18T11:59:30Z",
            "position_observed_at": "2026-01-18T11:59:30Z",
            "authoritative_until": "2026-01-18T12:05:00Z",
            "current_attempt_observation_id": 4711,
            "balance_payload_source_observation_id": 4711,
            "position_payload_source_observation_id": 4711,
            "error": null,
            "reconciliation": {
                "method": null,
                "evaluation_status": null,
                "effective_status": "incomplete",
                "is_authoritative": false,
                "evaluated_at": null,
                "current_observation_id": null,
                "last_full_observation_id": null,
                "detail_source_observation_id": null,
                "last_full_outcome": null,
                "consecutive_full_mismatches": 0,
                "anchor_public_id": null,
                "venue_account_state_public_id": null,
                "venue_account_observation_id": null,
                "source_watermark_kind": null,
                "source_watermark": null,
                "expected": null,
                "actual": null,
                "difference": null,
                "tolerance": null,
                "reconciled_at": null,
                "authoritative_until": null,
                "error": null,
                "open_drift_episode": null
            }
        }
    ],
    "count": 1
}
```

Fail-closed read semantics: `sync_status` is the raw stored
outcome of the last observation attempt (`observed` / `simulated` /
`unsupported` / `error`), while `effective_status` is DERIVED at read time and
is the one consumers must trust. It demotes an `observed` row to `stale` once
its `authoritative_until` window has elapsed (or is missing), to `clock_error`
on a future-dated observation clock, and to `corrupt` when a stored payload
fails to revalidate. `is_authoritative` is `true` ONLY when `effective_status`
is exactly `observed`; `simulated` (paper), `unsupported` (market-data-only),
`error`, `stale`, `clock_error`, and `corrupt` are never authoritative.
`balances` and `open_positions` are honest NULLs — never fabricated and cleared
only when the state is corrupt. A stale row can retain labeled last-known
values. Balance and positions are
independent reads, each carrying its own `*_observed_at` timestamp and
`*_payload_source_observation_id` provenance. Values are venue-native only
(`valuation_status` = `native_only`); no currency conversion is synthesized.

The strict `reconciliation` object is always present. With no persisted
reconciliation state it has `effective_status="incomplete"`, is not
authoritative, and carries no evidence. For a persisted state, the read surface
revalidates its method config, referenced observations, latest-observation
lineage, and open drift episode before returning it. Any inconsistency yields
`effective_status="corrupt"`, `is_authoritative=false`, and clears all evidence.
A valid verdict becomes `clock_error` when its evaluation stamp is more than
five seconds in the future and `stale` after 900 seconds. Only a fresh, fully
revalidated current `matched` or `mismatched` verdict is authoritative. Stale
views retain their validated `expected`, `actual`, `difference`, and
`tolerance` JSON objects and continue to surface an open drift episode; raw
persisted JSON strings are never exposed.

## Portfolio P&L

The P&L surfaces require `read:positions`; the current `viewer` set contains
this permission. They never mutate trading, review, or portfolio-projection
resources. A current-horizon request that omits `as_of` may atomically persist
the wallet/mode/currency scope's missing immutable activation anchor only when
the caller also has access to that wallet through the trade-scope resolver.
A personal read grant alone can read existing P&L but cannot create the anchor;
without one, it receives an empty series. Supplying `as_of` disables anchor
creation and performs a strict historical read; when no anchor was visible at
that horizon, the series is empty. Both routes use the same readable-wallet
scope as positions. An `operator_public_id` filter narrows the operator-grant
half of that scope; the caller's personal read grants remain included.

### GET /api/portfolio/pnl/series

Reconstruct one wallet and mode's net-P&L-since-activation series. The response
decomposes realized, fee, accrual, unrealized, and net P&L at the requested
granularity, with per-exchange and per-instrument attribution. Normal
reconstruction uses the response's bitemporal `as_of` horizon for source reads;
first activation additionally certifies independent request-horizon and
activation-minute execution-prefix cuts before persisting the anchor.

### GET /api/portfolio/pnl/timeline

Return the same P&L series plus bounded attribution markers for fills,
signals, and AI-review decisions. Its marker overlay is read-only and explains
when trading and review events affected the portfolio; it does not grant
authority to mutate any of those resources. At a historical horizon with no
visible activation anchor, the empty series may still carry independently read
markers.

**Query parameters shared by both routes:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `wallet_public_id` | string | yes | Single wallet scope; 403 when inaccessible |
| `operator_public_id` | string | no | Optional operator scope; 403 when inaccessible |
| `mode` | string | no | Trading mode, default `live` |
| `granularity` | string | no | `1m`, `5m`, `1h`, or `1d`; default `1m` |
| `from` | datetime | yes | Inclusive UTC window start |
| `to` | datetime | yes | Inclusive UTC window end |
| `as_of` | datetime | no | Explicit bitemporal knowledge horizon; disables missing-anchor creation |
| `valuation_ccy` | string | no | Three-letter valuation currency, default `USD` |

Invalid or excessive windows return 400. Both routes return 403 for a wallet
outside the caller's permission-derived accessible set.

### POST /api/execution-plans

Create a bracket (SL/TP) execution plan on an open position cycle. Requires
`create:orders` permission.

**Request:**

```http
POST /api/execution-plans
Content-Type: application/json
X-CSRF-Token: <csrf_token>
```

**Body (BracketCreateCommand envelope):**

```json
{
    "type": "create_bracket_command",
    "public_id": "019e1a2b-0000-7000-8000-000000000001",
    "session_id": "ui-session-1",
    "sequence_id": 1,
    "timestamp": "2026-04-12T18:00:00Z",
    "payload": {
        "position_cycle_public_id": "019e1a2b-4d5e-7f6a-8b9c-0d1e2f3a4b5c",
        "sl_price": 48000.0,
        "tp_price": 55000.0
    }
}
```

At least one of `sl_price` or `tp_price` is required.

**Response (200):** `ExecutionPlanResponse` wrapping the new armed bracket plan.

**Errors:** 422 (invalid params, missing capability, no truthful live position for
the cycle, position direction/exchange disagreeing with the cycle, ambiguous
cycle link, or a position with an honest-NULL entry price — SL/TP sanity cannot
be validated without one), 409 (cycle not open, duplicate), 403 (wallet
inaccessible), 503 (executor unavailable).

### POST /api/execution-plans/{plan_public_id}/cancel

Cancel a bracket execution plan. Armed brackets transition to cancelled directly.
Active brackets transition to cancel_requested with cancel TradeCommands emitted.
Requires `cancel:orders` permission.

**Request:**

```http
POST /api/execution-plans/{plan_public_id}/cancel
Content-Type: application/json
X-CSRF-Token: <csrf_token>
```

**Response (200):** `ExecutionPlanResponse` wrapping the updated plan.

**Errors:** 404 (not found), 409 (already terminal or concurrent cancel).

### GET /api/execution-plans/{plan_public_id}

Retrieve a single execution plan by public_id. Requires `read:orders` permission.

**Response (200):** `ExecutionPlanResponse` wrapping the plan.

### GET /api/execution-plans/{plan_public_id}/decisions

List decision audit rows for an execution plan. Requires `read:orders` permission.

**Response (200):** List of decision rows with action, reason, and timestamp.

### POST /api/trailing-stops

Create a trailing stop execution plan on an open position cycle. The trailing
stop ratchets the stop price as the market moves favorably and triggers a
reduce_only market close on breach.

**Request body:**

- `position_cycle_public_id` (string, required): Target position cycle.
- `trailing_pct` (float, required): Trailing distance as percentage (0 < x < 100).
- `min_lock_pct` (float, optional, default 0): Minimum profit % before trailing activates.
- `idempotency_key` (string, optional): Dedup key for retries.

**Permission:** `create:orders`. CSRF token required.

**Response (200):** `ExecutionPlanResponse` with `plan_type: "trailing_stop"`, `status: "armed"`.

**Response (409):** Cycle not open or duplicate trailing stop on same cycle.

**Response (422):** Invalid params, missing capability, no truthful live
position for the cycle (the historical peak-quantity fallback is not used), or
a position without a usable entry price.

### POST /api/trailing-stops/{plan_public_id}/cancel

Cancel a trailing stop. Armed stops transition directly to cancelled. Active
stops (with in-flight child orders) transition to cancel_requested.

**Permission:** `cancel:orders`. CSRF token required.

### GET /api/trailing-stops/{plan_public_id}

Retrieve a trailing stop plan by public_id. Only returns plans with
`plan_type: "trailing_stop"`.

**Permission:** `read:orders`.

### GET /api/trailing-stops/{plan_public_id}/decisions

List decision audit rows for a trailing stop plan.

**Permission:** `read:orders`.

### GET /api/trailing-stops/by-cycle/{cycle_public_id}

Get live trailing stop state for a position cycle. Returns peak_price and
current_stop from the evaluator's in-memory state.

**Permission:** `read:orders`.

**Response (200):** `TrailingStopStateResponse` with live state, or
`{ "type": "message", "payload": "none" }` if no active trailing stop.

**Response (404):** Position cycle not found.

## Paired Execution

The paired-execution operator surface exposes halted or exposed multi-leg
groups and the manual attestation used after an operator resolves a
`manual_intervention` group at the venue. Callers lacking effective
`impersonate:operator` are filtered to their accessible wallets; callers with
that effective permission are unscoped.

### GET /api/paired-execution/incidents

List halted or currently exposed paired-execution scopes visible to the caller.
Each incident is keyed by `(wallet_public_id, strategy_id, group_key)` and may
contain an active durable halt, exposed groups, or both. `halt_missing: true`
marks the anomalous window where an exposed group exists before the scanner has
restored the durable halt row.

**Permission:** `read:positions`.

**Response (200):** `PairedExecutionIncidentListResponse` with
`paired_execution_incident` payload items. Each item includes the scope,
optional `halt`, `halt_missing`, and `groups`; each group contains per-leg
signed exposure with `open_qty = filled_signed_qty - compensated_signed_qty`.

**Response (503):** Paired-execution tables are unavailable for the active
repository backend.

### POST /api/paired-execution/groups/{group_public_id}/terminalize

Attest that a paired-execution group in `manual_intervention` (or a
`compensating` group reopened onto a manual leg) has been resolved at the
venue. The repository completes the group; the guard scanner clears the
scope's durable halt and in-memory mirrors within one scan cycle.

**Permission:** `manage:paired_execution`. CSRF token required for
cookie-authenticated requests.

**Response (200):** `PairedGroupTerminalizeResponse` carrying the completed
group projection and its legs' true accounting.

**Response (404):** No current active group exists with that id, or the group
belongs to a wallet outside the caller's accessible set.

**Response (409):** The group is not currently attestable because its status is
not manual, it has no legs or a held original command, or a sibling leg still
has automation in flight.

**Response (503):** Paired-execution tables are unavailable for the active
repository backend.

### GET /api/position-cycles/open

List all open position cycles with age information. Requires `manage:users`
for diagnosing orphaned cycles (open cycles without a matching trading
engine).

**Query parameters:**

- `min_age_hours` (float, optional, default 0): Only return cycles open longer than this.

**Permission:** `manage:users`, which is present only in the current `admin`
set.

**Response (200):** `PositionCycleListResponse` envelope whose payload
items carry `cycle_public_id`, `shard_key`, `instrument_public_id`,
`exchange`, `mode`, `wallet_public_id`, `operator_public_id`, `direction`, `max_qty`
(per-cycle peak, not lifetime), `opened_at`, and `age_hours`.

### POST /api/position-cycles/close-orphan

Close a specific orphaned position cycle. The operator should verify via
`GET /open` that the cycle is genuinely orphaned before calling this.

**Query parameters:**

- `cycle_public_id` (string, required): Public ID of the cycle to close.

**Permission:** `manage:users`, which is present only in the current `admin`
set. CSRF token required.

**Response (200):** `OrphanSweepResponse` envelope with payload
`{ "closed_count": 1, "closed_cycle_ids": ["<id>"] }`.

**Response (404):** No active open cycle found for the given public_id.

### POST /api/position-cycles/sweep-orphans

Bulk-close all open position cycles older than `min_age_hours`. Default
threshold is 72 hours (3 days). Review via `GET /open?min_age_hours=72`
before running.

**Query parameters:**

- `min_age_hours` (float, optional, default 72, minimum 1): Minimum age threshold.

**Permission:** `manage:users`, which is present only in the current `admin`
set. CSRF token required.

**Response (200):** `OrphanSweepResponse` envelope with payload
`{ "closed_count": N, "closed_cycle_ids": [...] }`.

**Response (400):** `min_age_hours` must be at least 1.

### GET /api/exchanges

List distinct exchange names from active symbol aliases. Requires
`read:market_data` permission.

**Request:**

```http
GET /api/exchanges
```

**Response (200):**

```json
{
    "type": "exchange_list",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": ["kraken", "polygon", "walutomat"],
    "count": 3
}
```

### GET /api/exchanges/{exchange}/instruments

List distinct native symbols available on a given exchange. Requires
`read:market_data` permission.

**Request:**

```http
GET /api/exchanges/kraken/instruments
```

**Response (200):**

```json
{
    "type": "instrument_list",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": ["BTC-USD", "ETH-USD", "SOL-USD"],
    "count": 3
}
```

### GET /api/exchanges/{exchange}/instruments/detail

List capability-aware instrument rows for an exchange. Requires
`read:market_data`. Each row includes the native symbol plus trade and
market-data capability flags, instrument kind, and expiry metadata so
clients can show market-data-only instruments without a second request.

### GET /api/instruments/{exchange}/{native_symbol}/related

Return related instruments for a concrete exchange/native-symbol pair.
Requires `read:market_data`. Used by cross-asset and continuous-contract
UI flows to navigate from an instrument to its configured underlying,
front month, and sibling contracts.

### GET /api/status

System-wide status including trader process, backtests, and active
strategies. Requires `read:system_status` permission.

**Request:**

```http
GET /api/status
```

**Response payload excerpt (200):**

The actual response is a `SystemStatusResponse` envelope with provenance
fields and this object under `payload`.

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
```

**Response payload excerpt (200):**

The actual response is a `WsStatsResponse` envelope with provenance
fields and this object under `payload`.

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
        "available_topics": ["market.", "signals.", "system.egress.", "system.heartbeats.", "admin.", "orders.commands.", "orders.events.", "accruals.", "backtest.", "alerts.", "portfolio.accounts.", "plans.decisions.", "ai_reviews.", "ai_research.", "processes.events.summary.", "processes.events.configured.", "processes.events.runs.", "strategies.events.list."]
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
        "heartbeat_interval_ms": 1000
    }
}
```

### GET /api/zmq/health

ZMQ bridge health check. Requires `read:system_status` permission.

**Request:**

```http
GET /api/zmq/health
```

**Response payload excerpt (200):**

The actual response is a `ZmqHealthResponse` envelope with provenance
fields and this object under `payload`.

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
        "available_topics": ["market.", "signals.", "system.egress.", "system.heartbeats.", "admin.", "orders.commands.", "orders.events.", "accruals.", "backtest.", "alerts.", "portfolio.accounts.", "plans.decisions.", "ai_reviews.", "ai_research.", "processes.events.summary.", "processes.events.configured.", "processes.events.runs.", "strategies.events.list."]
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

### GET /api/market/cache/health

Diagnostic snapshot of the in-process market cache, configured pair
stats, and persist-policy universe. Requires `read:market_data`.
Returns counts for cached instruments, cached pairs, and persisted
instrument universe size.

### GET /api/market/cache/stats/configured

Return cached Pearson and cointegration stats for every configured
market-stats pair. Requires `read:market_data`. Configured-but-cold
pairs return placeholders with `is_warm=false` so dashboards can render
stable "computing" rows.

### GET /api/market/cache/stats/{exchange_a}/{symbol_a}/{exchange_b}/{symbol_b}

Return cached stats for one configured pair. Requires
`read:market_data`. Unknown exchanges return `400`; unconfigured pairs
return `404`; configured-but-not-yet-computed pairs return `200` with
`is_warm=false`.

### GET /api/market/coverage

Per-exchange market-data coverage over active instruments. Requires
`read:system_status`.

**Query parameters:**

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `tick_window_seconds` | int | Freshness window for ticks (default 600) |
| `candle_window_seconds` | int | Freshness window for candles (default 1800) |

The payload includes one row per exchange with instrument count,
`fresh_ticks`, `fresh_candles`, `gated_off`, and `dark` counts.

### GET /api/market/feed-health

Current per-symbol feed-health rows. Requires `read:system_status`.

**Query parameters:**

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `exchange` | string | Optional lowercase exchange filter |
| `fresh_within_seconds` | int | Optional staleness filter; older snapshots are dropped |

## Process Management

Process endpoints manage background services (feeds, strategies, executors,
brokers). Read endpoints require `read:processes`; the lightweight summary
uses `read:system_status` because it is part of the health surface.
Non-strategy mutations require `manage:processes`. Strategy targets use the
narrow permissions `configure:strategies`, `start:strategies`, and
`stop:strategies` according to the requested operation. The current `operator`
and `admin` sets contain all three strategy permissions; `viewer` contains the
read permissions and no process or strategy mutation permission.

### GET /api/processes/available

List registered process templates that can be instantiated. Requires
`read:processes` permission.

**Response (200):**

```json
{
    "type": "available_processes",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": [
        {
            "type": "available_process",
            "sequence_id": 1,
            "public_id": "<uuid7>",
            "timestamp": "2026-01-18T12:00:00Z",
            "session_id": "<server-session>",
            "topic": null,
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
`read:processes` permission.

**Response (200):**

```json
{
    "type": "configured_processes",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": [
        {
            "type": "configured_process",
            "sequence_id": 1,
            "public_id": "<uuid7>",
            "timestamp": "2026-01-18T12:00:00Z",
            "session_id": "<server-session>",
            "topic": null,
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
            "active_public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
            "kind": "instance",
            "wallet_public_id": null,
            "parent_template": null,
            "template": "zmq_broker",
            "coordinator": "coord-0",
            "managed_remotely": false
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
    "type": "process_summary_response",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": {
        "type": "process_summary",
        "sequence_id": 1,
        "public_id": "<uuid7>",
        "timestamp": "2026-01-18T12:00:00Z",
        "session_id": "<server-session>",
        "topic": null,
        "coordinator": "coord-0",
        "feeds": { "running": 2, "total": 3 },
        "strategies": { "running": 1, "total": 2 },
        "executors": { "running": 1, "total": 1 },
        "brokers": { "running": 1, "total": 1 },
        "processes": []
    }
}
```

### POST /api/processes

Create a new process configuration from a registered template. A non-strategy
template requires `manage:processes`. A strategy template requires
`configure:strategies`; creating it enabled also requires `start:strategies`.
Returns 201 on success.

**Request:**

```http
POST /api/processes
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "type": "process_create_request",
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "payload": {
        "name": "kraken_feed_btc",
        "template": "kraken_feed_publisher",
        "enabled": true,
        "mode": "thread",
        "parameters": { "symbols": ["BTC-USD"] },
        "note": "Kraken BTC feed"
    }
}
```

**Response (201):**

```json
{
    "type": "process_create_response",
    "public_id": "<uuid7>",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": {
        "type": "process_create",
        "public_id": "<uuid7>",
        "session_id": "<server-session>",
        "sequence_id": 1,
        "timestamp": "2026-01-18T12:00:00Z",
        "topic": null,
        "status": "created",
        "process": {
            "name": "kraken_feed_btc",
            "template": "kraken_feed_publisher"
        }
    }
}
```

### GET /api/processes/schema/{name}

Get the configuration schema and defaults for a registered process template.
Requires `read:processes` permission.

**Response (200):**

```json
{
    "type": "process_schema_response",
    "public_id": "<uuid7>",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": {
        "type": "process_schema",
        "public_id": "<uuid7>",
        "session_id": "<server-session>",
        "sequence_id": 1,
        "timestamp": "2026-01-18T12:00:00Z",
        "topic": null,
        "name": "kraken_feed_publisher",
        "description": "Kraken market data feed publisher",
        "class_path": "snapper.messaging.publishers.kraken.KrakenMarketDataPublisher",
        "method": "start",
        "default_enabled": true,
        "default_mode": "thread",
        "default_parameters": {},
        "lifecycle": "long_running"
    }
}
```

### POST /api/processes/{name}/start

Start a configured process. A strategy target requires `start:strategies`; a
non-strategy target requires `manage:processes`.

**Request:**

```http
POST /api/processes/zmq_broker/start
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "type": "process_start_request",
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "payload": {
        "mode": "process"
    }
}
```

All `payload` fields are optional. `mode` overrides the stored execution mode
for this run only; `parameters` supplies run-only constructor overrides.
Neither field persists to Settings, and omitting either value uses the stored
configuration. Strategy processes reject start-time `parameters` so their
operator/wallet/output scope check always runs against persisted launch
parameters. `operator_public_id` and `wallet_public_id` can never be
overridden at start time. Executor templates cannot be started directly; start
the generated `executor_<exchange>_w<wallet_short>` instance instead.

**Response (200):**

```json
{
    "type": "process_start_response",
    "public_id": "<uuid7>",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": {
        "type": "process_start",
        "public_id": "<uuid7>",
        "session_id": "<server-session>",
        "sequence_id": 1,
        "timestamp": "2026-01-18T12:00:00Z",
        "topic": null,
        "status": "success",
        "name": "zmq_broker",
        "process_public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
        "message": "Process started"
    }
}
```

### POST /api/processes/{name}/stop

Stop a running process. A strategy target requires `stop:strategies`; a
non-strategy target requires `manage:processes`.

**Request:**

```http
POST /api/processes/zmq_broker/stop
X-CSRF-Token: <csrf_token>
```

**Response (200):**

```json
{
    "type": "process_stop_response",
    "public_id": "<uuid7>",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": {
        "type": "process_stop",
        "public_id": "<uuid7>",
        "session_id": "<server-session>",
        "sequence_id": 1,
        "timestamp": "2026-01-18T12:00:00Z",
        "topic": null,
        "status": "success",
        "name": "zmq_broker",
        "message": "Process stopped"
    }
}
```

### PATCH /api/processes/{name}/desired-state

Set the persistent desired state (`enable`, `disable`, or `restart`) of the
`process_<name>` config — the source of truth for the reconcile loop. This
NEVER starts or stops anything locally, so it is safe for a process owned by
another container: the owning coordinator's reconcile loop converges the
running state to what is written here. A non-strategy target requires
`manage:processes`. For a strategy, `enable` requires `start:strategies`,
`disable` requires `stop:strategies`, and `restart` requires both permissions.
Cookie-authenticated requests also require CSRF.

**Request:**

```http
PATCH /api/processes/strategy_rsi_btc_1h/desired-state
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "type": "process_desired_state_request",
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "payload": {
        "action": "restart",
        "restart_nonce": "01960a7e-2c1a-7c00-8000-000000000000"
    }
}
```

`action` is `enable`, `disable`, or `restart`. `restart_nonce` is a
client-minted idempotency token (pattern `^[A-Za-z0-9_-]+$`, length 8-64),
**required** for `restart` and ignored otherwise; re-sending the same nonce
does not double-bounce the process. Enabling a strategy re-runs the
operator/wallet/grant scope check (fail-closed) before the write.

**Response (200):**

```json
{
    "type": "process_desired_state_response",
    "public_id": "<uuid7>",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": {
        "type": "process_desired_state",
        "public_id": "<uuid7>",
        "session_id": "<server-session>",
        "sequence_id": 1,
        "timestamp": "2026-01-18T12:00:00Z",
        "topic": null,
        "status": "success",
        "name": "strategy_rsi_btc_1h",
        "action": "restart",
        "coordinator": "coord-2",
        "managed_remotely": true,
        "message": "Desired state persisted; coord-2 will reconcile"
    }
}
```

**Errors:** 404 (per-wallet executor instance or unknown process), 422 (bare
executor template, or a `restart` without `restart_nonce`), 409 (`restart` of a
disabled process), 403/400 (strategy operator/wallet/grant scope denied).

### PATCH /api/processes/{name}/config

Retarget an existing **strategy** config's scope (operator / wallet /
AI-reviewer reference-identity params) without hand-editing the config JSON.
Requires `configure:strategies` and CSRF for cookie auth. The endpoint does not
restart the process: the response carries `restart_required: true` (surfaced by
the UI as a "restart required" banner), and the change applies on the next
start or restart. Authorization is fully enforced at edit time against the
caller's principal; it does not defer cross-tenant checks to the trusted start
resolver.

**Request:**

```http
PATCH /api/processes/strategy_rsi_btc_1h/config
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "type": "process_config_scope_request",
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "payload": {
        "operator_public_id": "019d6ca4-...",
        "wallet_public_id": "019d7e9a-...",
        "reference_identity_params": {
            "ai_review_user_public_id": "label:reviewer"
        }
    }
}
```

All payload fields are optional; an omitted field is left unchanged, and an
empty-string `operator_public_id` clears it. `operator_public_id` /
`wallet_public_id` accept either a concrete public_id or a `label:<name>`
reference. The scope is authorized at edit time against the caller's principal: a
`label:` reference is resolved scope-qualified to the caller's own operator
memberships (and, for a wallet, the resolved operator's accessible wallets of the
strategy's paper/live mode), so it can never resolve outside the caller's
authority. A label that resolves to exactly one in-scope entity is accepted; a
blank, unresolved, or ambiguous label is rejected with 400, and a concrete
operator the caller has no membership on is rejected with 403. Values are
persisted **verbatim** — a `label:` reference is stored as-is and resolved to a
concrete UUID only at the next (re)start (the authorization check runs on a
throwaway resolved copy). `reference_identity_params` values may likewise be
`label:<name>` references (e.g. `label:<username>` for the AI reviewer); their
keys must be reference-identity params the strategy **declares** (whitelisted
against the registry; an undeclared key returns 400).

**Response (200):**

```json
{
    "type": "process_config_scope_response",
    "public_id": "<uuid7>",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": {
        "type": "process_config_scope",
        "public_id": "<uuid7>",
        "session_id": "<server-session>",
        "sequence_id": 1,
        "timestamp": "2026-01-18T12:00:00Z",
        "topic": null,
        "status": "success",
        "name": "strategy_rsi_btc_1h",
        "parameters": {
            "operator_public_id": "019d6ca4-...",
            "wallet_public_id": "019d7e9a-...",
            "params": { "ai_review_user_public_id": "label:reviewer" }
        },
        "restart_required": true
    }
}
```

**Errors:** 404 (per-wallet executor instance or not configured), 422 (bare
executor template), 400 (not a strategy, undeclared reference-identity key,
blank/unresolved/ambiguous operator or wallet label, or wallet resolution
failure), 403
(operator not a member / missing wallet grant / output not covered).

### GET /api/processes/runs

List historical process runs. Requires `read:processes` permission.

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `limit` | int | no | Number of runs to return (default 50) |
| `name` | string | no | Filter by process name |

**Response (200):**

```json
{
    "type": "process_runs",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T18:00:00Z",
    "topic": null,
    "payload": [
        {
            "type": "process_run",
            "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
            "sequence_id": 1,
            "timestamp": "2026-01-18T18:00:00Z",
            "session_id": "<server-session>",
            "topic": null,
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
`read:strategies` permission.

**Response (200):**

```json
{
    "type": "strategy_list",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": [
        {
            "type": "strategy_process",
            "public_id": "<uuid7>",
            "session_id": "<server-session>",
            "sequence_id": 1,
            "timestamp": "2026-01-18T12:00:00Z",
            "topic": null,
            "name": "strategy_rsi_btc_1h",
            "running": true,
            "enabled": true,
            "mode": "process",
            "strategy_class": "RSIReversion"
        }
    ],
    "count": 1
}
```

Strategy mutations use the process routes described above:
`configure:strategies` controls creation and scope edits,
`start:strategies` controls starts and enables, and `stop:strategies` controls
stops and disables. A restart requires both start and stop permissions, and
creating an enabled strategy requires both configure and start permissions.

## Settings

Settings-management endpoints require `configure:system` permission
(present only in the current `admin` set). `GET /api/settings/features` is
the public exception.

### GET /api/settings

List all settings, optionally filtered by category.

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `category` | string | no | Filter by setting category |
| `as_of` | datetime | no | Point-in-time timestamp for temporal setting reads |

**Response (200):**

```json
{
    "type": "setting_list",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": [
        {
            "type": "setting_read",
            "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5c",
            "session_id": "<server-session>",
            "sequence_id": 1,
            "timestamp": "2026-01-18T12:00:00Z",
            "topic": null,
            "key": "polygon_api_key",
            "value": "IvLG...",
            "category": "api",
            "description": "Polygon.io market-data API key",
            "updated_at": "2026-01-15T10:00:00Z",
            "updated_by": "admin"
        }
    ],
    "count": 1
}
```

Note: per-wallet trading credentials (kraken, walutomat,
kraken_futures) are NOT exposed through the settings endpoints. They
live in the `wallet_credentials` table and are managed via the
dedicated `/api/wallets/{wallet_public_id}/credentials*` routes
(`src/snapper/server/credential_routes.py` — `GET` for the active
summaries, `POST` for create, and `POST` rotate for replacing an
existing credential row). Seed files
(`dev.toml` / `prod.toml` — see
[Configuration / Wallet Credentials](configuration.md#wallet-credentials))
remain the bootstrap source for dev/prod parity.

### GET /api/settings/features

Public feature-flag projection used by the frontend before auth-gated
navigation renders. No authentication required. Currently exposes
`ai_integration_enabled`; disabled AI integration also makes `/api/mcp`
and `/api/ai-delegates/*` return the shared `feature_disabled` envelope.

### GET /api/settings/categories

List distinct setting category names.

**Parameters:**

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `as_of` | datetime | no | Point-in-time timestamp for temporal category reads |

**Response (200):**

```json
{
    "type": "setting_categories",
    "public_id": "019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": ["exchanges", "strategy", "system"],
    "count": 3
}
```

### GET /api/settings/push-beta/users

Return the active push-beta gate configuration. Requires
`configure:system`. If the underlying `push_beta_config` setting is
absent or malformed, the endpoint returns the default disabled gate with
an empty allowlist so a permission-bearing caller can see the effective
routing state.

### POST /api/settings/push-beta/users

Replace the push-beta gate configuration in one call. Requires
`configure:system` and CSRF for cookie auth. The request body is
`UpdatePushBetaUsersCommand`; `user_public_ids` becomes the complete
allowlist after the write, so callers should read, edit locally, then
submit the full desired set.

**Request:**

```http
POST /api/settings/push-beta/users
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "type": "update_push_beta_users_command",
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "payload": {
        "enabled": true,
        "user_public_ids": ["019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b"]
    }
}
```

**Response (200):**

```json
{
    "type": "push_beta_config_response",
    "public_id": "<uuid7>",
    "session_id": "<server-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "payload": {
        "type": "push_beta_config_read",
        "public_id": "<uuid7>",
        "session_id": "<server-session>",
        "sequence_id": 1,
        "timestamp": "2026-01-18T12:00:00Z",
        "topic": null,
        "enabled": true,
        "user_public_ids": ["019e1a2b-3c4d-7e5f-8a9b-0c1d2e3f4a5b"]
    }
}
```

### POST /api/settings/{key}/set

Set (create or update) a setting value.

**Request:**

```http
POST /api/settings/polygon_api_key/set
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "type": "setting_update",
    "public_id": "<uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "payload": {
        "value": "new-api-key-value",
        "category": "api",
        "description": "Polygon.io market-data API key"
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
    "topic": null,
    "payload": {
        "type": "setting_read",
        "public_id": "<uuid7>",
        "session_id": "<server-session>",
        "sequence_id": 1,
        "timestamp": "2026-01-18T12:00:00Z",
        "topic": null,
        "key": "polygon_api_key",
        "value": "new-api-key-value",
        "category": "api",
        "description": "Polygon.io market-data API key",
        "updated_at": "2026-01-18T12:00:00Z",
        "updated_by": "admin"
    }
}
```

### POST /api/settings/{key}/remove

Soft-delete a setting by key (sets `known_to` to current time).

**Request:**

```http
POST /api/settings/polygon_api_key/remove
Content-Type: application/json
X-CSRF-Token: <csrf_token>

{
    "type": "remove_setting_request",
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
    "topic": null,
    "payload": "Setting 'polygon_api_key' deleted successfully"
}
```

## AI Delegates

AI delegate management is gated by `ai_integration_enabled`, the same feature
flag used by `/api/mcp`. When disabled, `/api/ai-delegates/*` returns the
shared `feature_disabled` envelope. Delegate tokens are bearer-only automation
credentials for MCP-compatible clients and are returned exactly once, on
creation.

### POST /api/ai-delegates

Create an AI delegate owned by the authenticated caller and mint a long-lived
access JWT.

**Permission:** `manage:ai_integration`. CSRF token required for
cookie-authenticated requests.

**Body (`DelegateCreateRequest` envelope):**

```json
{
    "type": "delegate_create_request",
    "public_id": "<client-uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "payload": {
        "label": "research-agent",
        "operator_public_id": null,
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
            "max_order_quantity_per_instrument": null,
            "max_open_orders": 2,
            "max_daily_notional_usd": 1000.0,
            "max_cancels_per_minute": 5
        }
    }
}
```

When supplied, `operator_public_id` must be one of the caller's operator
memberships unless its effective grant contains `impersonate:operator`; that
structural permission allows any explicit operator. A `null` value uses the
caller's primary operator. All caps are optional; `null` means the delegate
inherits the Snapper-wide fallback for that cap.

`permissions` is optional and chooses the delegate access token's actual
grant. Omission keeps the complete `ai_delegate` named permission set. Every
requested permission must belong to that set; the server rejects a superset
with 422.

**Response (200):** `DelegateCreatedResponse` with the delegate projection and
one-shot `access_token`. List and detail endpoints never re-serve the token.

**Response (409):** The label cannot produce a unique delegate username, or the
owner has reached the per-owner delegate cap.

**Response (422):** The requested operator binding is outside the caller's
claim set, or the requested token permission scope exceeds the
`ai_delegate` named set.

### GET /api/ai-delegates

List active delegates visible to the caller. A token with
`manage:ai_integration` keeps the creator-owned management view; a read-only
token is narrowed to delegates bound to the caller's operator memberships.
Deactivated delegates are omitted.

**Permission:** `read:ai_integration`.

**Response (200):** `DelegateListResponse` with `DelegateRead` payload items.

### GET /api/ai-delegates/{delegate_public_id}

Fetch one active delegate in the caller's permission-appropriate scope. A
management token uses creator ownership; a read-only token uses operator
memberships. An absent or out-of-scope identifier returns 404.

**Permission:** `read:ai_integration`.

**Response (200):** `DelegateResponse`.

**Response (404):** The delegate does not exist or is outside the caller's
permission-appropriate scope.

### PATCH /api/ai-delegates/{delegate_public_id}

Replace a delegate's trading caps while preserving username and label
immutability. The write is SCD2-preserved so cap history remains auditable.

**Permission:** `manage:ai_integration`. CSRF token required for
cookie-authenticated requests.

**Body (`DelegateCapsUpdateRequest` envelope):**

```json
{
    "type": "delegate_caps_update_request",
    "public_id": "<client-uuid7>",
    "session_id": "<client-session>",
    "sequence_id": 2,
    "timestamp": "2026-01-18T12:01:00Z",
    "payload": {
        "caps": {
            "max_order_quantity_per_instrument": null,
            "max_open_orders": 1,
            "max_daily_notional_usd": 500.0,
            "max_cancels_per_minute": 3
        }
    }
}
```

**Response (200):** `DelegateResponse` with the updated caps.

**Response (404):** The delegate does not exist or is not owned by the caller.

### POST /api/ai-delegates/{delegate_public_id}/deactivate

Deactivate a delegate using the shared user kill-switch flow. The server closes
the active delegate row, revokes active tokens, and publishes the same
deactivation event used by admin user deactivation.

**Permission:** `manage:ai_integration`. CSRF token required for
cookie-authenticated requests.

**Body:** Optional `DelegateDeactivateRequest` envelope with
`payload.reason` for audit context.

**Response (200):** `DelegateResponse` with `is_active: false`.

**Response (404):** The delegate does not exist or is not owned by the caller.

## Underlying Assets

### GET /api/underlyings

List all underlying assets with instrument counts.

```
GET /api/underlyings?as_of=2026-06-20T12:00:00Z
```

Returns `PayloadListResponse` with `UnderlyingAssetData` items (ticker, name,
asset_class, sector, instrument_count).

### GET /api/underlyings/{ticker}/instruments

List instruments mapped to an underlying asset.

```
GET /api/underlyings/SPX/instruments?relationship_type=derivative
```

Query parameters:

- `relationship_type` (optional): Filter by `exact`, `derivative`, or `proxy`
- `as_of` (optional): Point-in-time query timestamp

### GET /api/underlyings/{ticker}/front-month

Return the front-month (nearest non-expired) futures contract for an underlying.

```
GET /api/underlyings/SPX/front-month?exchange=kraken_equities&contract_family=ES
```

Query parameters:

- `exchange` (optional): Filter by exchange
- `contract_family` (optional): Filter by product root (e.g., `ES` vs `MES`)
- `as_of` (optional): Point-in-time query timestamp

Returns `PayloadResponse` with `FrontMonthData` (instrument_public_id,
native_symbol, exchange, expiry_at, relationship_type, contract_family).
Returns 404 if no active futures contracts exist.

### GET /api/underlyings/{ticker}/contracts

List all futures contracts for an underlying asset.

```
GET /api/underlyings/SPX/contracts?include_expired=true&contract_family=ES
```

Query parameters:

- `exchange` (optional): Filter by exchange
- `contract_family` (optional): Filter by product root
- `include_expired` (optional, default false): Include expired contracts
- `as_of` (optional): Point-in-time query timestamp

Returns `PayloadListResponse` with `ContractData` items. Each item includes
`is_front_month` (true for nearest non-expired within same contract family).

### GET /api/underlyings/{ticker}/continuous

Build a continuous futures series for an underlying from active contract
metadata and candle rows.

```
GET /api/underlyings/SPX/continuous?exchange=kraken_equities&contract_family=ES&timeframe=1d&start=2026-01-01T00:00:00Z&end=2026-01-05T00:00:00Z
```

Query parameters:

- `exchange` (required): Exchange for the contract ladder
- `contract_family` (required): Product root, e.g. `ES` vs `MES`
- `timeframe` (required): Candle timeframe to load
- `start` (required): Inclusive UTC start timestamp
- `end` (required): Exclusive UTC end timestamp
- `method` (optional, default `panama`): Adjustment method,
  `unadjusted`, `ratio`, or `panama`
- `rollover_days_before` (optional, default `0`): Roll contracts this
  many days before expiry, range `0..365`
- `as_of` (optional): Point-in-time query timestamp

Returns a continuous-series response with the selected contract windows
and candle points. A successful response is either a
`continuous_candle_list` (full series) or a `continuous_partial`
envelope; partial responses include `failed_roll` and
`message` fields describing the unavailable roll window. Returns 400
for invalid parameters and 404 when the underlying cannot be resolved.

## Multi-Tenant (Wallets, Operators, Scope Grants, Credentials)

Effective `impersonate:operator` provides global operator and wallet scope on
wallet-data read surfaces. Other principals see wallets covered by active
operator scope grants or their own active personal wallet read grants. Both
`viewer` and `operator` use this read scope, which can include a wallet even
without any operator membership. Trading and authorization-topology endpoints
continue to use the operator scope-grant plane; credential endpoints enforce
their own permissions independently.

### GET /api/wallets

List wallets accessible to the current principal. Effective
`impersonate:operator` exposes all active wallets; other callers see the union
of wallets covered by their operator set's active scope grants and their own
active personal read grants.

### GET /api/operators

List operators accessible to the current principal. Effective
`impersonate:operator` exposes all operators; every other caller sees only
operators in `principal.operator_public_ids`. Optional `as_of` resolves the
operator catalogue at the same UTC history horizon used by the desk-membership
directory; authorization remains bounded by the caller's current signed scope.

### GET /api/scope-grants

List active scope grants on a given wallet. Callers lacking effective
`impersonate:operator` must have an active operator scope grant covering the
target wallet; a personal wallet read grant alone does not expose this
authorization topology. Otherwise the route returns 403. Required query
parameter: `wallet_public_id`.

### POST /api/scope-grants

Create a new scope grant. Requires `manage:scope_grants`, which is present
only in the current `admin` set. Returns 409 on overlap conflict. Body fields:
`operator_public_id`, `wallet_public_id`, `scope_kind` (`underlying` or
`instrument`), `underlying_public_id` or `instrument_public_id`, and optional
`note`.

### POST /api/scope-grants/handover

Atomic SCD2 close + insert transfer of a scope grant to a different
operator. Requires ``manage:scope_grants``. Body: ``from_grant_public_id``,
``to_operator_public_id``, optional ``reason``. Returns 404 if source
grant missing, 400 on self-handover, 409 on cross-scope overlap.

### POST /api/scope-grants/{grant_public_id}/revoke

Atomic SCD2 close (no replacement row) of an active scope grant + emits
`admin.scope_revoked` on the bus for live AI-delegate subscription
revalidation. Requires `manage:scope_grants`, which is present only in the
current `admin` set. Body: optional `reason` (flows to event payload, not to the closed
row). Returns 404 if the grant does not exist or is already closed
(double-revoke).

### POST /api/wallets

Create a new wallet. Requires `manage:wallet_credentials`, which is present
only in the current `admin` set. Returns 409 if `(label, is_paper)` active
unique index is violated. Body: `label`, optional `description`, and
`is_paper` (default false).

### GET /api/wallets/{wallet_public_id}/credentials

List active credentials on a wallet as summaries (no encrypted payload
on the wire). Requires `read:wallet_credentials`, which is present only in
the current `admin` set.

### POST /api/wallets/{wallet_public_id}/credentials

Create a new wallet credential. Requires ``manage:wallet_credentials``.
Plaintext ``credential_payload`` is Fernet-encrypted server-side before
DB insert. Body: ``exchange``, ``credential_type`` (``api_key_secret``,
``rsa_pem``, ``oauth``, ``paper``), required ``reconciliation_method``,
``credential_payload`` (dict), and optional ``label``. A real method config
is committed atomically with the credential; ``unclassified`` creates no
config row. Concrete adapter policy rejects unsupported methods, and paper
credentials require ``unclassified``. Returns 409 if ``(wallet, exchange)``
already exists or durable reconciliation history conflicts.

### PUT /api/wallets/{wallet_public_id}/credentials/{credential_public_id}/reconciliation-method

Idempotently classify an existing active live credential. Requires
``manage:wallet_credentials`` and CSRF validation. The body accepts one real
``reconciliation_method``. Kraken Futures permits only ``futures_position``;
Kraken Spot permits ``spot_execution_replay`` or ``margin_ledger_replay``;
Walutomat permits only ``spot_execution_replay``. Paper, market-data-only,
unreviewed, and unknown concrete adapters reject every real method. Returns
404 when the active credential does not belong to the path wallet and 409 when
reconciliation history makes a different method immutable.

### POST /api/wallets/{wallet_public_id}/credentials/{credential_public_id}/rotate

SCD2 close + insert rotation. Old credential row closed, new row
inserted with updated encrypted payload. Requires
``manage:wallet_credentials``. Returns 404 if credential not found.
Validates ``credential_payload`` against the existing credential type
before encrypting.

## WebSocket

### Connection and Authentication

The WebSocket endpoint is at `/api/ws`. Authentication is message-based,
not query-parameter-based, but the upgrade must already carry an access
JWT via `Authorization: Bearer <access_token>` or, for browser clients,
the `access_token` cookie. Bearer auth is checked first; the cookie is
the fallback. Missing or invalid upgrade auth is rejected with close code
`4401` before the `auth_required` challenge.

**Connection flow:**

1. Client connects to `ws://host:port/api/ws` (or `wss://` over TLS)
   with an access bearer token or active auth session cookie
2. Server validates the origin header and bearer/cookie auth
3. Server sends `auth_required` message
4. Client sends `authenticate` message with a WebSocket token
5. Server validates the token and sends `auth_ok`
6. Server sends `auth_complete` with topics allowed by the token's effective
    permissions
7. Client can now subscribe to topics

**Obtaining a WebSocket token:**

Call `POST /api/auth/ws_token` with an access token (Bearer header or
cookie auth). The route returns a short-lived, one-shot `ws_token`
without rotating the refresh-token pair. `POST /api/auth/refresh` can
also return `ws_token` during session rotation, but long-running clients
should prefer `/api/auth/ws_token`.

Client-originated control frames (`authenticate`, `reauth`,
`subscribe`, `unsubscribe`, `get_subscriptions`, `ping`) inherit
`StrictDataSchema`; clients must stamp `public_id`, `session_id`,
`sequence_id`, and `timestamp` before sending. `topic` defaults to
`null` and can be omitted on client frames.

### Authentication Messages

**Server sends after connection:**

```json
{
    "type": "auth_required",
    "public_id": "019e1a2b-0000-7000-8000-000000000901",
    "session_id": "<server-ws-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "timeout": 10
}
```

**Client sends to authenticate:**

```json
{
    "type": "authenticate",
    "public_id": "019e1a2b-0000-7000-8000-000000000a01",
    "session_id": "<client-ws-session>",
    "sequence_id": 1,
    "timestamp": "2026-01-18T12:00:00Z",
    "ws_token": "eyJhbGciOi..."
}
```

**Server sends on success:**

```json
{
    "type": "auth_ok",
    "public_id": "019e1a2b-0000-7000-8000-000000000902",
    "session_id": "<server-ws-session>",
    "sequence_id": 2,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "exp": "2026-01-18T12:30:00Z"
}
```

**Server sends session info:**

```json
{
    "type": "auth_complete",
    "public_id": "019e1a2b-0000-7000-8000-000000000903",
    "session_id": "<server-ws-session>",
    "sequence_id": 3,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "available_topics": [
        "ai_research.",
        "ai_reviews.",
        "alerts.",
        "backtest.",
        "market.",
        "orders.commands.",
        "orders.events.",
        "plans.decisions.",
        "portfolio.accounts.",
        "processes.events.configured.",
        "processes.events.runs.",
        "processes.events.summary.",
        "signals.",
        "strategies.events.list.",
        "system.egress.",
        "system.heartbeats."
    ],
    "user_role": "operator",
    "session_expires_at": "2026-01-18T12:15:00Z",
    "ws_token_exp": "2026-01-18T12:30:00Z"
}
```

`available_topics` contains the registry roots derived from the token's
effective permissions. Subscriptions may use a registry root such as
`market.` or a full concrete topic such as
`market.kraken.BTC-USD.candles.1h`.
Intermediate prefixes such as `market.kraken.` are rejected. Backtests
also allow scoped prefixes: `backtest.`, `backtest.{wallet_public_id}.`,
and `backtest.{wallet_public_id}.{run_public_id}.`, subject to permission and
wallet scope.

**Server sends on failure:**

```json
{
    "type": "auth_failed",
    "public_id": "019e1a2b-0000-7000-8000-000000000904",
    "session_id": "<server-ws-session>",
    "sequence_id": 4,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "reason": "Invalid token"
}
```

### Reauthentication

Before the WebSocket token expires, the server sends a `reauth_required`
message. Clients may also refresh proactively before that deadline. In both
cases they obtain a new one-shot `ws_token` via `POST /api/auth/ws_token` and
send it as a `reauth` message. Use `POST /api/auth/refresh` only when the HTTP
access/refresh token session itself needs rotation.

**Server warning:**

```json
{
    "type": "reauth_required",
    "public_id": "019e1a2b-0000-7000-8000-000000000905",
    "session_id": "<server-ws-session>",
    "sequence_id": 5,
    "timestamp": "2026-01-18T12:28:00Z",
    "topic": null,
    "deadline": "2026-01-18T12:29:00Z"
}
```

**Client sends new token:**

```json
{
    "type": "reauth",
    "public_id": "019e1a2b-0000-7000-8000-000000000a02",
    "session_id": "<client-ws-session>",
    "sequence_id": 2,
    "timestamp": "2026-01-18T12:28:30Z",
    "ws_token": "eyJhbGciOi...(new token)..."
}
```

**Server confirms:**

```json
{
    "type": "reauth_ok",
    "public_id": "019e1a2b-0000-7000-8000-000000000906",
    "session_id": "<server-ws-session>",
    "sequence_id": 6,
    "timestamp": "2026-01-18T12:28:30Z",
    "topic": null,
    "exp": "2026-01-18T13:00:00Z"
}
```

If the client does not reauthenticate in time:

```json
{
    "type": "auth_expired",
    "public_id": "019e1a2b-0000-7000-8000-000000000907",
    "session_id": "<server-ws-session>",
    "sequence_id": 7,
    "timestamp": "2026-01-18T12:30:00Z",
    "topic": null
}
```

### Subscription Management

**Subscribe to topics:**

```json
{
    "type": "subscribe",
    "public_id": "019e1a2b-0000-7000-8000-000000000a03",
    "session_id": "<client-ws-session>",
    "sequence_id": 3,
    "timestamp": "2026-01-18T12:00:01Z",
    "topics": ["market.kraken.BTC-USD.candles.1h", "signals.paper.BTC-USD.rsi_btc_1h"]
}
```

**Server confirms subscription:**

```json
{
    "type": "subscription_success",
    "public_id": "019e1a2b-0000-7000-8000-000000000908",
    "session_id": "<server-ws-session>",
    "sequence_id": 8,
    "timestamp": "2026-01-18T12:00:01Z",
    "topic": null,
    "action": "subscribe",
    "status": "subscribed",
    "topics": ["market.kraken.BTC-USD.candles.1h", "signals.paper.BTC-USD.rsi_btc_1h"],
    "denied_topics": [],
    "active_subscriptions": ["market.kraken.BTC-USD.candles.1h", "signals.paper.BTC-USD.rsi_btc_1h"],
    "message": null
}
```

**Unsubscribe:**

```json
{
    "type": "unsubscribe",
    "public_id": "019e1a2b-0000-7000-8000-000000000a04",
    "session_id": "<client-ws-session>",
    "sequence_id": 4,
    "timestamp": "2026-01-18T12:00:02Z",
    "topics": ["market.kraken.BTC-USD.candles.1h"]
}
```

**List active subscriptions:**

```json
{
    "type": "get_subscriptions",
    "public_id": "019e1a2b-0000-7000-8000-000000000a05",
    "session_id": "<client-ws-session>",
    "sequence_id": 5,
    "timestamp": "2026-01-18T12:00:02Z"
}
```

**Response:**

```json
{
    "type": "subscriptions_list",
    "public_id": "019e1a2b-0000-7000-8000-000000000909",
    "session_id": "<server-ws-session>",
    "sequence_id": 9,
    "timestamp": "2026-01-18T12:00:02Z",
    "topic": null,
    "subscriptions": ["signals.paper.BTC-USD.rsi_btc_1h"],
    "available_topics": [
        "ai_research.",
        "ai_reviews.",
        "alerts.",
        "backtest.",
        "market.",
        "orders.commands.",
        "orders.events.",
        "plans.decisions.",
        "portfolio.accounts.",
        "processes.events.configured.",
        "processes.events.runs.",
        "processes.events.summary.",
        "signals.",
        "strategies.events.list.",
        "system.egress.",
        "system.heartbeats."
    ],
    "total_available": 15
}
```

### Ping/Pong

**Client sends:**

```json
{
    "type": "ping",
    "public_id": "019e1a2b-0000-7000-8000-000000000a06",
    "session_id": "<client-ws-session>",
    "sequence_id": 6,
    "timestamp": "2026-01-18T12:00:30Z"
}
```

**Server responds:**

```json
{
    "type": "pong",
    "public_id": "019e1a2b-0000-7000-8000-00000000090a",
    "session_id": "<server-ws-session>",
    "sequence_id": 10,
    "timestamp": "2026-01-18T12:00:30Z",
    "topic": null,
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
    "trades": 5678,
    "complete": true
}
```

The `complete` flag marks a provisional intra-minute update of a
still-forming bar when `false`, and the final bar for its window when
`true` (default `true`).

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
    "public_id": "019e1a2b-0000-7000-8000-000000000b01",
    "session_id": "<server-ws-session>",
    "sequence_id": 11,
    "timestamp": "2026-01-18T12:00:00Z",
    "topic": null,
    "message": "Topic 'invalid.topic' does not exist"
}
```

### Available Topic Patterns

Topics use dot-separated hierarchical names. Subscribe using a full topic
string or one of the registry roots returned in `available_topics`.
Intermediate prefixes are rejected; use `market.`, not
`market.kraken.`. Backtest subscriptions are the exception and support
the scoped prefixes documented in the WebSocket auth section above.

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
- `orders.events.{exchange}.{instrument}.unknown` -- Ambiguous venue submit outcome (non-terminal; resolved to accepted or rejected via venue verification)

#### Order Commands

- `orders.commands.{exchange}.{instrument}.submit` -- Submit order
- `orders.commands.{exchange}.{instrument}.cancel` -- Cancel order
- `orders.commands.{exchange}.{instrument}.replace` -- Replace order

#### Portfolio Accounts

- `portfolio.accounts.{wallet_public_id}` -- Thin committed snapshot or reconciliation invalidation; clients refetch `GET /api/portfolio/accounts`

#### System

- `system.heartbeats` -- Global heartbeat
- `system.heartbeats.strategy.{name}` -- Strategy heartbeat
- `system.heartbeats.executor.{exchange}` -- Single-wallet executor heartbeat
- `system.heartbeats.executor.{exchange}.{wallet_short}` -- Per-wallet executor heartbeat; `wallet_short` is exactly 12 lowercase hex characters
- `system.heartbeats.feed.{exchange}` -- Live feed heartbeat
- `system.heartbeats.feed.paper.{source}` -- Paper replay feed heartbeat
- `system.heartbeats.host.disk` -- API-host disk-pressure heartbeat emitted by the system metrics snapshotter
- `system.egress.snapshot` -- Egress pool route snapshots
- `system.egress.transfer` -- WireGuard transfer samples
- `admin.{resource}` -- Administrative events requiring `manage:users`
- `processes.events.summary.{coord_slug}` -- Process summary snapshots
- `processes.events.configured.{coord_slug}` -- Configured process-name snapshots
- `processes.events.runs.{process_name}` -- Per-process lifecycle transitions
- `strategies.events.list.{coord_slug}` -- Strategy class-path snapshots

#### Alerts and Reviews

- `alerts.{user_public_id}.{alert_type}` -- Notification stream
- `ai_research.{round_public_id}.request` -- Committed research-round wake, available through the `ai_research.` subscription root gated by `submit:market_view`
- `plans.decisions.{plan_public_id}` -- Execution-plan decision events
- `ai_reviews.{user_public_id}.{strategy_public_id}.{suffix}` -- AI delegate review frames
- `accruals.{exchange}.{instrument}.{accrual_type}` -- Funding, rollover, and borrow accruals (internal ZMQ bus topic; not currently subscribable over the WebSocket because no permission-derived category includes `accruals`)
- `backtest.{wallet_public_id}.{run_public_id}.{event}` -- Backtest lifecycle and progress frames

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
        # Login: LoginRequest envelope around LoginBody payload
        login_resp = await client.post(
            f"{BASE_URL}/auth/login",
            json={
                "type": "login_request",
                "sequence_id": 1,
                "public_id": "<client-uuid7>",
                "timestamp": "2026-01-18T12:00:00Z",
                "session_id": "<client-session>",
                "topic": None,
                "payload": {
                    "username": "admin",
                    "password": "password123",
                    "remember_me": False,
                },
            },
        )

        ws_token_resp = await client.post(f"{BASE_URL}/auth/ws_token")
        # WsTokenResponse envelope: ws_token lives under .payload
        ws_token = ws_token_resp.json()["payload"]["ws_token"]

        candles_resp = await client.get(
            f"{BASE_URL}/candles",
            params={
                "instrument": "BTC-USD",
                "exchange": "kraken",
                "timeframe": "1h",
            },
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
    async function mintWsToken() {
        const wsTokenResp = await fetch("/api/auth/ws_token", {
            method: "POST",
            credentials: "include",
        });
        // WsTokenResponse envelope wraps the data in `.payload`.
        const { payload } = await wsTokenResp.json();
        return payload.ws_token;
    }

    let wsToken = await mintWsToken();

    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${protocol}//${location.host}/api/ws`);
    const clientSessionId = crypto.randomUUID();
    let sequenceId = 0;

    const controlFrame = (type, body = {}) => ({
        type,
        public_id: crypto.randomUUID(),
        session_id: clientSessionId,
        sequence_id: ++sequenceId,
        timestamp: new Date().toISOString(),
        ...body,
    });

    ws.onopen = () => {
        console.log("Connected, waiting for auth_required...");
    };

    ws.onmessage = async (event) => {
        const msg = JSON.parse(event.data);

        if (msg.type === "auth_required") {
            ws.send(JSON.stringify(controlFrame("authenticate", {
                ws_token: wsToken,
            })));
        }

        if (msg.type === "auth_complete") {
            console.log("Authenticated. Topics:", msg.available_topics);
            ws.send(JSON.stringify(controlFrame("subscribe", {
                topics: ["market.kraken.BTC-USD.candles.1h"],
            })));
        }

        if (msg.type === "candle" || msg.type === "tick") {
            console.log("Data:", msg);
        }

        if (msg.type === "reauth_required") {
            wsToken = await mintWsToken();
            ws.send(JSON.stringify(controlFrame("reauth", {
                ws_token: wsToken,
            })));
        }
    };

    setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify(controlFrame("ping")));
        }
    }, 5000);
}
```

## Backtests

Backtest endpoints manage strategy backtesting runs. Read endpoints require
`read:backtests`; run creation, cancellation, and reruns require
`manage:backtests`. Comparison creation is a deliberately separate capability,
`create:backtest_comparisons`, held by the current `ai_reviewer`,
`ai_delegate`, `operator`, and `admin` sets. The `viewer` and `ai_researcher`
sets cannot create comparisons. Every wallet-bound read and mutation fails with
400 when the caller has no active wallet selected. The global
`GET /api/backtests/strategy-classes` registry read is the exception.

### POST /api/backtests

Create and launch a new backtest run. Requires an active wallet
selection and `manage:backtests`. The route accepts a
`BacktestCreateCommand` envelope wrapping a `BacktestCreateBody` payload,
creates a pending run row, and starts a one-shot `BacktestRunnerProcess`.

**Request body:**

```json
{
    "type": "backtest_create_command",
    "sequence_id": 1,
    "public_id": "<client-uuid7>",
    "timestamp": "2026-01-18T12:00:00Z",
    "session_id": "<client-session>",
    "topic": null,
    "payload": {
        "strategy_class": "RSIReversion",
        "instrument_public_id": "<instrument-public-id>",
        "exchange": "kraken",
        "timeframe": "1h",
        "start_date": "2026-01-01T00:00:00Z",
        "end_date": "2026-06-01T00:00:00Z",
        "initial_cash": 10000.0,
        "strategy_params": {"period": 14, "upper": 70.0, "lower": 30.0},
        "execution_mode": "direct_db",
        "fill_model": "market",
        "slippage_bps": 0.0,
        "commission_bps": 0.0,
        "target_execution_exchange": null
    }
}
```

**Response:** `BacktestRunResponse` with the created run details.

### GET /api/backtests

List backtest runs with optional filters.

**Query parameters:**

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `strategy` | string | Filter by strategy name |
| `status` | string | Filter by status (`pending`, `running`, `completed`, `failed`, `cancel_requested`, `cancelled`) |
| `config_hash` | string | Pairing-stable SHA-256 config hash used by comparison auto-pairing |
| `limit` | int | Page size (1-100, default 20) |
| `offset` | int | Page offset (default 0) |
| `as_of` | datetime | Temporal query timestamp |

### GET /api/backtests/strategy-classes

List registered strategy-class identifiers accepted by
`BacktestCreateBody.strategy_class`. Used by the UI to populate the
create-backtest strategy selector.

### POST /api/backtests/compare

Create or return an idempotent existing comparison row for two terminal
runs. Manual mode supplies `run_a_public_id` and `run_b_public_id`; auto
mode supplies `config_hash` and optionally `anchor_run_public_id`.
The route accepts a `BacktestCompareRequest` envelope wrapping
`BacktestCompareBody`. Requires an active wallet and
`create:backtest_comparisons`. See
[backtesting.md](backtesting.md#comparison) for pairing semantics.

### GET /api/backtests/compare

List recent comparison rows for the caller's active wallet. Requires
`read:backtests`.

**Query parameters:** `limit`, `offset`, and `as_of`.

### GET /api/backtests/compare/{comparison_public_id}

Fetch comparison metadata plus recomputed metrics, equity, trades, and
signals diffs from the current artifact rows. Requires `read:backtests`.

### GET /api/backtests/{run_id}

Get backtest run detail by public ID.

### POST /api/backtests/{run_id}/cancel

Cancel a pending or running backtest. The route accepts a
`BacktestCancelCommand` envelope wrapping `BacktestCancelBody`
(`reason` is optional), sets status to `cancel_requested` via SCD2
close-and-insert, and returns 409 if the run is already in a terminal
state. Requires `manage:backtests`.

### POST /api/backtests/{run_id}/rerun

Create a new backtest run with the same configuration as the original.
Requires `manage:backtests`.

### GET /api/backtests/{run_id}/trades

Get paginated trades for a backtest run.

### GET /api/backtests/{run_id}/signals

Get paginated signals for a backtest run.

### GET /api/backtests/{run_id}/events

Get lifecycle events for a backtest run (started, failed, etc.).

### GET /api/backtests/{run_id}/equity

Get paginated equity curve points for a run.

**Query parameters:** `limit` (1-20000, default 5000), `after`, and
`as_of`.

## AI Reviews

Endpoints for the human-in-the-loop review queue. Strategies emit
`ai_reviews.*` requests via the `create_ai_review_and_await()`
primitive (see [strategies.md](strategies.md)); the selected,
registered AI delegate replies via these routes.

### POST /api/ai-reviews/{review_public_id}/decision

REST counterpart to the `submit_ai_review_decision` MCP tool. Body is
`AiReviewDecisionCommand`, an envelope wrapping an inner
`AiReviewDecisionRequest`:

```json
{
    "type": "ai_review_decision_command",
    "sequence_id": 1,
    "public_id": "<uuid7>",
    "timestamp": "2026-01-18T12:00:00Z",
    "session_id": "<client-session>",
    "topic": null,
    "payload": {
        "decision": "approve",
        "rationale": "Risk profile within bounds."
    }
}
```

`decision` is `"approve"` or `"reject"` (validated against
`AiReviewDecisionEnum`; unknown values yield `422` with
`error_code='invalid_decision'`). `rationale` is optional free
text, ≤ 4096 chars per the `ai_reviews.rationale` column
constraint. Requires `submit:ai_review_decision`, resolved through the same
capability projection the `submit_ai_review_decision` MCP tool uses for both
visibility and execution, so one delegate token behaves identically on either
transport.

A legacy compatibility branch also admits tokens inside the pre-versioning
window. It applies only when every one of these holds:

- the token's permission-scope version is absent (minted before scope
    versioning existed) or is a literal `1`;
- the caller's role ceiling grants `submit:ai_review_decision`
    (`ai_delegate`, `ai_reviewer`);
- that ceiling carries no `manage:users` grant, which excludes `admin`;
- the token's effective grant retained `create:orders`.

Tokens on scope version `2` or later are decided purely by their effective
grant, with no compatibility path. So a create-only `operator` token is
rejected at the route gate regardless of version, and a create-only `admin`
token is rejected even with an absent version, while the pre-versioning
`ai_delegate` production token is admitted. The capability is not sufficient
by itself: the caller must still resolve to a registered `ai_delegates` row and
have a live scope grant for the review wallet and instrument. The dispatch fans out on `bus.ai_review_decision` and emits
`ai_reviews.{user}.{strategy}.decision_ack` on the WS surface.

While a review is pending before its persisted `fanout_after`, only its
`selected_delegate_public_id` may resolve it. A different granted delegate
receives `409` with `error_code="not_selected_before_fanout"` and
`details.fanout_opens_at`; the exact opening instant is accepted. At or after
that instant, any delegate that still passes the same grant check may answer.

Responses use `AiReviewDecisionResponse` — the canonical envelope
shared with the MCP `CallToolResult` payload:

```json
{
    "success": true,
    "error_code": null,
    "message": "Decision recorded.",
    "details": { "...": "..." }
}
```

Error status codes: `404` (review not found), `403` (caller not
authorized for this review), `409` (not selected before fanout or already
resolved by peer), `410` (deadline elapsed), `422` (invalid decision).

### GET /api/ai-reviews/pending

List pending CONSULT reviews where the caller is the selected
AI delegate. Used by the bridge for catch-up after WS reconnect
and by delegate-aware dashboards. Snapshot is bounded by `limit` and
ordered by `fanout_after ASC` (oldest first). The route first
requires `Permission.READ_SIGNALS` (callers without it receive
`403`) and then requires the operational delegate lifecycle identity exposed
as `delegate_public_id`. Callers that pass the permission gate without an
active delegate lifecycle identity receive `422`; this is a state check, not a
role capability check.

**Query parameters:**

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `limit` | int | Page size, 1..500 (default 100) |
| `wallet_public_id` | string | Optional wallet filter |

**Response:** `PendingReviewListResponse` (`items` +
`count`) where each `PendingReviewSummaryItem` carries the
resolved `instrument` ticker plus the raw `signal_envelope`
payload (thesis, side, news anchors) so the AI delegate inbox
can render a meaningful row without a follow-up read.

### GET /api/ai-reviews/{review_public_id}/aftermath

Return a terminal review and the exact wallet+instrument activity that became
visible between the review's `created_at` and one captured `as_of` instant,
inclusive. The route is read-only: it does not transition the review, write an
audit event, or grant authority to place an order.

Requires `Permission.READ_SIGNALS`, a populated operational
`delegate_public_id`, and an active delegate scope grant for the review's wallet
and instrument. Unknown and out-of-scope identifiers both return `404` with
`error_code="review_not_found"`; a non-terminal review returns `409` with
`error_code="review_not_terminal"`; a permission-bearing caller without a
delegate identity returns `422` with `error_code="not_a_delegate"`.

**Response:** `AiReviewAftermathResponse` with this top-level shape:

```json
{
    "review": { "public_id": "<uuid7>", "status": "timeout", "...": "..." },
    "window_started_at": "2026-07-20T10:00:00Z",
    "as_of": "2026-07-20T11:00:00Z",
    "orders": [],
    "executions": [],
    "position_cycle_transitions": [],
    "current_positions": []
}
```

`review` is the complete persisted `ai_reviews` row. Orders and executions are
the versions active at `as_of`, ordered oldest first and bounded by the review
window. Every execution carries `order_public_id`, `instrument_public_id`,
`exchange`, `mode`, and `scope_sequence` so fills whose orders predate the
window remain attributable. Its `client_order_id` and `exchange_order_id` are
nullable when the parent order was not yet visible at `as_of`. Each
position-cycle transition is an explicit `opened`, `closed`, or `liquidated`
event derived from lifecycle timestamps retained by the cycle version active
at `as_of`. `current_positions` is a list because the review row does not carry
a mode and more than one mode-specific position may exist; an instrument with
no position returns an empty list.

### GET /api/ai-reviews

Permission-gated audit list of AI reviews, newest first: the read-only
counterpart to `/pending` that answers "what did the AI decide, and why?".
Unlike `/pending`, it is not keyed by the delegate identity, so it never
returns `422` for a missing delegate identity, and it includes terminal
decided rows rather than only pending ones. Requires `read:ai_reviews`; the
current `viewer`, `operator`, and `admin` sets contain that permission. A
caller with effective `impersonate:operator` sees every operator's reviews;
every other reader is narrowed server-side to its explicit operator
memberships.

**Query parameters:**

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `limit` | int | Page size, 1..500 (default 100) |
| `status` | string | Optional exact-match status filter |
| `wallet_public_id` | string | Optional wallet filter |
| `strategy_public_id` | string | Optional strategy filter |

**Response:** `AdminAiReviewListResponse` (`items` + `count`). Each
`AdminAiReviewItem` carries `review_public_id`, `strategy_public_id`,
`user_public_id`, `operator_public_id`, `wallet_public_id`,
`instrument_public_id`, `selected_delegate_public_id`,
`responding_delegate_public_id`, `status`, `decision`, `rationale`,
`resolution_mode`, `dispatch_version`, `created_at`, `resolved_at`,
`deadline`, and the raw `signal_envelope`.

## Devices

APNs device registration for the iOS app (push notifications for
order / position / system events) plus per-device alert preferences.
All rows are bound to `user_public_id` and follow the SCD2
close-and-insert convention.

### POST /api/devices

Register or refresh the caller's APNs device token. Body is
`RegisterDeviceCommand`. Same-token re-registrations collapse onto
the existing SCD2 row via `upsert_notification_device` and return
the stable `public_id` reused across versions. Returns
`NotificationDeviceResponse`.

### GET /api/devices

List the caller's active devices, newest registered first. Returns
`NotificationDeviceListResponse` (`payload` + `count` — `PayloadListResponse` base); rows whose
active version has `token_status='user_unregistered'` (tombstones)
are excluded.

### DELETE /api/devices/{device_public_id}

Soft-delete a device via SCD2 close + tombstone successor: the
active row is closed and a successor with
`token_status='user_unregistered'` is inserted so an `as_of`
query after the close sees an inactive row rather than a gap.
Returns `MessageResponse`. Marking a nonexistent or not-owned
device inactive returns `404`.

### PATCH /api/devices/{device_public_id}/prefs

Upsert a per-`(device, alert_type, scope)` preference for the
caller. Body is `UpdateDevicePrefCommand`. The composite scope key
is `(device_public_id, alert_type, operator_public_id,
wallet_public_id)` — three null-permutations map to the three
partial unique indexes on `device_alert_prefs`. Returns
`DeviceAlertPrefResponse`.

### POST /api/devices/{device_public_id}/prefs/{pref_public_id}/revoke

Close one device-scoped alert preference (SCD2 in place). Backend
convention is `POST` + envelope (not `DELETE`) so every write
carries client-side provenance for the gap detector — same as
`cancel_order` / `revoke_scope_grant`. The pref row is closed by
stamping `known_to` at the revoke timestamp; no successor is
inserted, which frees the slot in the partial unique index. Body
is `RevokeDevicePrefCommand`; returns `RevokeDevicePrefResponse`.

### GET /api/devices/{device_public_id}/prefs

List active per-`(alert_type, scope)` prefs for a caller-owned
device. Returns `DeviceAlertPrefListResponse`. Tombstoned device
prefs are filtered out server-side via the join to
`notification_devices` on `token_status='active'`.

## Alerts

Real-time alert feed for the dashboard + iOS notification panel.
Alert events are minted by various subsystems and surfaced via WS
on `alerts.*` (see [messaging.md](messaging.md) for the canonical
`AlertType` enumeration) plus this REST catch-up tail.

### GET /api/alerts/history

Keyset-paginated page of the caller's active `alert_events`.
Order: `(timestamp DESC, public_id DESC)`.

**Query parameters:**

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `limit` | int | Page size, 1..200 (default 50) |
| `before` | string | Opaque cursor token from a previous page's `next_cursor`; `None` or malformed values map to page 1 (max 160 chars) |

The `before` cursor is decoded server-side back into the internal
`AlertListCursor` and applied as a keyset predicate against
`Repository.list_recent_alerts_for_user`. Returns
`AlertHistoryResponse`.

### GET /api/alerts/{alert_public_id}

Fetch a single active `alert_events` row if owned by the caller.
Returns `AlertEventResponse`. Ownership is enforced server-side
via `user_public_id` equality; `404` is returned for unknown IDs
**and** for IDs owned by other users, so the endpoint never leaks
the existence of foreign alert events.

## Alert Defaults

Per-user fallback preferences that apply when no per-device
`device_alert_prefs` row matches.

### GET /api/alert_defaults

List the caller's active user-level fallback prefs. Returns
`UserAlertDefaultListResponse` (`payload` + `count` — `PayloadListResponse` base). An empty list
is the legitimate "no overrides" state — clients should fall
through to the in-app default UI; the route does **not**
auto-create rows on first read.

### PATCH /api/alert_defaults

Upsert the caller's user-level fallback pref for a given
`alert_type` via SCD2 close-and-insert. Body is
`UpdateUserAlertDefaultCommand`.

## Metrics

Observability endpoints surfaced for the operations dashboard and
the iOS Home/system-status surface. All routes require
`Permission.READ_SYSTEM_STATUS`. See
[observability.md](observability.md) for the underlying retention /
system-metrics pipeline and snapshot schemas.

### GET /api/metrics/notifications

Current notify-sidecar outbox counters (per-status row counts —
*not* a rolling window). Returns `NotificationMetricsResponse`.

### GET /api/metrics/system

Most recent `SystemMetricsSnapshot` from the in-memory ring
buffer (CPU, memory, GC, file descriptors, plus the process
connection count under `process.num_connections`). Returns
`SystemMetricsResponse`.

### GET /api/metrics/system/history

Bounded slice of the in-memory system-metrics history (no
cursor / page tokens — the caller filters with `since` / `until`
+ `limit`). Returns `SystemMetricsHistoryResponse`.

**Query parameters:**

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `since` | datetime | Lower bound (inclusive) |
| `until` | datetime | Upper bound (inclusive) |
| `limit` | int | Page size, 1..100000 (default 720 — roughly the last hour at the default 5-second cadence) |

Ring-buffer depth is capped by `SYSTEM_METRICS_HISTORY_CAP`
(default 17280, ~24 h at 5 s cadence — see
[configuration.md](configuration.md)).

### POST /api/metrics/system/tracemalloc/start

Arm Python `tracemalloc` with an auto-stop deadline. Body-less;
`duration_s` is supplied as a query parameter. Returns
`TracemallocStateResponse`. Requires `manage:runtime_diagnostics`.

### POST /api/metrics/system/tracemalloc/stop

Disarm `tracemalloc` and cancel any pending auto-stop deadline.
Returns `TracemallocStateResponse`. Requires
`manage:runtime_diagnostics`.

### GET /api/metrics/retention

Most recent retention-scheduler run summary. Returns
`RetentionRunResponse` whose `payload` is a `RetentionRunData`
with top-level fields `run_started_at`, `run_completed_at`,
`dry_run`, and `results: list[RetentionPolicyResult]`. Each
per-policy result inside `results` carries `archived_rows`,
`purged_rows`, `files_written`, and the window boundaries
(`day_start`, `day_end`).

### GET /api/metrics/db/tables

Most recent per-table row-count snapshot. Refreshed on the
cadence configured by `DB_METRICS_INTERVAL_SECONDS`. Returns
`DbStatsResponse`.

### GET /api/metrics/rest-rate

Per-exchange REST call rates + venue rate-limit utilization
(`rps_1s` / `rps_10s` / `rps_60s` rolling windows, plus `limit_rps`
+ `utilization` when the venue's documented cap is known).
Read from the in-process `RestCallTracker` snapshot. Returns
`RestRateResponse`.

## AI Delegates

AI delegate access is feature-gated by `ai_integration_enabled`;
when disabled, these routes return a 503 `feature_disabled` envelope
matching `/api/mcp`.

| Route | Description |
| ----- | ----------- |
| `POST /api/ai-delegates` | Create a delegate and return its 90-day access JWT once |
| `GET /api/ai-delegates` | List active delegates in the caller's creator-owned management scope or read-only operator-membership scope |
| `GET /api/ai-delegates/{delegate_public_id}` | Fetch one delegate in the same permission-appropriate scope; 404 also covers out-of-scope IDs |
| `PATCH /api/ai-delegates/{delegate_public_id}` | SCD2 close+insert new caps for an owned delegate |
| `POST /api/ai-delegates/{delegate_public_id}/deactivate` | Deactivate via the shared kill-switch flow and revoke active tokens |

The routes are documented end-to-end in
[ai-integration.md](ai-integration.md) alongside the Claude Code /
Claude Desktop / Cursor / Windsurf wire-up instructions. They follow
the same `PayloadResponse` envelope convention used by the auth routes
above. List and detail reads require `read:ai_integration`; create, update,
deactivate, and researcher provisioning require `manage:ai_integration`.
Mutating routes also require CSRF for cookie-authenticated requests.
On create, omitted `operator_public_id` binds to the caller's
`primary_operator_public_id` and returns 422 when no primary exists.
Callers lacking effective `impersonate:operator` must choose one of their
authenticated operators when supplying `operator_public_id`; callers with
that effective permission may bind explicitly to any operator.

## AI Researchers

Researcher provisioning shares the `ai_integration_enabled` feature gate but
uses a separate principal named permission set and cap from AI delegates.

| Route | Description |
| ----- | ----------- |
| `POST /api/ai-researchers` | Create a research-only principal and return its 90-day access JWT once |

The route requires `manage:ai_integration`, plus CSRF for cookie
authentication. The request is a `ResearcherCreateRequest` envelope whose
payload contains `label` and an optional narrower `permissions` list. The
`ai_researcher` named-set ceiling is exactly `read:market_data`,
`read:market_views`, and `submit:market_view`; a requested permission outside
that set returns 422.

At most two active researchers may be provisioned per owner. Researcher
creation writes only the user identity and access-token inventory: it creates
no trading caps, operator membership, or `ai_delegates` runtime row, and
therefore cannot consume a delegate slot or enter consult admission accounting.

## Paired Execution (operator surface)

Operator endpoints for the paired-execution guard (dark behind
`PAIRED_EXECUTION_GUARD_ENABLED`; usable regardless for reading state).
Both narrow to the SQL repository (503 otherwise). The operator workflow —
triage, manual venue resolution, attestation — is documented in
[paired-execution.md](paired-execution.md).

| Route | Description |
| ----- | ----------- |
| `GET /api/paired-execution/incidents` | Per-scope incidents: active halts ∪ exposed groups with per-leg signed exposure (`READ_POSITIONS`, wallet-scoped) |
| `POST /api/paired-execution/groups/{group_public_id}/terminalize` | Attest a `manual_intervention` group resolved at the venue (`MANAGE_PAIRED_EXECUTION`, CSRF; 404 also covers out-of-scope wallets, 409 when not attestable) |
