# snapper-egress sidecar — operator runbook

The `snapper-egress` sidecar runs N WireGuard tunnels plus N matching
SOCKS5 listeners inside one container, so Kraken Spot/Futures/Equities
WebSocket publishers and Walutomat HTTP polling can route public
market-data traffic out through alternate egress IPs without changing
source code.

This sidecar is part of the egress-multiplexer subsystem.

## Prerequisites

- **Docker Compose v2.** `depends_on.condition: service_healthy` is
  Compose v2 syntax. Compose v1 silently ignores it.
- **Kernel WireGuard on the host when tunnels are configured.**
  Verify with `lsmod | grep wireguard` or `modprobe wireguard`. The
  sidecar bind-mounts `/lib/modules` and uses the host kernel module
  via netlink (no userspace WireGuard). Empty/default deployments
  with no declared tunnels still expose `/ready` without probing
  WireGuard. Use `/readyz` when you need strict "every declared tunnel
  is up" semantics.
- **`MASTER_PASSWORD`** in the same `.env` the `snapper` (API) container
  uses. The sidecar reads encrypted `egress_tunnel_*_private_key`
  settings via the same Fernet path; a mismatched master password makes
  the settings load raise (`Failed to decrypt encrypted setting`) and
  the sidecar exits at startup, so the container crash-loops, never
  reports healthy, and blocks `snapper` / `snapper-feed` via
  `depends_on: service_healthy`.
- **`DB_URL` and `ZMQ_BROKER_XSUB`** are both hard-required by the
  sidecar entrypoint (`snapper.egress.__main__`). The process exits with
  **code 2** at startup if either is missing: `DB_URL` lets the
  SettingsService read tunnel descriptors + encrypted keys, and
  `ZMQ_BROKER_XSUB` wires SettingsService change-broadcast support.
  `DB_URL` is supplied via `env_file: .env`. In the Compose split,
  `ZMQ_BROKER_XSUB` MUST be the routable broker host
  `tcp://snapper:7500`, NOT the `.env` default `tcp://127.0.0.1:7500` —
  the `docker-compose.yml` `snapper-egress` service overrides it for
  exactly this reason. `snapper-egress` runs in a separate container, so
  `localhost` would not reach the backend broker living in the `snapper`
  container.

## Architecture (short)

```
┌───────────────┐       ┌─────────────────┐       ┌──────────┐
│ snapper-feed  │       │ snapper-egress  │  WG   │  Kraken  │
│ (publishers)  │ SOCKS │ (this sidecar)  │ ────▶ │  WS      │
│               │ ────▶ │  wg-uk-1 :1081  │       │          │
└───────────────┘       │  wg-us-1 :1082  │       └──────────┘
                        │  wg-de-1 :1083  │
                        └─────────────────┘
```

Per tunnel:
1. WireGuard interface `wg-<id>` lives in the sidecar container.
2. Source-based routing rule sends packets from the tunnel's IP
   through the matching WG interface.
3. SOCKS5 listener binds outbound sockets to the tunnel's IP →
   kernel routes them via the tunnel.

Kraken WS handshakes go through the `EgressPool`
(`socks5h://snapper-egress:<port>`) inside the feed-publisher
subprocesses of the **snapper-feed** container (the API profile
excludes market-data publishers; the FEED profile runs only them).
Each publisher process builds its own process-local pool at start,
gated on the `feed_egress_enabled` DB setting (default **off** —
publishers dial direct until the gate is flipped). The API process also
initializes its own process-local pool from `egress_pool`; API-side Kraken
public REST reads may route through that pool even when `feed_egress_enabled`
is false, because the gate applies only to feed-publisher subprocess pools.
The pool reserves a route per Kraken WS handshake. A route is quarantined — and
the next handshake fails over to an alternate (or the direct fallback) —
on any of the `QuarantineReason` values: `http-429` (a handshake 429
carrying a positive numeric `Retry-After` header; a 429 without one
does not quarantine the route),
`close-1015` (Cloudflare close frame), `http-connect-error` (REST
connect failure via the pooled transport), and `ws-connect-error` (a WS
connect-level failure — TCP/SOCKS timeout, connection refused, or a
silent blackhole — through a proxy route, quarantined ~30 s via
`_WS_CONNECT_ERROR_QUARANTINE_S`). Connect-level failover matters
because a dead tunnel often blackholes rather than returning 429: without
it the reconnect loop would re-dial the same dead route indefinitely.

## Adding a tunnel

Preferred path: run the provisioning helper from a checkout that has
`scripts/` available. The production Docker image does not copy
`scripts/` into `/app`, so run it on the host or bind-mount the
checkout into a maintenance container with the same `DB_URL`.

```bash
poetry run python scripts/provision_egress_tunnel.py \
  --tunnel-id wg-uk-1 \
  --interface wg-uk-1 \
  --address 10.64.12.34 \
  --prefix-length 32 \
  --private-key-file ./secrets/wg-uk-1.key \
  --peer-pubkey "<peer-public-key>" \
  --peer-endpoint vpn.example.com:51820 \
  --socks5-listen-port 1081 \
  --priority 10 \
  --dry-run
```

Remove `--dry-run` after validation. The helper writes the descriptor,
encrypted private/optional preshared key settings, and merges the SOCKS5
route into `egress_pool`. It can also restart the sidecar with
`--restart-sidecar`. After `egress_pool` changes, restart
**snapper-feed** (each feed-publisher process builds its own
process-local pool at start) in addition to `snapper` (the API tier) —
restarting only the API rebuilds the API process's pool but not the pools the
Kraken/Walutomat feeds actually dial through.

Manual path:

Three DB settings per tunnel:

| Key | Encrypted? | Type | Example |
|---|---|---|---|
| `egress_tunnel_<id>` | no (JSON descriptor) | text | `{"interface":"wg-uk-1", "address":"10.64.12.34", "prefix_length":32, "peer_pubkey":"...", "peer_endpoint":"vpn.example.com:51820", "socks5_listen_port":1081, "priority":10}` |
| `egress_tunnel_<id>_private_key` | yes (auto-encrypted because the key contains the `private_key` substring) | text | base64 WG private key |
| `egress_tunnel_<id>_preshared_key` | yes, optional | text | base64 WG PSK (or absent / empty) |

Constraints:

- `id` MUST NOT contain underscores (it would collide with the
  `_private_key` / `_preshared_key` naming convention). Use hyphens.
- `interface` MUST start with `wg-` and be ≤ 15 chars total (Linux
  IFNAMSIZ).
- `socks5_listen_port` MUST be 1024-65535, unique across tunnels.
  Convention: 1081 for the first tunnel, then 1082, 1083, ….
- `address` must parse as IPv4 or IPv6; IPv4 prefix ≤ 32, IPv6 ≤ 128.

Once the three keys exist in the DB, restart the sidecar:

```
docker compose restart snapper-egress
```

The sidecar orchestrator (`run_sidecar`) calls `load_declared_tunnels(service)` →
`wg_control.bring_up(...)` → `Socks5Server.start()` for each tunnel.
Failures are best-effort: one bad tunnel does NOT block the others.

## Wiring the route into the EgressPool

Edit the `egress_pool` setting to add the new route:

```json
{
  "enabled": true,
  "on_all_quarantined": "wait",
  "private_fallback_route_id": "wg-uk-1",
  "routes": [
    {"id": "default", "kind": "direct", "priority": 100, "enabled": true},
    {"id": "wg-uk-1", "kind": "socks5",
     "proxy_url": "socks5h://snapper-egress:1081",
     "region": "uk-lon",
     "exit_ip": "203.0.113.20",
     "provider": "wireguard-uk",
     "priority": 10, "enabled": true}
  ]
}
```

`region`, `exit_ip`, and `provider` are optional operator metadata
fields. They do not affect routing; they are surfaced through
`GET /api/health/egress` so an operator can identify where a route exits.

`private_fallback_route_id` is an optional top-level key (default
`null`). It names a declared route used as the **private-traffic
fallback only when no healthy direct route exists** — so executor (order)
traffic CAN ride a SOCKS5 tunnel when configured, even though private
traffic normally goes direct (see *Public vs private traffic* below).
It is validated at config-load: when set, it MUST reference an existing
route id, otherwise `EgressPoolConfig` rejects the pool. Its
`allowed_exchanges` is **ignored** on the private fallback path (the
route may be publicly pinned to one exchange while still serving as the
private fallback for another). Kraken authenticated REST wires this
route for private executor REST reads.

**Priority semantics:** `EgressPool.reserve()` sorts by
`(priority, in_use_count)` and picks the **lowest** number first.
For SOCKS5 to actually win selection, its priority MUST be less
than the `default` route's priority. Bootstrap configurations
that ship `default: priority=0` and a new tunnel at
`priority=10` would silently keep using direct egress because
`0 < 10`. Set the direct route to a high number (e.g. `100`)
when you want it to act as fallback only. Keep at least one enabled
direct route whenever `egress_pool.enabled=true`; current schema does
not support disabling direct fallback for an enabled pool.

### Per-exchange routing — pinning a tunnel to one exchange

To restrict a SOCKS5 route to specific exchanges, add an
`allowed_exchanges` field listing the exchange names (matching
`ExchangeEnum` *values* — `"walutomat"`, `"kraken"`,
`"kraken_futures"`, `"kraken_equities"`, `"polygon"`, `"paper"`):

```json
{
  "id": "wg-eu-1", "kind": "socks5",
  "proxy_url": "socks5h://snapper-egress:1084",
  "priority": 10,
  "allowed_exchanges": ["walutomat"],
  "enabled": true
}
```

A non-empty `allowed_exchanges` restricts the pool: when
`pool.reserve(exchange=...)` is called with an exchange name not in
the list, this route is filtered out before the
`(priority, in_use_count)` sort. An empty `allowed_exchanges`
(the default) means the route serves any exchange — the
back-compatible path for existing pool entries.

**The direct fallback path IGNORES `allowed_exchanges` by design.**
When every preferred route is quarantined, `EgressPool` returns the
direct route as a last resort regardless of any exchange pin. If you
want SOCKS routes preferred for a specific exchange, pin those routes
with `allowed_exchanges` and give the direct route a worse priority;
current schema does not support per-exchange denial of direct fallback.
Unknown exchange names (typos) are rejected by `EgressPoolConfig` at
config-load time, so a `"krakeen"` typo never silently makes a route
unreachable.

### Public vs private traffic

Routing depends on the reservation's **traffic class**
(`TrafficClass = Literal["public", "private"]`, defaulting to
`"public"`):

- **Public** market-data traffic (Kraken WS publishers and other
  feeds) routes over the VPN/SOCKS5 tunnels by the public selection
  path — the `(priority, in_use_count)` sort honouring each route's
  `allowed_exchanges` allow-list, with the direct route as fallback.
- **Private** executor (order) traffic is routed **direct-first**.
  Executors wrap their work in
  `egress_identity(traffic_class="private", owner="executor")`, and the
  pool routes `traffic_class="private"` through a separate selection
  path (`_pick_private_locked`) that prefers a healthy `direct` route
  regardless of route priority and ignores `allowed_exchanges`,
  bypassing the public sort entirely. When direct is unavailable and
  `private_fallback_route_id` is configured, private idempotent REST reads
  and the next private WebSocket reconnect can ride that fallback route.
  Private mutations remain direct-only. Kraken authenticated REST ops are
  tagged the same way.

In short: **public = VPN/SOCKS5 preferred** (priority + allow-list, with
direct fallback), **private = direct-first**. A healthy direct route always
wins for private traffic; `private_fallback_route_id` is used only when
direct is unavailable, and private mutations remain direct-only.
Private executor WebSocket direct connect errors briefly quarantine the direct
route for about 15 seconds so the next reconnect can use the configured private
fallback. Public direct routes are not quarantined on connect errors; proxy
routes quarantine for about 30 seconds.

Then enable the feed gate (DB setting `feed_egress_enabled=true`) if
not already on, and restart both the API and the feed tier — the feed
publishers hold their own process-local pools:

```
docker compose restart snapper snapper-feed
```

The process-local egress preflight validates each SOCKS5 route before
installing the pool: it checks that `python-socks` is importable,
resolves the proxy host, opens the proxy port, and sends a SOCKS5
NO_AUTH greeting. A route that fails preflight is auto-disabled in that
process's effective config, so restart every process that should use
the updated route set.

The orchestrator logs:

```
egress_pool: configured with 2 route(s), on_all_quarantined=wait
```

## Verification

1. **Sidecar healthy?**
   ```
   docker compose exec snapper-egress \
     python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8081/ready', timeout=2).read())"
   ```
   Expect a 200 with the running + failed tunnel id arrays. This is the
   Compose health endpoint and intentionally stays 200 after bootstrap
   even if an individual tunnel failed.

2. **All tunnels up?**
   ```
   docker compose exec snapper-egress \
     python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8081/readyz', timeout=2).read())"
   ```
   Expect 200 only when every declared tunnel is up; otherwise it
   returns 503 with the failed tunnel ids.

3. **Tunnel status detail?**
   ```
   docker compose exec snapper-egress \
     python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8081/tunnels', timeout=2).read())"
   ```
   Each tunnel id maps to `{"status": "up", "reason": null}`.

4. **Backend egress snapshot visible?** From an authenticated operator
   session with `read:system_status`, call `GET /api/health/egress`.
   Expect `payload.enabled=true`, one row per configured route, route
   metadata (`region`, `exit_ip`, `provider`) when configured,
   quarantine state, merged `in_use_count`, per-reservation `container`,
   `connections` rows for open WebSocket host counts and capped REST
   last-seen hostnames, and a `transfer` object on SOCKS5 routes when
   `snapper-egress` has reported matching WireGuard counters for that
   listener port. Transfer rows include cumulative rx/tx bytes, current
   byte rates when a reliable delta exists, the latest WireGuard
   handshake timestamp, sample age, and stale/reset flags. Direct routes,
   missing samples, and ambiguous shared ports show `transfer=null`.
   Connection rows expose hostnames only, never URL paths, query strings,
   headers, request bodies, or credentials. A missing `snapper-feed` row
   means the API process has not received a `system.egress.snapshot`
   frame from that container yet; a stale row remains visible with
   `stale=true`.

5. **WireGuard handshake established?** (operator debugging — uses
   `iproute2` shipped in the image)
   ```
   docker compose exec snapper-egress ip -d link show wg-uk-1
   docker compose exec snapper-egress ip rule show table 5000
   ```
   The interface should be `UP` and the rule should pin `from
   <tunnel_addr>` → table N.

6. **Kraken WS uses the tunnel?** The pool is a process-local
   singleton inside each feed-publisher process — `get_egress_pool()`
   in a fresh `docker compose exec` interpreter returns `None`, and
   quarantining inside the API process would not touch the publisher
   pools, so a REPL-based quarantine cannot exercise the failover.
   Instead, steer by configuration: give the direct route the worst
   priority in the `egress_pool` setting, make sure
   `feed_egress_enabled=true`, then
   restart snapper-feed and observe the next handshake. The publisher
   log line should show the tunnel route id; Cloudflare's view of the
   source IP should match the VPN exit. Restore the normal direct-route
   priority when done.

7. **Stable tick flow?**
   ```sql
   SELECT i.exchange, COUNT(*) AS n
   FROM ticks t JOIN instruments i ON i.public_id = t.instrument_public_id
   WHERE t.timestamp > NOW() - INTERVAL '60 seconds'
   GROUP BY i.exchange ORDER BY n DESC;
   ```
   Kraken should show consistent counts (~1500-2000 ticks/s in steady
   state). Drops indicate either the tunnel is flapping or the SOCKS5
   listener is mis-bound.

## Threat-model + security notes

- The SOCKS5 listener runs WITH NO AUTHENTICATION. Isolation comes
  entirely from the Docker `snapper-internal` network. The CI lint
  hook `scripts/check_egress_compose.py` rejects any `ports:` entry,
  `network_mode: host`, or env-interpolated bypass on the
  snapper-egress service so an operator cannot accidentally publish
  the listener to the host.
- The same lint hook also enforces the unified-image runtime contract:
  `snapper-egress` must use the same image as `snapper`, run
  `command: ["egress"]` as `user: "0:0"`, include `NET_ADMIN`, and
  mount `/app/data` when the monolith does. The `snapper` service must
  remain unprivileged, with no `cap_add` and no `user` override.
- All traffic on `snapper-internal` is trusted equally. Do NOT
  attach untrusted containers (other tenants, debugging shells from
  unknown sources) to that network. If you must, add SOCKS5
  username/password auth in `socks5_server.py` (≈10-line change).
- For extra defense-in-depth, run `docker compose config | grep -A5
  snapper-egress` after any compose edit to verify the effective
  config still has no host port mapping. The lint hook catches the
  static cases but not every override pattern.
- WG private keys are stored Fernet-encrypted in the Postgres
  `settings` table. The encryption key is derived from
  `MASTER_PASSWORD` (env var). Loss of master password = loss of
  every encrypted setting (not just WG keys).

## Decommissioning a tunnel

1. Remove the route from `egress_pool` (set `enabled: false` on that
   route OR delete the entry); restart the API and feed tiers
   (`docker compose restart snapper snapper-feed`).
2. Delete the three settings (`egress_tunnel_<id>`,
   `egress_tunnel_<id>_private_key`,
   `egress_tunnel_<id>_preshared_key`).
3. Restart snapper-egress. The stopping sidecar's shutdown pass brings
   the removed tunnel's interface / ip-rule / routing-table entries
   down, and the container restart recreates the network namespace; the
   idempotent cleanup-before-bring_up in `wg_control.bring_up` only
   re-cleans interfaces and routing tables of tunnels that are still
   declared.

## Rotating private keys

1. Generate a new private key + matching peer config on the VPN
   side.
2. Update the `egress_tunnel_<id>_private_key` setting (SettingsService
   re-encrypts on write).
3. Restart snapper-egress to pick up the new key. The orchestrator's
   bring_up is idempotent and recreates the WG interface from scratch.

## Operational expectations

- Sidecar startup: ~5-15 s (DNS resolution of peer endpoints is the
  dominant cost).
- HTTP 429 failover: ~1-2 s (route quarantined → next handshake picks
  alternate).
- WS connect-error failover (`ws-connect-error`): the dead proxy route is
  quarantined ~30 s on a connect timeout/refused/blackhole, so the next
  reconnect rides an alternate route (or the direct fallback) instead of
  re-dialing the dead tunnel.
- Tunnel handshake age: WireGuard re-keys every 2 minutes; if `wg
  show <iface> latest-handshakes` returns > 180 s ago, the peer is
  unreachable. The backend also surfaces the latest handshake from
  sidecar `system.egress.transfer` samples on the matching SOCKS5 route.
- SOCKS5 listener latency: ≤ 1 ms additional vs direct route (single
  process, single asyncio loop, no auth handshake).
