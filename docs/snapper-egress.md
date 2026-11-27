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
  reports healthy, and blocks `snapper`, `snapper-feed`, and
  `snapper-strategies` via `depends_on: service_healthy`.
- **`DB_URL` and `ZMQ_BROKER_XSUB`** are both hard-required by the
  sidecar entrypoint (`snapper.egress.__main__`). The process exits with
  **code 2** at startup if either is missing: `DB_URL` lets the
  SettingsService read tunnel descriptors + encrypted keys, and
  `ZMQ_BROKER_XSUB` wires SettingsService change-broadcast support.
  `DB_URL` is supplied via `env_file: .env`. In the Compose split,
  `ZMQ_BROKER_XSUB` MUST be the routable broker host
  `tcp://snapper-broker:7500`, NOT the `.env` default
  `tcp://127.0.0.1:7500` — the `docker-compose.yml` `snapper-egress`
  service overrides it for exactly this reason (and depends on the
  `snapper-broker` service being healthy). `snapper-egress` runs in a
  separate container, so `localhost` would not reach the broker living
  in the `snapper-broker` container.

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
Set `feed_egress_enabled=true` so each publisher loads its own pool at
startup. The API also loads a process-local pool for routed Kraken REST.
A missing or disabled pool leaves the public Kraken and Walutomat routes
unavailable by default. Kraken WebSocket reconnects and publisher startup
wait for a VPN. One-shot and other public REST callers receive a typed
refusal. An explicit dev-only opt-in is available through the standalone
DB setting `public_direct_allowed`. Production never sets this.
The pool reserves a route per Kraken WS handshake. After a route is
quarantined, the next handshake uses another VPN or waits. Quarantine
reasons include: `http-429` (a handshake 429
carrying a positive numeric `Retry-After` header; a 429 without one
does not quarantine the route),
`close-1015` (Cloudflare close frame), `http-connect-error` (REST
connect failure via the pooled transport), `http-proxy-error` (a REST
request through the pooled transport whose SOCKS or proxy hop failed
(`httpx.ProxyError`), quarantined for the same
`_HTTP_CONNECT_ERROR_QUARANTINE_S`, or a Kraken public REST read routed
by `kraken_rest_egress` with `requests.exceptions.ProxyError` or
`urllib3.exceptions.ProxyError` in the cause chain, quarantined for
`_KRAKEN_REST_CONNECT_ERROR_QUARANTINE_S`; proxy errors never make a
private read fallback-eligible), and `ws-connect-error` (a WS
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
route id, otherwise `EgressPoolConfig` rejects the pool. The route
serves private traffic only, and private selection ignores its
`allowed_exchanges`. Kraken authenticated REST uses this route for
private executor REST reads.

**Priority semantics.** Public selection ranks eligible healthy SOCKS5 routes by
`(priority, in_use_count)` within the preferred exchange scope, with the
lowest values first. Keep an enabled direct route for private traffic
whenever `egress_pool.enabled=true`.

### Per-exchange routing

To prefer a SOCKS5 route for specific exchanges, add an
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

An empty `allowed_exchanges` marks the route as preferred for all exchanges.
Public traffic ranks those routes together with routes pinned to that exchange.
If they are all unavailable, the pool ranks the remaining healthy public
VPNs and logs a warning with the requesting exchange and selected route.
The configured private fallback stays outside public selection and release
timing, even when its `allowed_exchanges` matches. Config validation
rejects unknown exchange names.

### Public vs private traffic

Routing depends on the reservation's **traffic class**
(`TrafficClass = Literal["public", "private"]`, defaulting to
`"public"`):

- **Public** market data uses the public VPN/SOCKS5 selector. When every
  eligible VPN is unavailable, both values of `on_all_quarantined` produce
  a typed refusal by default. With the dev-only opt-in, a new reservation
  can use the host's direct route and the pool warns on its first use per
  exchange during the episode.
  Kraken WS sleeps until the next public quarantine release with a one-second
  minimum. The pool logs each exchange's wait start and end at WARNING and
  includes the elapsed and cumulative wait seconds at the end.
- **Private** executor traffic is direct-first. Executors set
  `egress_identity(traffic_class="private", owner="executor")`. A
  healthy direct route wins, followed by the configured private fallback.
  Kraken private REST mutations stay on the direct-only path. A private
  WS connect error briefly quarantines direct so a subsequent reconnect
  can use a configured private fallback.

Then enable the feed gate (DB setting `feed_egress_enabled=true`) if
not already on, and restart both the API and the feed tier — the feed
publishers hold their own process-local pools:

```
docker compose restart snapper snapper-feed
```

The process-local egress preflight validates each SOCKS5 route before
installing the pool: it checks that `python-socks` is importable,
resolves the proxy host, opens the proxy port, and sends a SOCKS5
NO_AUTH greeting. Missing `python-socks` or an unusable proxy host or port
disables the affected route for that process. DNS, TCP, and greeting
failures leave the route enabled and quarantine it for 30 seconds from pool
installation. Feed callers can try the route again when that deadline expires.

The orchestrator logs:

```
egress_pool: configured with 2 route(s), on_all_quarantined=wait
```

## Deploy ordering and one-shot snapshots

Recreate `snapper-egress` first. Probe `/ready-public` from the sidecar
before recreating `snapper-feed` and `snapper-strategies`.

```sh
docker compose exec snapper-egress \
  python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8081/ready-public', timeout=2).read().decode())"
```

Expect HTTP 200 with the ID of a running SOCKS5 tunnel whose WireGuard
peer handshook within the last 180 seconds. HTTP 503 means no running
tunnel meets that condition. Compose still uses `/ready` as the
sidecar's bootstrap check. Waiting for public readiness reduces the feed
gap. If a feed starts before the listener is ready, its initially
quarantined routes become eligible for retry without a feed restart.

The `update-kraken-market-snapshot`, `update-kraken-futures-market-snapshot`,
`update-kraken-equities-market-snapshot` and
`update-walutomat-market-snapshot` commands load `egress_pool` from the
settings database in their own process, including when invoked with
`docker compose run snapper <command>`. Each selects its own venue's
public scope independently of `feed_egress_enabled`. A missing or exhausted
pool at startup gives a clear error and a nonzero exit by default. Dev
snapshot commands honor the same explicit direct opt-in as publishers.

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

4. **Backend egress snapshot visible?** From an authenticated session whose
    effective grant contains `read:system_status` (including a read-only
    `viewer` session), call `GET /api/health/egress`. Expect
    `payload.enabled=true`, one row per configured route, route metadata
    (`region`, `exit_ip`, `provider`) when configured, quarantine state,
    merged `in_use_count`, per-reservation `container`, `connections` rows for
    open WebSocket host counts and capped REST last-seen hostnames, and a
    `transfer` object on SOCKS5 routes when `snapper-egress` has reported
    matching WireGuard counters for that listener port. Transfer rows include
    cumulative rx/tx bytes, current byte rates when a reliable delta exists,
    the latest WireGuard handshake timestamp, sample age, and stale/reset
    flags. Direct routes, missing samples, and ambiguous shared ports show
    `transfer=null`. Connection rows expose hostnames only, never URL paths,
    query strings, headers, request bodies, or credentials. A missing
    `snapper-feed` row means the API process has not received a
    `system.egress.snapshot` frame from that container yet; a stale row remains
    visible with `stale=true`.

5. **WireGuard handshake established?** (operator debugging — uses
   `wireguard-tools` and `iproute2` shipped in the image)

    ```
    docker compose exec snapper-egress wg show wg-uk-1 latest-handshakes
    docker compose exec snapper-egress wg show wg-uk-1 transfer
    docker compose exec snapper-egress ip -d link show wg-uk-1
    docker compose exec snapper-egress ip rule show table 5000
    ```

    A nonzero recent handshake and increasing transfer counters after a test
    request establish peer connectivity. An `UP` interface, `/readyz` success,
    or a tunnel `status: up` only proves local configuration succeeded. The
    routing rule should pin `from <tunnel_addr>` → table N.

6. **Kraken WS uses the tunnel?** The pool is a process-local
   singleton inside each feed-publisher process — `get_egress_pool()`
   in a fresh `docker compose exec` interpreter returns `None`, and
   quarantining inside the API process would not touch the publisher
   pools, so a REPL-based quarantine cannot exercise the failover.
   Enable `feed_egress_enabled`, recreate the feed after public readiness,
   and check for `traffic_class=public` and `route_kind=socks5` in the
   publisher's route-selection log. The source IP should match the VPN exit.

7. **Stable tick flow?**

    ```sql
    SELECT i.exchange, COUNT(*) AS n
    FROM ticks t JOIN instruments i ON i.public_id = t.instrument_public_id
    WHERE t.timestamp > NOW() - INTERVAL '60 seconds'
    GROUP BY i.exchange ORDER BY n DESC;
    ```

    Compare counts and freshness with this deployment's baseline for the same
    subscriptions and market activity. Tick persistence policy also affects
    these counts; a drop alone does not identify an egress failure. Correlate
    it with publisher logs, handshake age, and transfer counters.

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
  reconnect tries an alternate VPN or waits until a route can be tried again.
- Tunnel handshake age: WireGuard re-keys every 2 minutes; if `wg
  show <iface> latest-handshakes` returns > 180 s ago, the peer is
  unreachable. The backend also surfaces the latest handshake from
  sidecar `system.egress.transfer` samples on the matching SOCKS5 route.
- SOCKS5 listener latency: ≤ 1 ms additional vs direct route (single
  process, single asyncio loop, no auth handshake).
