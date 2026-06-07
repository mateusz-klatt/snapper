# snapper-egress sidecar — operator runbook

The `snapper-egress` sidecar runs N WireGuard tunnels plus N matching
SOCKS5 listeners inside one container, so the Kraken Spot publisher
(and any future market-data publisher) can route its WebSocket
traffic out through an alternate egress IP without changing source
code.

This is Phase B' of the Kraken-429 / egress-multiplexer plan. See
`proprietary/plans/plan_2026_05_21_snapper_egress_sidecar.md` (v4
APPROVED) for the full design.

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
- **`MASTER_PASSWORD`** in the same `.env` the snapper-api container
  uses. The sidecar reads encrypted `egress_tunnel_*_private_key`
  settings via the same Fernet path; mismatched master passwords
  silently mean "no tunnels load".

## Architecture (short)

```
┌───────────────┐       ┌─────────────────┐       ┌──────────┐
│ snapper-api   │       │ snapper-egress  │  WG   │  Kraken  │
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

The snapper-api's `EgressPool` (`socks5h://snapper-egress:<port>`)
reserves a route per Kraken WS handshake. On HTTP 429 the active
route is quarantined and the next handshake uses an alternate.

## Adding a tunnel

Three DB settings per tunnel:

| Key | Encrypted? | Type | Example |
|---|---|---|---|
| `egress_tunnel_<id>` | no (JSON descriptor) | text | `{"interface":"wg-uk-1", "address":"10.64.12.34", "prefix_length":32, "peer_pubkey":"...", "peer_endpoint":"vpn.example.com:51820", "socks5_listen_port":1081, "priority":10}` |
| `egress_tunnel_<id>_private_key` | yes (auto-encrypted by `_key` pattern) | text | base64 WG private key |
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

The container's lifespan calls `load_declared_tunnels(service)` →
`wg_control.bring_up(...)` → `Socks5Server.start()` for each tunnel.
Failures are best-effort: one bad tunnel does NOT block the others.

## Wiring the route into the EgressPool

Edit the `egress_pool` setting to add the new route:

```json
{
  "enabled": true,
  "on_all_quarantined": "wait",
  "routes": [
    {"id": "default", "kind": "direct", "priority": 100, "enabled": true},
    {"id": "wg-uk-1", "kind": "socks5",
     "proxy_url": "socks5h://snapper-egress:1081",
     "priority": 10, "enabled": true}
  ]
}
```

**Priority semantics:** `EgressPool.reserve()` sorts by
`(priority, in_use_count)` and picks the **lowest** number first.
For SOCKS5 to actually win selection, its priority MUST be less
than the `default` route's priority. Bootstrap configurations
that ship `default: priority=0` and a new tunnel at
`priority=10` would silently keep using direct egress because
`0 < 10`. Set the direct route to a high number (e.g. `100`)
when you want it to act as fallback only, or set
`"enabled": false` on it to disable direct egress entirely.

### Per-exchange routing — pinning a tunnel to one exchange

To restrict a SOCKS5 route to specific exchanges, add an
`allowed_exchanges` field listing the exchange names (matching
`ExchangeEnum` *values* — `"walutomat"`, `"kraken"`,
`"kraken_futures"`, `"kraken_equities"`, `"polygon"`):

```json
{
  "id": "eset-pl1", "kind": "socks5",
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
want to deny direct egress for a specific exchange, set
`"enabled": false` on the direct route instead. Unknown exchange
names (typos) are rejected by `EgressPoolConfig` at config-load
time, so a `"krakeen"` typo never silently makes a route
unreachable.

Then restart snapper-api so the lifespan re-runs the egress-pool
preflight:

```
docker compose restart snapper
```

The lifespan logs:

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

4. **WireGuard handshake established?** (operator debugging — uses
   `iproute2` shipped in the image)
   ```
   docker compose exec snapper-egress ip -d link show wg-uk-1
   docker compose exec snapper-egress ip rule show table 5000
   ```
   The interface should be `UP` and the rule should pin `from
   <tunnel_addr>` → table N.

4. **Kraken WS uses the tunnel?** Quarantine the direct route from
   the snapper-api Python REPL:
   ```python
   from datetime import UTC, datetime, timedelta
   from snapper.infrastructure.network.egress_pool import get_egress_pool
   pool = get_egress_pool()
   pool._quarantine_route(
       "default",
       datetime.now(UTC) + timedelta(seconds=600),
       "http-429",
   )
   ```
   Then force a Kraken Spot reconnect (restart the publisher process)
   and observe the next handshake. The log line should show the
   tunnel route id; Cloudflare's view of the source IP should match
   the VPN exit.

5. **Stable tick flow?**
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
  hook `scripts/check_egress_compose.py` rejects any `ports:` entry
  or `network_mode: host` on the snapper-egress service so an
  operator cannot accidentally publish the listener to the host.
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
   route OR delete the entry); restart snapper-api.
2. Delete the three settings (`egress_tunnel_<id>`,
   `egress_tunnel_<id>_private_key`,
   `egress_tunnel_<id>_preshared_key`).
3. Restart snapper-egress. The orchestrator's startup `bring_down`
   pass (idempotent cleanup before bring_up) removes any stale
   interface / ip-rule / routing-table entry left over.

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
- Tunnel handshake age: WireGuard re-keys every 2 minutes; if `wg
  show <iface> latest-handshakes` returns > 180 s ago, the peer is
  unreachable.
- SOCKS5 listener latency: ≤ 1 ms additional vs direct route (single
  process, single asyncio loop, no auth handshake).
