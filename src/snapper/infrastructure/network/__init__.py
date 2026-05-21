"""Network infrastructure: egress routing, proxy multiplexer.

This package owns the egress-route abstraction used by exchange
publishers to choose an outbound source per WebSocket / HTTP
connection. See ``egress_pool`` for the route registry and
``egress_models`` for the route + reservation types.

Phase B' of plan_2026_05_21_kraken_429_retry_after_egress_pool. Direct
route is the default; SOCKS5 routes are provided by the future
snapper-egress sidecar.
"""
