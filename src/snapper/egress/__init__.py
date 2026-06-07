"""snapper-egress sidecar entry-point package.

This package's ``__main__`` module is the executable launched by the
``snapper-egress`` Docker container. The orchestrator lives in
``snapper.infrastructure.network.egress_sidecar``; this package only
wires the CLI / environment / asyncio scaffolding.
"""
