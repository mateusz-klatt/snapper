"""snapper-egress sidecar entry-point package.

This package's ``__main__`` module is the executable launched by the
``snapper-egress`` Docker container (SC.3 of
``proprietary/plans/plan_2026_05_21_snapper_egress_sidecar.md``).
The orchestrator lives in
``snapper.infrastructure.network.egress_sidecar``; this package only
wires the CLI / environment / asyncio scaffolding.
"""
