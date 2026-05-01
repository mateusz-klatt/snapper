"""Per-table DB-stats sampler (Cluster B observability).

Surfaces ``total / current / closed / archivable`` row counters for
every registered event + state SCD2 table under
``GET /api/metrics/db/tables``. Reads
:data:`snapper.application.retention.policies.RETENTION_POLICIES` for
the ``archivable`` window so Cluster B counts align with Cluster C
retention purges by construction.
"""
