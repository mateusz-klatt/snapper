"""Per-table DB-stats sampler.

Surfaces ``total / current / closed / archivable`` row counters for
every registered event + state SCD2 table under
``GET /api/metrics/db/tables``. Reads
:data:`snapper.application.retention.policies.RETENTION_POLICIES` for
the ``archivable`` window so the counts align with retention purges
by construction.
"""
