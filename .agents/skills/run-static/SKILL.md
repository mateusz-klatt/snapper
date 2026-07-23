---
name: run-static
description: Refresh Snapper symbols, underlying mappings, and market snapshots with make run-static. Use when local static reference data needs updating.
---

Before running the pipeline:

1. Check the hostname.
2. Resolve the database target used by `.env` without printing credentials.
3. Proceed only when `home` targets the intended local-development database.
4. Stop for explicit confirmation when the host is `holzera`, or when the target is remote, shared, staging, production, or uncertain.

Then run the full static data refresh pipeline:

```bash
make run-static
```

This executes in order:
1. Symbol updaters (Kraken spot, Kraken Futures, Kraken Equities, Walutomat, Polygon)
2. Underlying asset mapping update
3. Market snapshots (Kraken spot, Kraken Futures, Kraken Equities, Walutomat)

The Polygon symbol updater is best-effort and may fail without stopping the pipeline. Report that failure separately if it appears in the output.
If another step fails, check which step failed and report the error.
If the database doesn't exist, run `make migrate-dev` first.
