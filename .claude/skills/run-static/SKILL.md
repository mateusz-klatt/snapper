---
name: run-static
description: Run symbol updaters, underlying mappings, and market snapshots (make run-static)
user-invocable: true
allowed-tools:
  - Bash
---

Run the full static data refresh pipeline:

```bash
make run-static
```

This executes in order:

1. Symbol updaters (Kraken spot, Kraken Futures, Kraken Equities, Walutomat, Polygon)
2. Underlying asset mapping update
3. Market snapshots (Kraken spot, Kraken Futures, Kraken Equities, Walutomat)

The Polygon symbol updater is best-effort: its failure is tolerated by the Makefile and must be reported separately. If another step fails, check which step failed and report the error.
If the database doesn't exist, run `make migrate-dev` first.
