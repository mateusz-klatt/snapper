---
name: run-static
description: Run all symbol updaters and market snapshots (make run-static)
user-invocable: true
allowed-tools:
  - Bash
---

Run the full static data refresh pipeline:

```bash
make run-static
```

This executes in order:
1. Symbol updaters (Kraken spot, Kraken Futures, Kraken Equities, Zonda, Walutomat, Polygon)
2. Market snapshots (Kraken spot, Kraken Futures, Kraken Equities, Zonda, Walutomat)

If it fails, check which step failed and report the error.
If the database doesn't exist, run `make migrate-dev` first.
