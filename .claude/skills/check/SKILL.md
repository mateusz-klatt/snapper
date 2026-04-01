---
name: check
description: Run the full quality gate (make check-all)
user-invocable: true
allowed-tools:
  - Bash
---

Run the complete quality gate:

```bash
make check-all
```

This runs: format check → lint → typecheck → docstrings → exclusion scan → frontend checks → tests with 100% coverage.

If it fails, run `make fix-all` first to auto-fix what's possible, then address remaining issues manually.
