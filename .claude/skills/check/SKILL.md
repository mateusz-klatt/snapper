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

This runs: format check → lint → typecheck → backend policy scans → frontend lint/format/dead-code/typecheck/i18n scans → exclusion scan → backend coverage → frontend coverage, with 100% coverage required.

If it fails, run `make fix-all` first to auto-fix what's possible, then address remaining issues manually.
