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

This runs: format check → lint and complexity ratchet → typecheck → backend policy scans → frontend lint/format/dead-code/typecheck/i18n scans → generated type-drift checks → MCP bridge contract/build/tests → exclusion scan → backend coverage → frontend coverage, with 100% coverage required.

Use `make fix-all` for mechanically fixable formatting, lint, import or frontend dead-code failures. Diagnose type, policy, contract, test and coverage failures at their failing targets before changing unrelated files.
