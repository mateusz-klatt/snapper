---
name: check
description: Run the complete Snapper quality gate with make check-all. Use before committing, opening a pull request, or finishing a code or documentation task.
---

Run the complete quality gate:

```bash
make check-all
```

This runs: backend format/lint/type checks and policy scans → frontend lint/format/dead-code/type/i18n checks → generated type-drift checks → MCP bridge contract/build/tests → exclusion scan → backend coverage → frontend coverage, with 100% coverage required.

If a format, lint, import-placement, or frontend dead-code step fails, use `make fix-all` for the mechanical repair. Diagnose type, policy, drift, bridge, test, and coverage failures at their failing target instead of creating unrelated auto-fix churn.
