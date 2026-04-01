---
name: fix
description: Auto-fix formatting, linting, and imports (make fix-all)
user-invocable: true
allowed-tools:
  - Bash
---

Run all auto-fixers:

```bash
make fix-all
```

This runs: ruff fix → isort → black → frontend lint fix → prettier → dead code fix.

After fixing, report what changed. If issues remain that can't be auto-fixed, list them.
