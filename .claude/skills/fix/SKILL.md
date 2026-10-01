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

This runs: Ruff safe fixes → isort → Black → Ruff lint fixes → import relocation → frontend ESLint fixes → Prettier → dead-code fixes.

After fixing, report what changed. If issues remain that can't be auto-fixed, list them.
