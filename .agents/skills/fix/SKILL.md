---
name: fix
description: Auto-fix Snapper formatting, linting, imports, and frontend dead-code findings with make fix-all. Use when the quality gate reports mechanically fixable issues.
---

Run all auto-fixers:

```bash
make fix-all
```

This runs: Ruff safe fixes → isort → Black → Ruff lint fixes → import relocation → frontend ESLint fixes → Prettier → dead-code fixes.

After fixing, inspect and report what changed, then run:

```bash
make check-all
```

If issues remain that cannot be auto-fixed, list them with their failing targets.
