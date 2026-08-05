# Copilot Agent – Post-Change Quality Gate

Permanent instruction: After any code rewrite or significant change and once unit tests are green locally, always run
the complete quality gate using the consolidated Makefile targets before creating a PR or finishing tasks.

## CRITICAL CODING STANDARDS

**TypeScript/Python Type Safety (MANDATORY):**

- ALL test functions MUST include return type annotation `-> None:`
- ALL function arguments MUST include type annotations (e.g., `arg: SomeType`)
- NO unit test functions without proper typing allowed
- Apply mypy-compliant typing standards to ALL Python code
- When generating/refactoring/modifying unit tests, be STRICT about types
- NO `Any` in new code — use concrete types, TypedDicts, `JsonObject`, or Pydantic models
    - `Any` is allowed ONLY at system boundaries: external SDK/WS raw payloads (first parse layer), SQLAlchemy expression internals, OpenAPI/JSON schema manipulation, generic wrappers (`_with_retry`)
    - For genuinely arbitrary JSON data, use `JsonObject` (`dict[str, JsonValue]`) from `snapper.core.json_types`
    - For internal data transfer (repo rows, batch upserts), use TypedDicts from `snapper.data.repository_types`
    - Process parameters use Pydantic models from `snapper.application.process_manager.process_parameters`

**Python Import Rules (MANDATORY):**

- ALL imports MUST be at the top of the file (after module docstring)
- NO `from __future__ import annotations` allowed
- NO `if TYPE_CHECKING:` blocks allowed
- Use direct imports and proper type annotations instead
- `__init__.py` files MUST be empty - NO re-exports, NO imports
- Example valid `__init__.py`: empty file or just docstring

**Python Comments (MANDATORY):**

- NO `#` comments in Python source files (including inline/trailing comments)
- Put rationale, guidance, and decisions into docstrings (module/class/function/test pydoc)
- Enforced via `make check-no-comments` (checks COMMENT tokens; `#` inside strings/docstrings is allowed)

**Test Warnings (MANDATORY):**

- Tests MUST produce ZERO pytest warnings (RuntimeWarning, DeprecationWarning, etc.)
- Common pitfall: `AsyncMock()` makes ALL attributes async, including synchronous methods like `session.add()`. Fix: `AsyncMock(add=MagicMock())` for synchronous methods on async mocks.
- Run `make test` for the default warning-clean suite (`tests/` plus `proprietary/tests/` when present). For targeted coroutine-warning debugging, run pytest with `-W error::RuntimeWarning` against the same paths under the isolated test DB.
- Warnings degrade signal quality and mask real issues. Fix immediately, never leave for later.

**Function Complexity (MANDATORY):**

- New and changed functions must satisfy Ruff `C901 <= 10`, `PLR0912 <= 12`, `PLR0913 <= 5`, and
  `PLR0915 <= 50`.
- Existing debt is grandfathered per exact file and rule, then pinned by symbol and metric in
  `scripts/complexity_baseline.json`. Never refresh that baseline to admit unrelated new debt.
- The adoption snapshot is complete and the shipped checker has no re-baselining command. Never regenerate the
  baseline wholesale.
- Run `make check-complexity-ratchet` after refactoring a grandfathered file. Improvements require lowering or
  removing the corresponding baseline and per-file entry so the ratchet cannot become stale.

**Language Requirements (MANDATORY):**

- Keep all content inside source code files (identifiers, docstrings, comments, log messages, UI strings, CLI output, runtime content) in English.
- Write and maintain Markdown documentation (README, docs/*.md) in English.
- Maintain Markdown using a strict CommonMark-compatible structure (indent nested content by four spaces, keep required blank lines) so the Python `markdown` renderer produces correct HTML/PDF output.
- Exception: translation catalog files under `frontend/src/locales/**/*.json` and `ios/Snapper/Resources/Localization/Localizable.xcstrings` are exempt — they contain UI copy for all supported locales. Polish (and any future-locale) characters must be stored as UTF-8 codepoints, not Unicode escapes.

## Project Memory and Plans (MANDATORY)

When the `proprietary/` submodule is present, it holds all durable project notes:

- Memory (feedback, project notes, references) lives in `proprietary/memory/`, indexed by
    `proprietary/memory/MEMORY.md`. Read that index at the start of a session, and add a one-line pointer to it
    for every new memory file.
- Implementation plans live in `proprietary/plans/`.

Never write these to an assistant's private or default location (for example `~/.claude/projects/*/memory/`,
`~/.claude/plans/`, or any tool-specific scratch directory). They must be committed with the repository so that
every agent, every session, and every host sees the same notes.

## Checklist (ALWAYS run before finishing tasks or creating PR)

**Primary Quality Gate (REQUIRED):**

- `make check-all` - Runs complete quality gate: backend checks + frontend checks + exclusion scan + backend coverage + frontend coverage (100%)

**Individual steps (if needed for debugging):**

1) Format the code

- `make fmt` (CI-style check) or `make fmt-fix` (auto-fix)

2) Lint

- `make lint`

3) Type-check

- `make typecheck`

4) Backend policy scans

- `make check-docstrings`
- `make check-complexity-ratchet`
- `make check-no-comments`
- `make check-main-guard`
- `make check-init-files`
- `make check-temporal-mutations`
- `make check-vendor-neutral`
- `make check-pydantic-routes`
- `make check-egress-compose`

5) Unit tests

- `make test`

6) UI/Frontend checks

- `make ui-lint` - ESLint checks for frontend
- `make ui-format` - Prettier format checks for frontend
- `make ui-dead-code` - Dead code analysis for frontend
- `make ui-typecheck` - TypeScript type checking
- `make ui-check-types` - Generated frontend/iOS type and backend i18n drift check
- `make ui-i18n-check` - Frontend hardcoded-string i18n scan
- `make ui-i18n-check-alerts` - Verify iOS alerts catalog parity
- `make ui-i18n-check-market` - Verify iOS market catalog parity

7) Exclusion scan (NO pragma/noqa/ignore comments allowed)

- `make check-exclusions` - Fails if any coverage/lint bypass comments exist

8) Coverage (must be 100% - TDD requirement)

- `make cov`
    - The repo enforces 100% coverage via `[tool.coverage.report]`
      `fail_under = 100` in `pyproject.toml`. There is **no omit list** — the
      measured scope is exactly `[tool.coverage.run]` `source`, which covers
      `src/snapper`, `proprietary/src`,
      `integrations/snapper-delegate/src/snapper_delegate` and `scripts`, with
      `branch = true`. Anything added under those roots must reach 100% line
      AND branch coverage; there is no sanctioned way to exempt a file.
- `make ui-cov`
    - The frontend enforces 100% coverage thresholds via `frontend/vite.config.ts`.

9) Full verification (REQUIRED before task completion)

- `make check-all` - Complete quality gate: backend + frontend + exclusions + backend coverage + frontend coverage (100%)

## Database Backend for Tests vs. Local Server (MANDATORY)

Two distinct DB usages, do not mix:

1. **Tests and coverage** (`make test`, `make cov`, `make test-serial`,
    `make cov-serial`, `make check-all`) use an **isolated SQLite fixture
    at the fixed path `./data/dev.db`** by default, regardless of what's in
    `.env`. Every SQLite test or coverage invocation rebuilds that fixture
    from scratch. The builder holds a cross-platform lock, migrates and
    seeds a unique same-directory staging database, validates its WAL
    checkpoint and integrity, then atomically publishes it. The destructive
    target is literal and cannot be redirected with a Make command-line
    variable.
2. **Local server runs** (`make dev-backend`, `make run-server`,
    `make run-static`, `make dev-all`) read `DB_URL` from `.env`. Snapper
    deployments use Postgres there; local-only setups can keep SQLite.

To run tests against Postgres (staging integration only, never against
a production database): `make test TEST_DB_URL=postgresql+asyncpg://USER:PASS@HOST/DB`.
A Postgres `TEST_DB_URL` bypasses the local SQLite bootstrap entirely.
Never override `TEST_DB_URL` to point at the live server's database —
tests mutate state and `isolated_sqlite_db` only protects SQLite paths.

**Auto-fix commands:**

- `make fix` - Backend fixes: fmt-fix + lint-fix + move-imports
- `make fix-all` - Complete fixes: backend + frontend

## Git Commit Attribution (MANDATORY)

Every commit created by an AI assistant MUST include a `Co-authored-by:` trailer.

Rules:

- Use the canonical Git trailer form: `Co-authored-by: Name <email>`
- Choose identity in this order:
    - product name + model/version, only when the assistant exposes a reliable version string
    - otherwise, stable product identity only
- Choose email in this order:
    - provider no-reply address when available
    - otherwise, provider contact/product address officially defined for that assistant identity
- Separate the trailer from the commit body with a blank line
- Use an identity/address only if it is explicitly configured and confirmed for the assistant in use
- Do not invent a version string or email address

Use a HEREDOC to pass the commit message (avoids shell escaping issues):

```bash
git commit -m "$(cat <<'EOF'
commit message here

Co-authored-by: ...
EOF
)"
```

Cross-platform alternative (works in CMD, PowerShell, Git Bash, WSL):

```bash
git commit -m "commit message here" -m "Co-authored-by: ..."
```

Examples (illustrative; use only verified configured identities):

- `Co-authored-by: Claude Opus 4.6 <noreply@anthropic.com>`
- `Co-authored-by: Claude Sonnet 4.6 <noreply@anthropic.com>`
- `Co-authored-by: GitHub Copilot (Gemini 3.1 Pro) <copilot@github.com>`
- `Co-authored-by: Google Deepmind Antigravity <noreply@google.com>`
- `Co-authored-by: OpenAI Codex (GPT-5) <codex@openai.com>`
- `Co-authored-by: OpenAI Codex (GPT-5.3-Codex) <codex@openai.com>`

## IMPORTANT: Task Completion Protocol

Before finishing ANY task or making changes:

1. ALWAYS run `make check-all` to verify backend + frontend quality + coverage
2. Review affected documentation after `make check-all` passes and before any git commit
    - Check `README.md`, `docs/*.md`, and `.env.example` when behavior, commands, configuration, architecture, API contracts, messaging topics, or workflows changed
    - Update documentation to match the current code before creating the commit
3. Only mark task complete if `make check-all` passes successfully

Notes

- SQLite is the default DB; PostgreSQL can be used by switching `DB_URL` without code changes.
- Ensure Alembic migrations are up-to-date for any ORM model change.
- Keep README and .env.example consistent with current behavior when relevant changes are made.
