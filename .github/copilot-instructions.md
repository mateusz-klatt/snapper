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
- Run `python -m pytest tests/ -W error::RuntimeWarning` to verify no coroutine warnings.
- Warnings degrade signal quality and mask real issues. Fix immediately, never leave for later.

**Language Requirements (MANDATORY):**

- Keep all content inside source code files (identifiers, docstrings, comments, log messages, UI strings, CLI output, runtime content) in English.
- Write and maintain Markdown documentation (README, docs/*.md) in English.
- Maintain Markdown using a strict CommonMark-compatible structure (indent nested content by four spaces, keep required blank lines) so the Python `markdown` renderer produces correct HTML/PDF output.

## Checklist (ALWAYS run before finishing tasks or creating PR)

**Primary Quality Gate (REQUIRED):**

- `make check-all` - Runs complete quality gate: backend checks + frontend checks + exclusion scan + tests with coverage (100%)

**Individual steps (if needed for debugging):**

1) Format the code

- `make fmt` (CI-style check) or `make fmt-fix` (auto-fix)

2) Lint

- `make lint`

3) Type-check

- `make typecheck`

4) Unit tests

- `make test`

5) UI/Frontend checks

- `make ui-lint` - ESLint checks for frontend
- `make ui-format` - Prettier format checks for frontend
- `make ui-dead-code` - Dead code analysis for frontend

6) Exclusion scan (NO pragma/noqa/ignore comments allowed)

- `make check-exclusions` - Fails if any coverage/lint bypass comments exist

7) Coverage (must be 100% - TDD requirement)

- `make cov`
    - The repo enforces 100% coverage threshold via .coveragerc configuration (TDD).

8) Full verification (REQUIRED before task completion)

- `make check-all` - Complete quality gate: backend + frontend + exclusions + tests with coverage (100%)

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
