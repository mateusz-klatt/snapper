# Development

Guidelines for developers working on the Snapper project.

## Requirements

- Python 3.14+
- Poetry
- Node.js 26+ and pnpm 11+ (frontend engines, Docker UI build, and CI
  workflows all standardize on Node 26)
- Build tools for Python packages. TA-Lib is optional at runtime: the
  indicator adapter uses TA-Lib when importable and falls back to pure
  Python implementations otherwise.
- Pre-commit hooks

## Environment Setup

Initialize the public submodules before installing frontend tools or running the
full gate; this HTTPS override also works without GitHub SSH credentials:

```bash
git -c url."https://github.com/".insteadOf=git@github.com: \
  submodule update --init frontend integrations/snapper-mcp ios
```

The private `proprietary` and `data/polygon` submodules require separate access
and are optional for the public checkout.

```bash
# Install system dependencies (Linux/macOS)
make system-deps

# Install Python dependencies
make setup

# Install frontend dependencies
make ui-setup

# Synchronize pre-commit hooks
make pre-refresh

# Refresh dependencies, local tools, and Dockerfile tool pins
make update
```

`make refresh` upgrades Python, frontend, and MCP dependencies, GitHub Actions,
and pre-commit hooks. The Python refresh also reinstalls the current project
so its installed dependency metadata matches the updated manifest and lock file.
`make update` additionally refreshes Docker build-tool pins.

CCXT currently uses a [reproducible dependency metadata patch](../vendor/ccxt/README.md)
so its exact urllib3 requirement includes the current security fixes. Keep the
vendored wheel in source checkouts and Docker build contexts; the linked note
documents verification, rebuilding, distribution limits, and retirement of the patch.

## Isolated Worktrees for Parallel Sessions

When more than one agent or developer session works on the repository at
the same time, a shared checkout makes every quality-gate verdict
unattributable: a red result cannot be assigned to either change, and a
green one proves nothing about either. Concurrent frontend coverage runs
in one checkout also corrupt each other through the shared
`frontend/coverage/.tmp/` directory. Run each session in its own git
worktree instead:

```bash
make worktree NAME=my-task
```

This creates `../snapper-wt-<NAME>` on a fresh `wt/<NAME>` branch cut
from `master` and prepares the full verification harness:

- initializes the `frontend`, `integrations/snapper-mcp`, `ios`, and
    `proprietary` submodules, because missing submodules manufacture
    failures that look inherited but are environmental
- writes a minimal `.env` pointing `DB_URL` at the isolated SQLite test
    fixture (`./data/dev.db`); the real `.env` is never copied because it
    carries live venue credentials
- builds a dedicated `.venv` and asserts that `import snapper` resolves
    inside the worktree rather than the main checkout
- installs frontend dependencies, reuses the main checkout's
    `frontend/dist` through a symlink when one exists, and installs
    `snapper-mcp` node modules

The target requires access to the private `proprietary` repository as well as a
POSIX shell, and is not available in native Windows shells. For a public-only
checkout, create a worktree with Git and initialize only the public submodules
using the setup command above, then install its dependencies and build its UI. Tear
a worktree down with:

```bash
git worktree remove --force ../snapper-wt-<NAME>
git branch -D wt/<NAME>
```

## Quality Gates

### Main Command

```bash
make check-all
```

Executes the complete quality gate:

1.  Formatting (ruff, black, isort)
2.  Linting (ruff)
3.  Type checking (mypy)
4.  Docstring compliance
5.  No `#` comments in Python
6.  Canonical `__main__` guards
7.  Empty `__init__.py` files
8.  No forbidden temporal mutations
9.  Vendor-neutrality check (`check-vendor-neutral`)
10. Pydantic-only FastAPI I/O (`check-pydantic-routes`)
11. Egress compose safety and delegate/read-visibility boundary checks
    (`check-egress-compose`, `check-delegate-boundary`, `check-read-visibility-boundary`)
12. Frontend checks (`ui-typecheck`, ESLint, Prettier, dead code, i18n checks)
13. Generated frontend/iOS type and backend i18n drift check (`ui-check-types`)
14. MCP wire-contract drift, public prose, type, lint, test, and stdout checks (`bridge-check`)
15. No pragma/noqa/ignore exclusions
16. Backend tests with 100% line and branch coverage of the configured source roots
17. Frontend tests with 100% coverage

### Individual Steps

| Command | Description |
| ------- | ----------- |
| `make fmt` | Check formatting |
| `make fmt-fix` | Fix formatting |
| `make lint` | Ruff linting |
| `make lint-fix` | Fix linting issues |
| `make typecheck` | Mypy type checking |
| `make test` | Tests (parallel) |
| `make test-serial` | Tests (sequential, debug) |
| `make test-integration` | Paper-mode E2E tests (`tests/integration/`) |
| `make cov` | Tests with coverage |
| `make cov-xml` | Export coverage XML for SonarCloud (after `make cov`) |
| `make check-egress-compose` | Reject unsafe snapper-egress compose wiring |

`make check-egress-compose` scans Compose files and override files for
the snapper-egress sidecar contract. The sidecar must share the unified
Snapper image, run through `command: ["egress"]`, run as root with
`NET_ADMIN`, avoid host `ports:` / `network_mode: host`, and share the
SQLite data mount when the monolith uses `/app/data`. The monolith stays
unprivileged: no `cap_add` and no `user:` override.

### Automatic Fixes

```bash
make fix        # Backend: fmt-fix + lint-fix + move-imports
make fix-all    # Backend + frontend
```

## Code Standards

### Types (MANDATORY)

All functions must have type annotations:

```python
def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    """Calculate RSI indicator."""
    ...

async def fetch_data(symbol: str) -> list[Candle]:
    """Fetch candle data."""
    ...
```

Test functions must have `-> None`:

```python
async def test_rsi_calculation() -> None:
    """Test RSI calculation returns expected values."""
    ...
```

### Docstrings (Google style)

```python
def process_signal(signal: Signal, config: StrategyConfig) -> Order | None:
    """Process trading signal and generate order.

    Takes a trading signal and configuration, validates the signal,
    and generates an appropriate order if conditions are met.

    Args:
        signal: Trading signal to process.
        config: Strategy configuration dictionary.

    Returns:
        Generated order if valid, None otherwise.

    Raises:
        ValidationError: If signal data is invalid.
    """
    ...
```

### No `#` Comments

`#` comments are forbidden. Use docstrings:

```python
# WRONG
def foo():
    # This calculates something
    x = 1 + 1

# CORRECT
def foo():
    """Calculate the sum."""
    x = 1 + 1
```

### Imports

Imports at the top of file, single lines:

```python
from datetime import UTC
from datetime import datetime

import pandas as pd
from sqlalchemy import select

from snapper.config.settings import get_settings
from snapper.data.models import Candle
```

Forbidden:

- `from __future__ import annotations`
- `if TYPE_CHECKING:` blocks
- Re-exports in `__init__.py`

### `__init__.py`

`__init__.py` files must be empty (or docstring only):

```python
"""Snapper strategies module."""
```

## Tests

### Structure

```
tests/
├── conftest.py          # Shared fixtures
├── api/                 # API tests
├── application/         # Business logic tests
├── cli/                 # CLI tests
├── data/                # Data layer tests
├── indicators/          # Indicator tests
├── integration/         # Paper-mode E2E tests
├── messaging/           # ZMQ tests
├── strategies/          # Strategy tests
└── ...
```

### Writing Tests

```python
import pytest
from snapper.indicators.rsi import rsi


class TestRSI:
    """Test suite for RSI indicator."""

    def test_rsi_returns_series_with_correct_length(self) -> None:
        """
        Given a price series of length N
        When RSI is calculated with period P
        Then the result has length N.
        """
        prices = pd.Series([100, 102, 101, 103, 105])
        result = rsi(prices, period=14)
        assert len(result) == len(prices)

    def test_rsi_values_in_valid_range(self) -> None:
        """
        Given any price series
        When RSI is calculated
        Then all values are between 0 and 100.
        """
        prices = pd.Series([100, 102, 101, 103, 105, 104, 106])
        result = rsi(prices, period=3)
        assert (result >= 0).all()
        assert (result <= 100).all()
```

### Async Tests

```python
import pytest


@pytest.mark.asyncio
async def test_fetch_candles() -> None:
    """Test fetching candles from repository."""
    repo = get_repository(db_url)
    candles = await repo.get_candles("BTC-USD", limit=10)
    assert len(candles) <= 10
```

Tests must produce zero pytest warnings. A common pitfall:
`AsyncMock()` makes **every** attribute on the mock async, including
synchronous methods such as `session.add()` or `session.expunge()`.
The fix is to override the synchronous attributes explicitly:

```python
from unittest.mock import AsyncMock, MagicMock

session = AsyncMock(add=MagicMock(), expunge=MagicMock())
```

Run `python -m pytest tests/ -W error::RuntimeWarning` to fail the
suite on any unawaited-coroutine warnings.

### Fixtures

```python
@pytest.fixture
def sample_candles() -> list[Candle]:
    """Provide sample candle data for tests."""
    return [
        Candle(open=100, high=105, low=95, close=102, volume=1000),
        Candle(open=102, high=110, low=100, close=108, volume=1200),
    ]
```

### Integration Tests

```bash
make test-integration
```

Runs the paper-mode end-to-end tests in `tests/integration/` with a
120-second per-test timeout. The fixtures spin up a real
`ZmqBrokerThread`, a `PaperOrderExecutor`, and a `TraderCoordinator`
against the SQLite test fixture, so the full order path is exercised
without touching a live venue. These tests carry the `integration`
pytest marker and are excluded from `make test` / `make cov` (the
default `addopts` select `-m 'not integration'`).

### 100% Coverage

The project requires 100% code coverage (TDD). Check:

```bash
make cov
```

Coverage is configured under `[tool.coverage.run]` /
`[tool.coverage.report]` in `pyproject.toml`. The `fail_under = 100`
threshold enforces line and branch coverage for `src/snapper`,
`proprietary/src` when present, `integrations/snapper-delegate/src/snapper_delegate`,
and `scripts`. There is no file omit list.

## Frontend

### Setup

```bash
make ui-setup
```

### Development

```bash
make ui-dev
```

Frontend at `http://localhost:3000/` with backend proxy.

### Quality Checks

| Command | Description |
| ------- | ----------- |
| `make ui-lint` | ESLint |
| `make ui-format` | Prettier check |
| `make ui-typecheck` | TypeScript type check |
| `make ui-dead-code` | Knip dead code |
| `make ui-i18n-check` | Hardcoded user-facing string scan (frontend TSX/TS) |
| `make ui-i18n-check-alerts` | iOS alerts.* xcstrings and frontend alerts.json in sync |
| `make ui-i18n-check-market` | Frontend market.* JSON and iOS xcstrings in sync |
| `make ui-test` | Vitest tests |
| `make ui-cov` | Tests with coverage |

### Fixes

```bash
make ui-fix   # lint-fix + format-fix + dead-code-fix
```

### Type Generation

```bash
make ui-gen-types   # Frontend OpenAPI/WS types, Zod schemas, entity aliases, permissions
make ios-gen-types  # Swift models from OpenAPI + WebSocket schemas
make ts-bridge      # snapper-mcp bridge wire contract only
make bridge-regen   # Regenerate bridge contract and run bridge checks
make bridge-check   # Verify committed bridge contract without regenerating
make ui-check-types # Non-destructive drift check for generated types and backend i18n catalogs
```

The shared generator is `scripts/generate_types.py`. With no explicit
target it runs `--all`, which covers the main-repo frontend/iOS export
path but intentionally excludes the snapper-mcp bridge because that
target writes across the integration boundary. Use `--bridge` (or the
Makefile targets above) when the bridge wire contract must be updated.
`make bridge-check` verifies the committed wire-contract file against
the current backend schemas (`scripts/bridge_check/check_drift.py` and
`check_oss_prose.py`) and runs the bridge npm stack (typecheck, lint,
tests, stdout gate) without rewriting any files.
`scripts/check_type_drift.py` backs up generated files, runs
`make ui-gen-types ios-gen-types gen-backend-i18n-catalog`, compares
the result, then restores the working tree.

## Internationalization

The frontend i18n catalogs in `frontend/src/locales/<lang>/*.json` are
the source of truth for translated UI strings across 45 locales. iOS
mirrors the subset of strings used on the native client by porting
them into `ios/Snapper/Resources/Localization/Localizable.xcstrings`.
Backend `user.default_language` validation accepts the union of the
iOS and frontend catalog code forms: 45 iOS cases plus 45 frontend
cases, with five naming divergences (`pt-BR`/`pt`, `nb`/`no`,
`zh-Hans`/`zh`, `sr-Latn`/`sr`, `my`/`my-MM`) for 50 accepted codes.
Backend lookup resolves those five frontend aliases to the corresponding
iOS catalog; explicit Chinese scripts remain distinct. Initial client selection
uses the preferred language and script, with a valid saved selection taking
precedence. Display locale never determines an instrument's quote currency.

### Backend alert catalog

`scripts/gen_backend_i18n_catalog.py` reads the iOS xcstrings file and
writes committed backend JSON catalogs to
`src/snapper/i18n/catalogs/<lang>.json`. The backend catalog is limited
to alert title/body templates and application-owned argument labels across
45 languages, one JSON file per language. It is generated with `make gen-backend-i18n-catalog`
and checked by `scripts/check_type_drift.py`.

Known side, health-status and fallback-reason values are localized at render
time; persisted arguments remain stable so iOS can re-render after a language
change. Instrument, venue and instance identifiers and arbitrary external
diagnostics retain their original text. APNs receives server-rendered strings.

New fill alerts use `alerts.body.order_fill_full_quoted` with the existing five
arguments. The price argument preserves execution precision and appends the
quote currency from a matching historical instrument reference. Known
currency-leg revisions are treated as ambiguous. If identity or unambiguous
quote metadata is missing, it contains only the price rather than an assumed dollar amount.
Older iOS clients fall back to the server-rendered body for this unknown key.
Historical rows retain their recorded price precision; it cannot be recovered
from localization arguments. New unresolved-order alerts similarly use
`alerts.body.order_unknown_unresolved` so older clients retain the server's
explicit instruction not to assume the position is closed.

### Market catalog

The `market.description.*`, `market.assetClass.*`,
`market.sector.*`, `market.related.*`, `market.pairStats.*`, and
`market.cacheBanner.*` namespaces are ported from the frontend JSON
into the iOS xcstrings by `scripts/port_market_catalog.py`. Run after
editing any `frontend/src/locales/<lang>/market.json` file under one of
those namespaces:

```bash
python scripts/port_market_catalog.py        # write
python scripts/port_market_catalog.py --check  # CI parity gate
```

The `--check` mode is wired into `make ui-check` via
`ui-i18n-check-market`, so a forgotten regeneration fails the
backend + frontend quality gate. Placeholder tokens
(`{{name}}` / `{{assetClass}}`) are rewritten to xcstrings positional
codes (`%1$@`, `%2$@`) based on appearance order in the EN template.

iOS-side coverage of the resulting catalog is enforced by
`CatalogParityTests` (45 locales × every key declared in
`SnapperTests/I18n/ExpectedKeys.swift`); adding a new key to the
catalog requires adding it to `ExpectedKeys.swift` in the same iOS PR.

`make ios-i18n-check` (run from the main repo) delegates into the ios
submodule Makefile and lints Swift call sites: bare
`String(localized:)` usage under `ios/Snapper` is rejected in favor of
the `LocaleStrings` helper, so catalog lookups follow the selected app
locale rather than `Locale.current`. An allowlist file
(`ios/scripts/check_i18n_allowlist.txt`) exempts specific paths.

### Alerts catalog (historical direction: xcstrings → JSON)

Historically the iOS alerts catalog was the source of truth and
frontend caught up via `scripts/port_ios_alert_catalog.py`. That
direction is preserved for back-compatibility; new namespaces should
flow frontend → iOS via the market-catalog pattern above. The alert
port processes all `alerts.*` keys across 45 locales, writes
one `frontend/src/locales/<lang>/alerts.json` file per locale, and
updates `common.nav.alerts` from the `alerts.navTitle` xcstrings key.
The five divergent locale directory names are remapped between iOS and
frontend in the same way as `SUPPORTED_LANGUAGES`.

## Scanner Behavior

The backend quality scanners are intentionally source-tree scanners,
not import-time checks. `check_docstrings.py` scans `src`, `tests`,
`proprietary` when present, and `scripts`; with the Makefile flags it
enforces module/public docstrings, BDD-style test docstrings, and
Google-style `Args:` / `Returns:` sections. `check_no_comments.py`
tokenizes Python under `src`, `tests`, `scripts`, `proprietary/src`,
and `proprietary/tests`; hashes inside strings and docstrings are
allowed, but real comment tokens fail strict mode. `check_pydantic_routes.py`
walks `src/snapper/server`, `src/snapper/auth`, `src/snapper/config`,
and `src/snapper/api`, then filters for FastAPI route functions whose
I/O must use concrete Pydantic schemas rather than `dict`, `Any`, or
raw response types.

The remaining scanners are similarly path-scoped: coverage/lint
exclusions scan Python in `src`, `tests`, `scripts`, optional
`proprietary` code, and frontend TypeScript; temporal-mutation and
vendor-neutrality scans walk `src/snapper`; `check_init_files.py`
checks configured `__init__.py` roots for docstring-only files; and
`check_main_guard.py` requires every discovered entry point to end with
`raise SystemExit(main())`.

## Database Migrations

### Dev DB Location

The local server SQLite database lives at `./data/snapper.db`.
Test and coverage Make targets use an isolated SQLite fixture at
`./data/dev.db` by setting `DB_URL=sqlite+aiosqlite:///./data/dev.db`
inside the Makefile. This keeps `make test`, `make cov`, and
`make check-all` from mutating the database used by a running local
server, even when `.env` points at `./data/snapper.db` or PostgreSQL.

The fixture is rebuilt automatically before every SQLite test or coverage
invocation. `make migrate-dev-sqlite` rebuilds it explicitly using the same
locked, staged migration and dev-seed process. The builder validates the
new database before atomically replacing `./data/dev.db`; no manual
deletion is needed.

The fixture URL is the `TEST_DB_URL` Make variable (default
`sqlite+aiosqlite:///./data/dev.db`). Override it to run the suite
against a PostgreSQL staging database:

```bash
make test TEST_DB_URL=postgresql+asyncpg://USER:PASS@HOST/DB
```

Never point `TEST_DB_URL` at a live server's database: tests mutate
state, and the `isolated_sqlite_db` conftest fixture only protects
SQLite paths.

### Migration Strategy (Development)

The schema is built from a chain of sequential Alembic revisions
(`0001_init.py`, `0002_instrument_feed_health.py`, ...), each of which
revises the previous one.  To change the schema, add a new numbered
revision rather than editing an existing one, then apply and reseed:

```bash
make migrate-dev
```

`make migrate-dev` applies Alembic migrations (via `snapper db-init`) then
seeds development data (via `snapper db-seed --profile dev`).  There is no
standalone `make migrate` target -- always use `make migrate-dev` or
`make migrate-prod`.

### Applying (Production)

```bash
make migrate-prod
```

### Rollback

```bash
snapper db-downgrade
```

## Docker

### Build

```bash
make docker-build-dev    # Development image
make docker-build-prod   # Production image
make docker-migrate-dev  # Initialize and seed the Docker SQLite database
```

### Run

```bash
make docker-migrate-dev
make docker-run
```

### Docker Compose

```bash
make docker-migrate-dev
docker compose up -d
```

## CI/CD

The pipeline (`.github/workflows/ci.yml`) wraps the consolidated gate
used locally in additional build and smoke stages:

1.  Setup — `make system-deps`, `make setup`, `make ui-setup`
2.  `make ui-build` — Production frontend bundle
3.  `make migrate-dev` — Initialize and seed the database
4.  `make ui-check-types` — Drift check for generated types and backend i18n catalogs
5.  `make check-all` — Backend checks, frontend checks, generated-type drift, exclusion scan, and tests with coverage
6.  `make cov-xml` — Export coverage XML, consumed by the SonarCloud scan step
7.  Docker smoke stage — `make docker-build-dev`, `make docker-migrate-dev`, `docker compose up` for the `snapper` and `snapper-web` services, then a `make server-check` health check

SonarCloud analyzes checked-out backend code, data, migrations, and tooling without
coverage, duplication, or issue suppressions. The parent project excludes
`frontend/`, `integrations/snapper-mcp/`, and `ios/` because those public submodules
have independent SonarCloud projects. It also carries one temporary exact-file
analyzer exception for `src/snapper/data/repository.py`: the 35,000-line module
exhausts the scanner JVM while the rest of the project remains analyzable. Local and
CI lint, type, test, and coverage gates still include it. The exception must be
removed after the repository is split into bounded domain modules. Parent-owned
`integrations/snapper-delegate/` remains in the backend analysis; each independent
project likewise suppresses no owned sources or findings.
`tests/meta/test_sonar_configuration.py` pins this boundary and also rejects
equivalent workflow, command-line, and inline overrides. Every project workflow
waits for the pin-versioned SonarCloud Quality Gate after its scan, so a failed gate
fails CI instead of leaving the GitHub check green.

The private `proprietary/` submodule is not initialized by the public CI workflow,
so it is not present in the current SonarCloud analysis. Covering it requires either
a CI credential with read access to that repository or a separate private
SonarCloud workflow/project; the root configuration cannot compensate for an absent
checkout.

For debugging a failing `make check-all` locally, the equivalent steps are:

1.  `make check` — Backend quality checks
2.  `make ui-check` — Frontend lint, format, dead-code, type, and i18n catalog checks (`ui-lint ui-format ui-dead-code ui-typecheck ui-i18n-check ui-i18n-check-alerts ui-i18n-check-market`)
3.  `make ui-check-types` — Generated frontend/iOS type and backend i18n drift check
4.  `make bridge-check` — MCP wire-contract drift, public prose, type, lint, test, and stdout checks
5.  `make check-exclusions` — No pragma/noqa/ignore bypasses
6.  `make cov` — Backend tests with coverage
7.  `make ui-cov` — Frontend tests with coverage

## Workflow

### Before Commit

```bash
make check-all
```

### Before PR

1.  Ensure `make check-all` passes
2.  Add/update tests
3.  Update documentation if needed
4.  Verify 100% coverage

### Code Review Checklist

- [ ] Types for all functions
- [ ] Docstrings (Google style)
- [ ] No `#` comments
- [ ] Tests for new functionality
- [ ] 100% coverage
- [ ] `make check-all` passes

## Project Structure

```
snapper/
├── src/snapper/           # Source code
│   ├── api/               # API schemas
│   ├── application/       # Business logic
│   ├── auth/              # Authentication
│   ├── cli/               # CLI interface
│   ├── config/            # Configuration
│   ├── core/              # Core types
│   ├── data/              # Data layer
│   ├── egress/            # snapper-egress sidecar entry point
│   ├── i18n/              # Backend i18n catalogs
│   ├── indicators/        # Technical indicators
│   ├── infrastructure/    # External integrations
│   ├── interface/        # WebSocket
│   ├── mcp/              # MCP server (delegate-facing tool surface)
│   ├── messaging/        # ZeroMQ
│   ├── server/            # FastAPI
│   ├── strategies/        # Trading strategies
│   └── utils/             # Utilities
├── tests/                 # Tests
├── frontend/              # React dashboard submodule
├── ios/                   # SwiftUI iOS client submodule
├── docs/                  # Documentation
├── scripts/               # Helper scripts
└── data/                  # Local data
```

## Tools

### Ruff

Linter and formatter. Configuration in `pyproject.toml`:

```toml
[tool.ruff.lint]
select = ["E", "F", "W", "I", "B", "UP", "C4", "SIM", "N", "PERF", "ISC", "PLW", "PLC", "A", "TID", "D"]
```

### Mypy

Type checker. Strict mode enabled:

```toml
[tool.mypy]
python_version = "3.14"
strict = true
```

### Black

Formatter. Line length 100:

```toml
[tool.black]
line-length = 100
```

### Pre-commit

Hooks run before commit:

```bash
pre-commit install
pre-commit run --all-files
```

## Debugging

### Logging

```python
from loguru import logger

logger.info(f"Processing signal: {signal}")
logger.debug(f"Calculated RSI: {rsi_value}")
logger.error(f"Failed to execute order: {e}")
```

### ZMQ Logger

```bash
snapper zmq-logger --payload
```

### Serial Tests

```bash
make test-serial
```

### pdb/ipdb

```python
import ipdb; ipdb.set_trace()
```

## Documentation

### Docstrings

Every module, class, and public function must have a docstring.

### docs/

Markdown documentation in `docs/` directory.

### PDF

```bash
make docs-pdf
```

Generates `frontend/public/snapper.pdf` from README + docs/*.md so the docs ship with the frontend bundle and are linked from the login page.
Because `frontend/` is the `snapper-frontend` submodule, commit and push
`frontend/public/snapper.pdf` from inside that submodule, then commit the
updated frontend submodule pointer in the parent Snapper repository.
