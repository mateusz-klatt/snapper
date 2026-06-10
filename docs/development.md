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
9.  Egress compose safety check (`check-egress-compose`)
10. No pragma/noqa/ignore exclusions
11. Vendor-neutrality check (`check-vendor-neutral`)
12. Pydantic-only FastAPI I/O (`check-pydantic-routes`)
13. Tests with 100% coverage
14. Frontend (`ui-typecheck`, ESLint, Prettier, dead code, i18n checks, tests with coverage)

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
| `make cov` | Tests with coverage |
| `make check-egress-compose` | Reject unsafe snapper-egress compose wiring |

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

### 100% Coverage

The project requires 100% code coverage (TDD). Check:

```bash
make cov
```

Coverage is configured under `[tool.coverage.run]` /
`[tool.coverage.report]` in `pyproject.toml`. The `fail_under = 100`
threshold enforces the TDD requirement.

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
| `make ui-i18n-check` | Frontend locale/catalog consistency |
| `make ui-test` | Vitest tests |
| `make ui-cov` | Tests with coverage |

### Fixes

```bash
make ui-fix   # lint-fix + format-fix + dead-code-fix
```

### Type Generation

```bash
make ui-gen-types   # OpenAPI types, WebSocket types, Zod schemas, entity aliases, permissions
```

## Internationalization

The frontend i18n catalogs in `frontend/src/locales/<lang>/*.json` are
the source of truth for translated UI strings across 45 locales. iOS
mirrors the subset of strings used on the native client by porting
them into `ios/Snapper/Resources/Localization/Localizable.xcstrings`.

### Market catalog (description / asset class / sector)

The `market.description.*`, `market.assetClass.*`, and `market.sector.*`
namespaces are ported from the frontend JSON into the iOS xcstrings
by `scripts/port_market_catalog.py`. Run after editing any
`frontend/src/locales/<lang>/market.json` file under one of those
namespaces:

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

### Alerts catalog (historical direction: xcstrings → JSON)

Historically the iOS alerts catalog was the source of truth and
frontend caught up via `scripts/port_ios_alert_catalog.py`. That
direction is preserved for back-compatibility; new namespaces should
flow frontend → iOS via the market-catalog pattern above.

## Database Migrations

### Dev DB Location

The local server SQLite database lives at `./data/snapper.db`.
Test and coverage Make targets use an isolated SQLite fixture at
`./data/dev.db` by setting `DB_URL=sqlite+aiosqlite:///./data/dev.db`
inside the Makefile. This keeps `make test`, `make cov`, and
`make check-all` from mutating the database used by a running local
server, even when `.env` points at `./data/snapper.db` or PostgreSQL.

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

Pipeline executes the same consolidated gate used locally:

1.  `make check-all` — Backend checks, frontend checks, exclusion scan, and tests with coverage

For debugging a failing pipeline locally, the equivalent steps are:

1.  `make check` — Backend quality checks
2.  `make ui-check` — Frontend lint, format, dead-code, type, and i18n catalog checks (`ui-lint ui-format ui-dead-code ui-typecheck ui-i18n-check ui-i18n-check-alerts ui-i18n-check-market`)
3.  `make check-exclusions` — No pragma/noqa/ignore bypasses
4.  `make cov` — Backend tests with coverage
5.  `make ui-cov` — Frontend tests with coverage

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
├── frontend/              # React dashboard
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
