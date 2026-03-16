# Development

Guidelines for developers working on the Snapper project.

## Requirements

- Python 3.14+
- Poetry
- Node.js 25+ and pnpm
- TA-Lib (C library)
- Pre-commit hooks

## Environment Setup

```bash
# Install system dependencies (macOS)
make system-deps

# Install Python dependencies
make setup

# Install frontend dependencies
make ui-setup

# Synchronize pre-commit hooks
make pre-refresh
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
5.  No `#` comments
6.  No pragma/noqa/ignore
7.  Tests with 100% coverage
8.  Frontend (ESLint, Prettier, dead code)

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
def process_signal(signal: Signal, config: dict[str, Any]) -> Order | None:
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
from typing import Any

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
    repo = Repository(db_url)
    async with repo.session() as session:
        candles = await repo.get_candles(session, "BTC-USD", limit=10)
        assert len(candles) <= 10
```

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

Configuration in `.coveragerc`.

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
| `make ui-dead-code` | Knip dead code |
| `make ui-test` | Vitest tests |
| `make ui-cov` | Tests with coverage |

### Fixes

```bash
make ui-fix   # lint-fix + format-fix + dead-code-fix
```

### Type Generation

```bash
make ui-gen-types   # All types from OpenAPI + WebSocket
```

## Database Migrations

### Dev DB Location

The development SQLite database lives at `./data/snapper.db`.

### Destructive Migration Strategy (Development)

The project uses a single-file migration (`0001_init.py`) that is rewritten in
place when the schema changes.  After editing the migration, reset and reseed:

```bash
rm -f data/snapper.db && make migrate-dev
```

`make migrate-dev` runs both `make migrate` (applies Alembic migrations via
`snapper db-init`) and `make seed` (seeds development data via
`snapper db-seed --profile dev`).  Never use bare `make migrate` after a DB
reset -- always use `make migrate-dev` so seed data is included.

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
make docker-build
```

### Run

```bash
make docker-run
```

### docker-compose

```bash
docker-compose up -d
```

## CI/CD

Pipeline executes:

1.  `make check` — Quality checks
2.  `make cov` — Tests with coverage
3.  `make ui-check` — Frontend checks
4.  `make ui-cov` — Frontend tests

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
│   ├── indicators/        # Technical indicators
│   ├── infrastructure/    # External integrations
│   ├── interface/         # WebSocket
│   ├── messaging/         # ZeroMQ
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

logger.info("Processing signal", signal=signal)
logger.debug("Calculated RSI", value=rsi_value)
logger.error("Failed to execute order", error=str(e))
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

Generates `snapper.pdf` from README + docs/*.md.
