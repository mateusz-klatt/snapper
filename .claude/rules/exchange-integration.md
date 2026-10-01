---
description: Pattern for adding a new exchange integration to Snapper
paths:
  - src/snapper/infrastructure/exchanges/**
  - src/snapper/application/updaters/symbols/**
  - src/snapper/messaging/publishers/**
  - src/snapper/infrastructure/market_data/**
---

# Exchange Integration Pattern

When adding a new exchange, follow this checklist in order.
Each step has an existing reference implementation to copy from.

## 1. ExchangeEnum + types

- `src/snapper/core/types.py` — add to `ExchangeEnum`, `MarketSubscribeExchange`, `MarketDataExchange`, `AllExchange`
- If new asset types needed, add to `AssetTypeEnum` + update `AssetType` Literal
- Update the affected ORM CHECK constraints and add a new Alembic migration for existing databases. Editing `0001_init.py` alone does not upgrade databases that already applied it; inspect later migrations for constraints introduced after the initial schema.

## 2. Schemas (Pydantic)

- `src/snapper/infrastructure/exchanges/schemas/{exchange}.py`
- Instrument, Ticker, Trade schemas extending `ExchangeResponse`
- Reference: `schemas/kraken_futures.py`, `schemas/kraken_equities.py`

## 3. Adapters (parse functions)

- `src/snapper/infrastructure/exchanges/adapters/{exchange}.py`
- `parse_{exchange}_ticker()` → `TickerUpdate`
- `parse_{exchange}_trade()` → `TradeUpdate`
- `parse_{exchange}_instrument()` → `InstrumentPairDescriptor`
- Plus `_list` variants and `_tick_size_to_precision` helper
- Reference: `adapters/kraken_equities.py`

## 4. Symbol functions

- `src/snapper/infrastructure/symbols/functions.py` — add `native_to_{exchange}_ws()`, `{exchange}_ws_to_native()`, `get_available_{exchange}_symbols()`
- `src/snapper/infrastructure/symbols/mapper.py` — add forward/reverse shortcut tuples + init dicts + docstring
- Add to the union in `_rebuild_available_symbols_cache()` used by `get_available_symbols()` and `get_available_symbols_set()`; preserve cache invalidation after symbol alias changes.
- Reference: existing kraken_futures/kraken_equities entries

## 5. Exchange client

- `src/snapper/infrastructure/exchanges/implementations/{exchange}.py`
- Extend `ExchangeClientBase`
- Implement: `connect`, `disconnect`, `subscribe_ticks`, `subscribe_trades`, `subscribe_instruments`
- Order methods raise `NotImplementedError` for market-data-only exchanges
- All docstrings must have Args/Returns/Raises even for stubs
- Reference: `implementations/kraken_equities.py` (SpotWSClient pattern), `implementations/kraken_futures.py` (SDK callback pattern)

## 6. Symbol updater

- `src/snapper/application/updaters/symbols/{exchange}.py`
- Extend `SymbolUpdaterService[ExchangeClient]`
- `@register_process("{exchange}_symbol_updater", enabled=False, ...)`
- Must override `__init__` with default `update_threshold_hours`
- Upsert: Symbol → SymbolAlias (WS channel) → Capability → Instrument
- Reference: `updaters/symbols/kraken_equities.py`

## 7. Publisher

- `src/snapper/messaging/publishers/{exchange}.py`
- Extend `MarketDataPublisherService[ExchangeClient]`
- Register `"{exchange}_feed_publisher"` with an explicit startup default and publisher parameters. The existing Kraken Equities publisher has `enabled=True`; the symbol updater is a separate disabled one-shot task.
- Reference: `publishers/kraken_equities.py`

## 8. Market data snapshot

- `src/snapper/infrastructure/market_data/{exchange}.py`
- Extend `MarketSnapshotUpdaterService`
- `run_{exchange}_snapshot_update()` entry point for CLI
- Reference: `market_data/kraken.py` (WS-based), `market_data/walutomat.py` (polling-based)

## 9. CLI commands

- `src/snapper/cli/app.py` — add import + two commands:
    - `update-{exchange}-symbols` (with `--force` flag)
    - `update-{exchange}-market-snapshot`

## 10. Makefile

- Add both commands to `run-static` target

## 11. Tests

- One test file per source file, mirroring directory structure under `tests/`
- 100% coverage required
- Follow existing test patterns (class-based, docstrings, no comments)
