"""Unit tests for timebar utilities and autoload functionality."""

import importlib
import importlib.util
import types
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from importlib.abc import Loader
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec
from typing import Any
from typing import cast

import pytest

from snapper.utils import autoload
from snapper.utils.timebars import floor_ts
from snapper.utils.timebars import ohlc_from_trades


def test_floor_ts_minutes_and_hours() -> None:
    """Test floor_ts with various minute and hour timeframes.

    Given: timestamp 2024-01-01 12:07:33,
    When: flooring to 1m, 5m, 15m, 1h,
    Then: returns correctly floored timestamps.
    """
    ts = datetime(2024, 1, 1, 12, 7, 33, 123456)
    assert floor_ts(ts, "1m") == datetime(2024, 1, 1, 12, 7)
    assert floor_ts(ts, "5m") == datetime(2024, 1, 1, 12, 5)
    assert floor_ts(ts, "15m") == datetime(2024, 1, 1, 12, 0)
    assert floor_ts(ts, "1h") == datetime(2024, 1, 1, 12, 0)
    assert floor_ts(ts.replace(hour=23, minute=59), "1h") == datetime(2024, 1, 1, 23, 0)


def test_ohlc_from_trades_basic() -> None:
    """Test ohlc_from_trades with basic trade data.

    Given: trades spanning two 1m candles,
    When: calling ohlc_from_trades with 1m timeframe,
    Then: returns 2 candles with correct OHLCV values.
    """
    base = datetime(2024, 1, 1, 12, 0, 0)
    trades = [
        {"ts": base, "price": 100.0, "size": 1.0},
        {"ts": base.replace(second=10), "price": 101.0, "size": 2.0},
        {"ts": base.replace(second=50), "price": 99.0, "size": 1.5},
        {"ts": base.replace(minute=1), "price": 102.0, "size": 0.5},
    ]
    candles = ohlc_from_trades(trades, "1m")
    assert len(candles) == 2
    c0 = candles[0]
    assert c0["open"] == pytest.approx(100.0)
    assert c0["high"] == pytest.approx(101.0)
    assert c0["low"] == pytest.approx(99.0)
    assert c0["close"] == pytest.approx(99.0)
    assert c0["volume"] == pytest.approx(4.5)
    c1 = candles[1]
    assert c1["open"] == pytest.approx(102.0)
    assert c1["close"] == pytest.approx(102.0)


def test_floor_ts_invalid_timeframe() -> None:
    """Test floor_ts raises ValueError for invalid timeframe.

    Given: valid timestamp,
    When: calling floor_ts with 'invalid' timeframe,
    Then: raises ValueError with message.
    """
    ts = datetime(2024, 1, 1, 12, 7, 33)
    with pytest.raises(ValueError, match="Unsupported timeframe: invalid"):
        floor_ts(ts, "invalid")


def test_ohlc_from_trades_empty() -> None:
    """Test ohlc_from_trades with empty trade list.

    Given: empty list of trades,
    When: calling ohlc_from_trades,
    Then: returns empty list.
    """
    candles = ohlc_from_trades([], "1m")
    assert candles == []


def test_ohlc_from_trades_multiple_timeframes() -> None:
    """Test ohlc_from_trades with 5m timeframe.

    Given: trades at 0m, 3m, 7m,
    When: calling ohlc_from_trades with 5m,
    Then: returns 2 candles with correct aggregation.
    """
    base = datetime(2024, 1, 1, 12, 0, 0)
    trades = [
        {"ts": base, "price": 100.0, "size": 1.0},
        {"ts": base.replace(minute=3), "price": 110.0, "size": 2.0},
        {"ts": base.replace(minute=7), "price": 105.0, "size": 1.5},
    ]
    candles = ohlc_from_trades(trades, "5m")
    assert len(candles) == 2
    c0 = candles[0]
    assert c0["ts"] == datetime(2024, 1, 1, 12, 0, 0)
    assert c0["open"] == pytest.approx(100.0)
    assert c0["high"] == pytest.approx(110.0)
    assert c0["low"] == pytest.approx(100.0)
    assert c0["close"] == pytest.approx(110.0)
    assert c0["volume"] == pytest.approx(3.0)
    c1 = candles[1]
    assert c1["ts"] == datetime(2024, 1, 1, 12, 5, 0)
    assert c1["open"] == pytest.approx(105.0)
    assert c1["close"] == pytest.approx(105.0)


class TestTimeBarUtilities:
    """Test suite for time bar utility functions."""

    def test_floor_ts_minutes(self) -> None:
        """Test floor_ts with minute timeframes.

        Given: UTC timestamp with microseconds,
        When: flooring to 1m, 5m, 15m,
        Then: returns timestamps floored to boundary.
        """
        dt = datetime(2023, 1, 1, 12, 34, 56, 789000, tzinfo=UTC)
        result_1m = floor_ts(dt, "1m")
        expected_1m = datetime(2023, 1, 1, 12, 34, 0, 0, tzinfo=UTC)
        assert result_1m == expected_1m
        result_5m = floor_ts(dt, "5m")
        expected_5m = datetime(2023, 1, 1, 12, 30, 0, 0, tzinfo=UTC)
        assert result_5m == expected_5m
        result_15m = floor_ts(dt, "15m")
        expected_15m = datetime(2023, 1, 1, 12, 30, 0, 0, tzinfo=UTC)
        assert result_15m == expected_15m

    def test_floor_ts_hours(self) -> None:
        """Test floor_ts with hour timeframes.

        Given: UTC timestamp,
        When: flooring to 1h, 4h,
        Then: returns timestamps floored to hour boundary.
        """
        dt = datetime(2023, 1, 1, 12, 34, 56, 789000, tzinfo=UTC)
        result_1h = floor_ts(dt, "1h")
        expected_1h = datetime(2023, 1, 1, 12, 0, 0, 0, tzinfo=UTC)
        assert result_1h == expected_1h
        result_4h = floor_ts(dt, "4h")
        expected_4h = datetime(2023, 1, 1, 12, 0, 0, 0, tzinfo=UTC)
        assert result_4h == expected_4h

    def test_floor_ts_edge_cases(self) -> None:
        """Test floor_ts with timestamps on exact boundaries.

        Given: timestamps exactly on 5m and 1h boundaries,
        When: flooring,
        Then: returns unchanged timestamps.
        """
        dt_exact = datetime(2023, 1, 1, 12, 30, 0, 0, tzinfo=UTC)
        result = floor_ts(dt_exact, "5m")
        assert result == dt_exact
        dt_hour = datetime(2023, 1, 1, 12, 0, 0, 0, tzinfo=UTC)
        result = floor_ts(dt_hour, "1h")
        assert result == dt_hour

    def test_floor_ts_invalid_timeframe(self) -> None:
        """Test floor_ts raises ValueError for invalid/unsupported.

        Given: valid timestamp,
        When: flooring with 'invalid' or '30s',
        Then: raises ValueError.
        """
        dt = datetime(2023, 1, 1, 12, 34, 56, tzinfo=UTC)
        with pytest.raises(ValueError):
            floor_ts(dt, "invalid")
        with pytest.raises(ValueError):
            floor_ts(dt, "30s")

    def test_ohlc_from_trades_basic(self) -> None:
        """Test ohlc_from_trades basic aggregation.

        Given: 4 trades within same minute,
        When: calling ohlc_from_trades with 1m,
        Then: returns 1 candle with correct OHLCV.
        """
        trades = [
            {
                "ts": datetime(2023, 1, 1, 12, 0, 10, tzinfo=UTC),
                "price": "100.0",
                "size": "1.0",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 0, 20, tzinfo=UTC),
                "price": "101.0",
                "size": "0.5",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 0, 30, tzinfo=UTC),
                "price": "99.0",
                "size": "2.0",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 0, 40, tzinfo=UTC),
                "price": "102.0",
                "size": "1.5",
            },
        ]
        candles = ohlc_from_trades(trades, "1m")
        assert len(candles) == 1
        candle = candles[0]
        assert candle["ts"] == datetime(2023, 1, 1, 12, 0, 0, 0, tzinfo=UTC)
        assert candle["timeframe"] == "1m"
        assert candle["open"] == pytest.approx(100.0)
        assert candle["high"] == pytest.approx(102.0)
        assert candle["low"] == pytest.approx(99.0)
        assert candle["close"] == pytest.approx(102.0)
        assert candle["volume"] == pytest.approx(5.0)

    def test_ohlc_from_trades_multiple_buckets(self) -> None:
        """Test ohlc_from_trades with trades in multiple buckets.

        Given: trades at 12:00, 12:01, 12:02,
        When: calling ohlc_from_trades with 1m,
        Then: returns 3 candles, one per minute.
        """
        trades = [
            {
                "ts": datetime(2023, 1, 1, 12, 0, 10, tzinfo=UTC),
                "price": "100.0",
                "size": "1.0",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 1, 20, tzinfo=UTC),
                "price": "101.0",
                "size": "0.5",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 2, 30, tzinfo=UTC),
                "price": "99.0",
                "size": "2.0",
            },
        ]
        candles = ohlc_from_trades(trades, "1m")
        assert len(candles) == 3
        assert candles[0]["ts"] == datetime(2023, 1, 1, 12, 0, 0, 0, tzinfo=UTC)
        assert candles[0]["open"] == pytest.approx(100.0)
        assert candles[0]["close"] == pytest.approx(100.0)
        assert candles[0]["volume"] == pytest.approx(1.0)
        assert candles[1]["ts"] == datetime(2023, 1, 1, 12, 1, 0, 0, tzinfo=UTC)
        assert candles[1]["open"] == pytest.approx(101.0)
        assert candles[1]["close"] == pytest.approx(101.0)
        assert candles[1]["volume"] == pytest.approx(0.5)
        assert candles[2]["ts"] == datetime(2023, 1, 1, 12, 2, 0, 0, tzinfo=UTC)
        assert candles[2]["open"] == pytest.approx(99.0)
        assert candles[2]["close"] == pytest.approx(99.0)
        assert candles[2]["volume"] == pytest.approx(2.0)

    def test_ohlc_from_trades_empty(self) -> None:
        """Test ohlc_from_trades returns empty for empty input.

        Given: empty trade list,
        When: calling ohlc_from_trades,
        Then: returns empty list.
        """
        candles = ohlc_from_trades([], "1m")
        assert candles == []

    def test_ohlc_from_trades_single_trade(self) -> None:
        """Test ohlc_from_trades with single trade.

        Given: single trade,
        When: calling ohlc_from_trades,
        Then: returns 1 candle with OHLC all equal to price.
        """
        trades = [
            {
                "ts": datetime(2023, 1, 1, 12, 0, 10, tzinfo=UTC),
                "price": "100.0",
                "size": "1.0",
            }
        ]
        candles = ohlc_from_trades(trades, "1m")
        assert len(candles) == 1
        candle = candles[0]
        assert candle["open"] == pytest.approx(100.0)
        assert candle["high"] == pytest.approx(100.0)
        assert candle["low"] == pytest.approx(100.0)
        assert candle["close"] == pytest.approx(100.0)
        assert candle["volume"] == pytest.approx(1.0)

    def test_ohlc_from_trades_hour_timeframe(self) -> None:
        """Test ohlc_from_trades with 1h timeframe.

        Given: trades at 12:00, 12:30, 13:15,
        When: calling ohlc_from_trades with 1h,
        Then: returns 2 candles aggregated by hour.
        """
        trades = [
            {
                "ts": datetime(2023, 1, 1, 12, 0, 10, tzinfo=UTC),
                "price": "100.0",
                "size": "1.0",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 30, 20, tzinfo=UTC),
                "price": "101.0",
                "size": "0.5",
            },
            {
                "ts": datetime(2023, 1, 1, 13, 15, 30, tzinfo=UTC),
                "price": "99.0",
                "size": "2.0",
            },
        ]
        candles = ohlc_from_trades(trades, "1h")
        assert len(candles) == 2
        assert candles[0]["ts"] == datetime(2023, 1, 1, 12, 0, 0, 0, tzinfo=UTC)
        assert candles[0]["volume"] == pytest.approx(1.5)
        assert candles[1]["ts"] == datetime(2023, 1, 1, 13, 0, 0, 0, tzinfo=UTC)
        assert candles[1]["volume"] == pytest.approx(2.0)

    def test_ohlc_from_trades_precision(self) -> None:
        """Test ohlc_from_trades preserves decimal precision.

        Given: trades with high precision prices,
        When: calling ohlc_from_trades,
        Then: OHLC values preserve full precision.
        """
        trades = [
            {
                "ts": datetime(2023, 1, 1, 12, 0, 10, tzinfo=UTC),
                "price": "100.123456",
                "size": "1.123456",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 0, 20, tzinfo=UTC),
                "price": "101.987654",
                "size": "0.567890",
            },
        ]
        candles = ohlc_from_trades(trades, "1m")
        assert len(candles) == 1
        candle = candles[0]
        assert candle["open"] == pytest.approx(100.123456)
        assert candle["high"] == pytest.approx(101.987654)
        assert candle["low"] == pytest.approx(100.123456)
        assert candle["close"] == pytest.approx(101.987654)

    def test_ohlc_from_trades_large_values(self) -> None:
        """Test ohlc_from_trades handles large values.

        Given: trades with large prices and volumes,
        When: calling ohlc_from_trades,
        Then: returns candle with correct large values.
        """
        trades = [
            {
                "ts": datetime(2023, 1, 1, 12, 0, 10, tzinfo=UTC),
                "price": "50000.0",
                "size": "100.0",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 0, 20, tzinfo=UTC),
                "price": "51000.0",
                "size": "200.0",
            },
        ]
        candles = ohlc_from_trades(trades, "1m")
        assert len(candles) == 1
        candle = candles[0]
        assert candle["open"] == pytest.approx(50000.0)
        assert candle["high"] == pytest.approx(51000.0)
        assert candle["volume"] == pytest.approx(300.0)

    def test_ohlc_from_trades_chronological_order(self) -> None:
        """Test ohlc_from_trades with out-of-order trades.

        Given: trades not in chronological order,
        When: calling ohlc_from_trades,
        Then: open/close based on insertion order, high/low correct.
        """
        trades = [
            {
                "ts": datetime(2023, 1, 1, 12, 0, 30, tzinfo=UTC),
                "price": "102.0",
                "size": "1.0",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 0, 10, tzinfo=UTC),
                "price": "100.0",
                "size": "1.0",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 0, 20, tzinfo=UTC),
                "price": "101.0",
                "size": "1.0",
            },
        ]
        candles = ohlc_from_trades(trades, "1m")
        assert len(candles) == 1
        candle = candles[0]
        assert candle["open"] == pytest.approx(102.0)
        assert candle["close"] == pytest.approx(101.0)
        assert candle["high"] == pytest.approx(102.0)
        assert candle["low"] == pytest.approx(100.0)

    def test_ohlc_from_trades_different_timeframes(self) -> None:
        """Test ohlc_from_trades with 1m vs 5m comparison.

        Given: trades at 0m, 2m, 7m,
        When: calling with 1m and 5m,
        Then: 1m returns 3 candles, 5m returns 2 candles.
        """
        trades = [
            {
                "ts": datetime(2023, 1, 1, 12, 0, 10, tzinfo=UTC),
                "price": "100.0",
                "size": "1.0",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 2, 20, tzinfo=UTC),
                "price": "101.0",
                "size": "1.0",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 7, 30, tzinfo=UTC),
                "price": "99.0",
                "size": "1.0",
            },
        ]
        candles_1m = ohlc_from_trades(trades, "1m")
        assert len(candles_1m) == 3
        candles_5m = ohlc_from_trades(trades, "5m")
        assert len(candles_5m) == 2

    def test_ohlc_from_trades_edge_timestamps(self) -> None:
        """Test ohlc_from_trades with exact boundary timestamps.

        Given: trades exactly at 12:00:00 and 12:01:00,
        When: calling ohlc_from_trades with 1m,
        Then: returns 2 candles at those boundaries.
        """
        trades = [
            {
                "ts": datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC),
                "price": "100.0",
                "size": "1.0",
            },
            {
                "ts": datetime(2023, 1, 1, 12, 1, 0, tzinfo=UTC),
                "price": "101.0",
                "size": "1.0",
            },
        ]
        candles = ohlc_from_trades(trades, "1m")
        assert len(candles) == 2
        assert candles[0]["ts"] == datetime(2023, 1, 1, 12, 0, 0, 0, tzinfo=UTC)
        assert candles[1]["ts"] == datetime(2023, 1, 1, 12, 1, 0, 0, tzinfo=UTC)


class DummyModule(types.ModuleType):
    """Mock module for testing autoload functionality."""

    def __init__(self, name: str) -> None:
        """Initialize the instance."""
        super().__init__(name)
        self.loaded = False

    def mark_loaded(self) -> None:
        """Mark this module as loaded."""
        self.loaded = True


class DummyLoader(Loader):
    """Mock loader for testing module imports."""

    def __init__(self, module: DummyModule) -> None:
        """Initialize the instance."""
        self.module = module

    def create_module(self, spec: ModuleSpec) -> types.ModuleType | None:
        """Create the module instance."""
        return self.module

    def exec_module(self, module: types.ModuleType) -> None:
        """Execute the module and mark it as loaded."""
        if isinstance(module, DummyModule):
            module.mark_loaded()


class DummyFinder(MetaPathFinder):
    """Mock meta path finder for testing module discovery."""

    def __init__(self, root: str, modules: dict[str, DummyModule]) -> None:
        """Initialize the instance."""
        self.root = root
        self.modules = modules

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: Any | None = None,
    ) -> ModuleSpec | None:
        """Find module spec for the given module name."""
        if fullname not in self.modules:
            return None
        spec = ModuleSpec(fullname, DummyLoader(self.modules[fullname]))
        if fullname == self.root:
            spec.submodule_search_locations = ["<virtual>"]
        return spec


@pytest.fixture()
def temp_modules(monkeypatch: pytest.MonkeyPatch) -> dict[str, DummyModule]:
    """Provide mocked module entries for autoload testing."""
    base = "autoload_pkg"
    entries: dict[str, DummyModule] = {
        base: DummyModule(base),
        f"{base}.keep": DummyModule(f"{base}.keep"),
        f"{base}.exclude": DummyModule(f"{base}.exclude"),
        f"{base}.skip.tests": DummyModule(f"{base}.skip.tests"),
    }
    finder = DummyFinder(base, entries)

    def fake_import(name: str) -> DummyModule:
        module = entries[name]
        module.mark_loaded()
        return module

    monkeypatch.setattr(importlib, "import_module", fake_import)
    find_spec_callable = cast(
        Callable[[str, Sequence[str] | None, Any | None], ModuleSpec | None],
        finder.find_spec,
    )
    monkeypatch.setattr(importlib.util, "find_spec", find_spec_callable)

    def fake_walk_packages(
        search: Sequence[str] | None, prefix: str = ""
    ) -> Iterator[tuple[None, str, bool]]:
        yield (None, f"{base}.keep", False)
        yield (None, f"{base}.exclude", False)
        yield (None, f"{base}.skip.tests", False)

    monkeypatch.setattr(importlib, "invalidate_caches", lambda: None)
    monkeypatch.setattr("snapper.utils.autoload.pkgutil.walk_packages", fake_walk_packages)
    return entries


def test_walk_module_names_respects_exclusions(temp_modules: dict[str, DummyModule]) -> None:
    """Test _walk_module_names respects exclusion patterns.

    Given: module hierarchy with 'keep', 'exclude', 'skip' modules,
    When: walking with exclusions ('exclude', 'skip'),
    Then: only 'keep' module returned.
    """
    walk_modules = cast(
        Callable[[str, Sequence[str]], list[str]],
        autoload.__dict__["_walk_module_names"],
    )
    modules = walk_modules("autoload_pkg", ("exclude", "skip"))
    assert "autoload_pkg.keep" in modules
    assert "autoload_pkg.exclude" not in modules
    assert "autoload_pkg.skip.tests" not in modules


def test_import_all_under_warns_on_error(
    monkeypatch: pytest.MonkeyPatch, temp_modules: dict[str, DummyModule]
) -> None:
    """Test import_all_under warns on import error.

    Given: fake_import that raises RuntimeError for 'exclude',
    When: calling import_all_under with on_error='warn',
    Then: prints warning and continues importing others.
    """
    import_calls: list[str] = []

    def fake_import(name: str) -> DummyModule:
        import_calls.append(name)
        if name.endswith("exclude"):
            raise RuntimeError("boom")
        module = temp_modules[name]
        module.mark_loaded()
        return module

    monkeypatch.setattr(importlib, "import_module", fake_import)
    printed: list[str] = []

    def fake_print(msg: str) -> None:
        printed.append(msg)

    monkeypatch.setattr("builtins.print", fake_print)
    imported_count = autoload.import_all_under("autoload_pkg", exclude_parts=(), on_error="warn")
    assert imported_count == 2
    assert any("Failed to import autoload_pkg.exclude" in entry for entry in printed)


def test_import_all_under_returns_zero_for_missing_package() -> None:
    """Test import_all_under returns 0 for missing package.

    Given: nonexistent package name,
    When: calling import_all_under,
    Then: returns 0 imports.
    """
    missing_root = "autoload_nonexistent_package_for_test"
    imported_count = autoload.import_all_under(missing_root)
    assert imported_count == 0


def test_import_all_under_imports_modules(temp_modules: dict[str, DummyModule]) -> None:
    """Test import_all_under imports all modules.

    Given: temp_modules fixture with 'keep' and 'exclude',
    When: calling import_all_under without exclusions,
    Then: both modules loaded.
    """
    imported_count = autoload.import_all_under("autoload_pkg")
    assert imported_count == 2
    assert temp_modules["autoload_pkg.keep"].loaded is True
    assert temp_modules["autoload_pkg.exclude"].loaded is True


def test_import_all_under_ignore_errors(
    monkeypatch: pytest.MonkeyPatch, temp_modules: dict[str, DummyModule]
) -> None:
    """Test import_all_under with on_error='ignore'.

    Given: fake_import that raises for 'exclude',
    When: calling import_all_under with on_error='ignore',
    Then: no warning printed, continues silently.
    """
    import_calls: list[str] = []

    def fake_import(name: str) -> DummyModule:
        import_calls.append(name)
        if name.endswith("exclude"):
            raise RuntimeError("boom")
        module = temp_modules[name]
        module.mark_loaded()
        return module

    monkeypatch.setattr(importlib, "import_module", fake_import)
    printed: list[str] = []

    def fake_print(msg: str) -> None:
        printed.append(msg)

    monkeypatch.setattr("builtins.print", fake_print)
    imported_count = autoload.import_all_under("autoload_pkg", exclude_parts=(), on_error="ignore")
    assert imported_count == 2
    assert len(printed) == 0


def test_import_all_under_raise_errors(
    monkeypatch: pytest.MonkeyPatch, temp_modules: dict[str, DummyModule]
) -> None:
    """Test import_all_under with on_error='raise'.

    Given: fake_import that raises for 'exclude',
    When: calling import_all_under with on_error='raise',
    Then: RuntimeError propagated.
    """
    import_calls: list[str] = []

    def fake_import(name: str) -> DummyModule:
        import_calls.append(name)
        if name.endswith("exclude"):
            raise RuntimeError("boom")
        module = temp_modules[name]
        module.mark_loaded()
        return module

    monkeypatch.setattr(importlib, "import_module", fake_import)
    with pytest.raises(RuntimeError, match="boom"):
        autoload.import_all_under("autoload_pkg", exclude_parts=(), on_error="raise")


def test_walk_module_names_returns_empty_for_missing_package() -> None:
    """Test _walk_module_names returns empty for missing package.

    Given: nonexistent package name,
    When: calling _walk_module_names,
    Then: returns empty list.
    """
    walk_modules = cast(
        Callable[[str, Sequence[str]], list[str]],
        autoload.__dict__["_walk_module_names"],
    )
    modules = walk_modules("nonexistent_package_xyz", ())
    assert modules == []
