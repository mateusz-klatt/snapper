"""Tests for the ``capture_phase2c_baseline`` script."""

import asyncio
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.capture_phase2c_baseline import SCENARIOS
from scripts.capture_phase2c_baseline import _candle_row
from scripts.capture_phase2c_baseline import _make_config
from scripts.capture_phase2c_baseline import _make_strategy
from scripts.capture_phase2c_baseline import _run_all_scenarios
from scripts.capture_phase2c_baseline import _serialise_equity
from scripts.capture_phase2c_baseline import _serialise_portfolio
from scripts.capture_phase2c_baseline import _serialise_signal
from scripts.capture_phase2c_baseline import _serialise_trade
from scripts.capture_phase2c_baseline import main
from scripts.capture_phase2c_baseline import serialise_run
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker
from snapper.strategies.models import StrategySignal


class TestHelpers:
    """Deterministic helpers (candle, config, strategy stubs)."""

    def test_candle_row_fields_are_deterministic(self) -> None:
        """Verify deterministic candle row construction.

        Given: a fixed open_at / close / seq,
        When: _candle_row is invoked,
        Then: all OHLCV fields equal close and public_id embeds seq.
        """
        row = _candle_row(open_at=SCENARIOS[0].run.__globals__["_BASE_NOW"], close=42.0, seq=7)
        assert row["public_id"] == "candle-7"
        assert row["open"] == row["high"] == row["low"] == row["close"] == 42.0
        assert row["volume"] == 1.0

    def test_make_config_defaults_target_execution_exchange_to_none(self) -> None:
        """Verify make_config sets the carrier attribute to None by default.

        Given: no target_execution_exchange argument,
        When: _make_config is invoked,
        Then: the spec'd MagicMock carries None explicitly.
        """
        config = _make_config(start_date=SCENARIOS[0].run.__globals__["_BASE_NOW"])
        assert config.target_execution_exchange is None
        assert config.slippage_bps == 0.0
        assert config.commission_bps == 0.0

    def test_make_config_accepts_explicit_target_exchange(self) -> None:
        """Verify make_config passes through explicit target_execution_exchange.

        Given: target_execution_exchange='kraken',
        When: _make_config is invoked,
        Then: the attribute is set on the mock.
        """
        config = _make_config(
            start_date=SCENARIOS[0].run.__globals__["_BASE_NOW"],
            target_execution_exchange="kraken",
        )
        assert config.target_execution_exchange == "kraken"

    def test_make_strategy_stub_returns_configured_signal(self) -> None:
        """Verify strategy stub returns the signal passed at construction.

        Given: a StrategySignal,
        When: the stub's _handle_candle_data is awaited,
        Then: the configured signal is returned.
        """
        sig = StrategySignal(instrument="BTC-USD", side="buy", strength=1.0, reason="t", price=1.0)
        strategy = _make_strategy(sig)
        result = asyncio.run(strategy._handle_candle_data("BTC-USD", "{}"))
        assert result is sig

    def test_make_strategy_stub_returns_none_when_no_signal(self) -> None:
        """Verify strategy stub returns None when constructed with None.

        Given: None signal,
        When: the stub's _handle_candle_data is awaited,
        Then: None is returned.
        """
        strategy = _make_strategy(None)
        assert asyncio.run(strategy._handle_candle_data("BTC-USD", "{}")) is None


class TestSerialisers:
    """Domain-value preservation + nondeterministic-field stripping."""

    def test_serialise_signal_strips_identifiers(self) -> None:
        """Verify uuid7 / session / sequence / timestamp are stripped.

        Given: a BacktestSignalInsertRow-shaped dict with all fields,
        When: _serialise_signal is invoked,
        Then: only signal_type, instrument, price, indicators remain.
        """
        row = {
            "run_public_id": "run-1",
            "public_id": "sig-1",
            "signal_time": "ignore",
            "signal_type": "buy",
            "instrument": "BTC-USD",
            "price": 100.0,
            "indicators": {"rsi": 30.0},
            "session_id": "drop",
            "sequence_id": 5,
            "timestamp": "drop",
        }
        out = _serialise_signal(row)
        assert out == {
            "signal_type": "buy",
            "instrument": "BTC-USD",
            "price": 100.0,
            "indicators": {"rsi": 30.0},
        }

    def test_serialise_trade_strips_identifiers(self) -> None:
        """Verify trade serialiser keeps only domain values.

        Given: a BacktestTradeInsertRow-shaped dict,
        When: _serialise_trade is invoked,
        Then: identifiers + timestamps are stripped; domain values remain.
        """
        row = {
            "run_public_id": "run-1",
            "executed_at": "drop",
            "instrument": "BTC-USD",
            "side": "buy",
            "quantity": 1.0,
            "price": 100.0,
            "fee": 0.1,
            "pnl": None,
            "position_after": 1.0,
            "signal_public_id": "drop",
            "session_id": "drop",
            "sequence_id": 7,
            "timestamp": "drop",
        }
        assert _serialise_trade(row) == {
            "instrument": "BTC-USD",
            "side": "buy",
            "quantity": 1.0,
            "price": 100.0,
            "fee": 0.1,
            "pnl": None,
            "position_after": 1.0,
        }

    def test_serialise_equity_strips_identifiers(self) -> None:
        """Verify equity serialiser keeps only domain values.

        Given: a BacktestEquityPointInsertRow-shaped dict,
        When: _serialise_equity is invoked,
        Then: run_public_id / point_time / session_id / sequence_id / timestamp are stripped.
        """
        row = {
            "run_public_id": "run-1",
            "point_time": "drop",
            "equity": 100.0,
            "cash": 50.0,
            "position_value": 50.0,
            "drawdown": 0.0,
            "session_id": "drop",
            "sequence_id": 9,
            "timestamp": "drop",
        }
        assert _serialise_equity(row) == {
            "equity": 100.0,
            "cash": 50.0,
            "position_value": 50.0,
            "drawdown": 0.0,
        }

    def test_serialise_portfolio_sorts_positions(self) -> None:
        """Verify positions dict is serialised in sorted-key order.

        Given: a portfolio with two positions inserted out of order,
        When: _serialise_portfolio is invoked,
        Then: the positions dict iterates alphabetically on the instrument key.
        """
        portfolio = PortfolioTracker(cash=9000.0)
        portfolio.turnover = 1234.0
        portfolio.update_fill("ZEC-USD", "buy", 1.0, 10.0, 0.0)
        portfolio.update_fill("AAA-USD", "buy", 2.0, 5.0, 0.0)
        out = _serialise_portfolio(portfolio)
        assert list(out["positions"].keys()) == ["AAA-USD", "ZEC-USD"]
        assert out["cash"] == pytest.approx(portfolio.cash)
        assert out["turnover"] == pytest.approx(portfolio.turnover)

    def test_serialise_run_combines_collector_and_portfolio(self) -> None:
        """Verify serialise_run builds the combined-run dict.

        Given: an empty collector + fresh portfolio,
        When: serialise_run is invoked,
        Then: every expected top-level key is present with the right shape.
        """
        out = serialise_run(ResultCollector(), PortfolioTracker(cash=10.0))
        assert set(out.keys()) == {"signals", "trades", "equity_points", "portfolio"}
        assert out["signals"] == []
        assert out["trades"] == []
        assert out["equity_points"] == []
        assert out["portfolio"]["cash"] == 10.0


class TestScenarioRegistry:
    """Top-level scenario + main() driver."""

    def test_scenarios_cover_all_four_paths(self) -> None:
        """Verify SCENARIOS carries the 4 named baseline scenarios.

        Given: the module-level SCENARIOS registry,
        When: iterated,
        Then: names match the documented happy-path / no-signal / warmup /
            skipped-fill coverage matrix.
        """
        names = [s.name for s in SCENARIOS]
        assert names == [
            "single_feed_buy_filled",
            "multi_timestamp_no_signals",
            "warmup_gated_signal_dropped",
            "signal_emitted_fill_skipped",
        ]

    def test_run_all_scenarios_yields_expected_cardinalities(self) -> None:
        """Verify the aggregated scenario dict shape + artifact counts.

        Given: the 4 registered scenarios,
        When: _run_all_scenarios is awaited,
        Then: each scenario name yields a dict with the expected artifact
            counts per the serialiser contract.
        """
        out = asyncio.run(_run_all_scenarios())
        assert set(out.keys()) == {s.name for s in SCENARIOS}
        assert len(out["single_feed_buy_filled"]["signals"]) == 1
        assert len(out["single_feed_buy_filled"]["trades"]) == 1
        assert len(out["single_feed_buy_filled"]["equity_points"]) == 1
        assert out["multi_timestamp_no_signals"]["signals"] == []
        assert out["multi_timestamp_no_signals"]["trades"] == []
        assert len(out["multi_timestamp_no_signals"]["equity_points"]) == 2
        assert out["warmup_gated_signal_dropped"] == {
            "signals": [],
            "trades": [],
            "equity_points": [],
            "portfolio": {"cash": 10_000.0, "positions": {}, "turnover": 0.0},
        }
        assert len(out["signal_emitted_fill_skipped"]["signals"]) == 1
        assert out["signal_emitted_fill_skipped"]["trades"] == []

    def test_main_writes_snapshot_to_fixture_path(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify main() writes deterministic JSON to _FIXTURE_PATH.

        Given: a redirected _FIXTURE_PATH in a tmpdir,
        When: main() is invoked,
        Then: the target file exists, is sorted-key JSON, and carries all
            4 scenario keys.
        """
        target = tmp_path / "snap.json"
        with patch("scripts.capture_phase2c_baseline._FIXTURE_PATH", target):
            main()
        payload = json.loads(target.read_text(encoding="utf-8"))
        assert set(payload.keys()) == {s.name for s in SCENARIOS}
        raw = target.read_text(encoding="utf-8")
        assert raw.endswith("\n")
        captured = capsys.readouterr()
        assert "wrote" in captured.out
        assert str(target) in captured.out

    def test_main_creates_parent_directory_if_missing(self, tmp_path: Path) -> None:
        """Verify main() mkdir-parents on a nested missing fixture path.

        Given: a _FIXTURE_PATH whose parent directory does not exist,
        When: main() is invoked,
        Then: the directory is created and the snapshot is written.
        """
        target = tmp_path / "nested" / "deeper" / "snap.json"
        assert not target.parent.exists()
        with patch("scripts.capture_phase2c_baseline._FIXTURE_PATH", target):
            main()
        assert target.exists()
