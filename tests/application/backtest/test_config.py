"""Tests for BacktestConfig validation and fingerprinting."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.config import BacktestExecutionMode
from snapper.application.backtest.config import BacktestFillModel
from snapper.application.backtest.config import compute_fingerprint
from snapper.strategies.base import BaseStrategy

NOW = datetime(2026, 4, 13, tzinfo=UTC)
END = NOW + timedelta(days=30)

MOCK_STRATEGIES = {"sma_cross": BaseStrategy, "rsi_reversion": BaseStrategy}


def _valid_config(**overrides: object) -> dict[str, object]:
    """Build a valid BacktestConfig dict with optional overrides."""
    base: dict[str, object] = {
        "strategy_class": "sma_cross",
        "instruments": {"kraken": ["BTC-USD"]},
        "start_date": NOW,
        "end_date": END,
        "wallet_public_id": "w-1",
    }
    base.update(overrides)
    return base


class TestBacktestConfigValidation:
    """Tests for BacktestConfig field validation."""

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_valid_config(self) -> None:
        """Valid config passes validation."""
        config = BacktestConfig(**_valid_config())
        assert config.strategy_class == "sma_cross"
        assert config.initial_balance == 10_000.0
        assert config.execution_mode == BacktestExecutionMode.DIRECT_DB
        assert config.fill_model == BacktestFillModel.MARKET

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_valid_config_with_all_fields(self) -> None:
        """Config with all optional fields passes."""
        config = BacktestConfig(
            **_valid_config(
                operator_public_id="op-1",
                initial_balance=50000.0,
                strategy_params={"fast": 10, "slow": 30},
                timeframe="15m",
                slippage_bps=5.0,
                commission_bps=10.0,
            )
        )
        assert config.operator_public_id == "op-1"
        assert config.initial_balance == 50000.0

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", {})
    def test_unknown_strategy_class(self) -> None:
        """Unknown strategy_class raises ValidationError."""
        with pytest.raises(ValidationError, match="Unknown strategy class"):
            BacktestConfig(**_valid_config())

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_end_date_before_start_date(self) -> None:
        """end_date <= start_date raises ValidationError."""
        with pytest.raises(ValidationError, match="end_date must be after"):
            BacktestConfig(**_valid_config(end_date=NOW - timedelta(days=1)))

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_end_date_equals_start_date(self) -> None:
        """end_date == start_date raises ValidationError."""
        with pytest.raises(ValidationError, match="end_date must be after"):
            BacktestConfig(**_valid_config(end_date=NOW))

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_empty_instruments(self) -> None:
        """Empty instruments dict raises ValidationError."""
        with pytest.raises(ValidationError, match="at least one exchange"):
            BacktestConfig(**_valid_config(instruments={}))

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_exchange_with_empty_symbols(self) -> None:
        """Exchange with empty symbol list raises ValidationError."""
        with pytest.raises(ValidationError, match="at least one instrument"):
            BacktestConfig(**_valid_config(instruments={"kraken": []}))

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_negative_initial_balance(self) -> None:
        """Negative initial_balance raises ValidationError."""
        with pytest.raises(ValidationError, match="initial_balance must be positive"):
            BacktestConfig(**_valid_config(initial_balance=-100))

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_zero_initial_balance(self) -> None:
        """Zero initial_balance raises ValidationError."""
        with pytest.raises(ValidationError, match="initial_balance must be positive"):
            BacktestConfig(**_valid_config(initial_balance=0))

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_negative_slippage(self) -> None:
        """Negative slippage_bps raises ValidationError."""
        with pytest.raises(ValidationError, match="bps fields must be in"):
            BacktestConfig(**_valid_config(slippage_bps=-1))

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_negative_commission(self) -> None:
        """Negative commission_bps raises ValidationError."""
        with pytest.raises(ValidationError, match="bps fields must be in"):
            BacktestConfig(**_valid_config(commission_bps=-1))

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_slippage_above_cap(self) -> None:
        """slippage_bps above 500 raises ValidationError."""
        with pytest.raises(ValidationError, match="bps fields must be in"):
            BacktestConfig(**_valid_config(slippage_bps=501))

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_zmq_replay_now_accepted(self) -> None:
        """ZMQ_REPLAY mode is accepted at schema level (Phase 2a)."""
        config = BacktestConfig(**_valid_config(execution_mode=BacktestExecutionMode.ZMQ_REPLAY))
        assert config.execution_mode == BacktestExecutionMode.ZMQ_REPLAY

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_cancel_poll_ms_must_be_positive(self) -> None:
        """cancel_poll_ms <= 0 raises ValidationError."""
        with pytest.raises(ValidationError, match="cancel_poll_ms must be in"):
            BacktestConfig(**_valid_config(cancel_poll_ms=0))

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_cancel_poll_ms_upper_bound(self) -> None:
        """cancel_poll_ms above 60000 raises ValidationError."""
        with pytest.raises(ValidationError, match="cancel_poll_ms must be in"):
            BacktestConfig(**_valid_config(cancel_poll_ms=60_001))


class TestFingerprint:
    """Tests for compute_fingerprint determinism."""

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_same_config_same_fingerprint(self) -> None:
        """Same config produces identical fingerprint."""
        config = BacktestConfig(**_valid_config())
        fp1 = compute_fingerprint(config)
        fp2 = compute_fingerprint(config)
        assert fp1 == fp2

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_different_params_different_fingerprint(self) -> None:
        """Different strategy_params produce different fingerprints."""
        config1 = BacktestConfig(**_valid_config(strategy_params={"fast": 10}))
        config2 = BacktestConfig(**_valid_config(strategy_params={"fast": 20}))
        assert compute_fingerprint(config1) != compute_fingerprint(config2)

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_snapshot_affects_fingerprint(self) -> None:
        """Different snapshot_as_of produces different fingerprint."""
        config = BacktestConfig(**_valid_config())
        fp1 = compute_fingerprint(config, snapshot_as_of=NOW)
        fp2 = compute_fingerprint(config, snapshot_as_of=NOW + timedelta(hours=1))
        assert fp1 != fp2

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_fingerprint_is_hex_sha256(self) -> None:
        """Fingerprint is a 64-char hex string (SHA-256)."""
        config = BacktestConfig(**_valid_config())
        fp = compute_fingerprint(config)
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)


class TestRequiredCandleHistory:
    """Tests for BaseStrategy.required_candle_history default."""

    def test_default_returns_zero(self) -> None:
        """BaseStrategy.required_candle_history returns 0 by default."""
        mock_strategy = MagicMock(spec=BaseStrategy)
        result = BaseStrategy.required_candle_history(mock_strategy)
        assert result == 0


class TestTargetExecutionExchange:
    """Tests for BacktestConfig.target_execution_exchange (BE-1 D6 carrier)."""

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_target_execution_exchange_defaults_to_none(self) -> None:
        """Default config has target_execution_exchange=None (backward compat).

        Given: a config constructed without the field,
        When: BacktestConfig is built,
        Then: target_execution_exchange is None so fills attribute to the
            source candle's exchange (Phase 2c byte-identical path).
        """
        config = BacktestConfig(**_valid_config())
        assert config.target_execution_exchange is None

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_target_execution_exchange_accepts_order_capable_venue(self) -> None:
        """Target exchange accepts a value inside the OrderExchange Literal.

        Given: target_execution_exchange='kraken',
        When: the config is constructed,
        Then: the field carries the string value (serialized from the
            OrderExchange enum literal).
        """
        config = BacktestConfig(**_valid_config(target_execution_exchange="kraken"))
        assert config.target_execution_exchange == "kraken"

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_target_execution_exchange_rejects_unknown_value(self) -> None:
        """Unknown exchange strings are rejected by Pydantic Literal validation.

        Given: target_execution_exchange='not_a_real_exchange',
        When: BacktestConfig is built,
        Then: Pydantic raises ValidationError because 'not_a_real_exchange'
            is not a member of the OrderExchange Literal (paper/kraken/
            kraken_futures/zonda/walutomat).
        """
        with pytest.raises(ValidationError):
            BacktestConfig(**_valid_config(target_execution_exchange="not_a_real_exchange"))

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_target_execution_exchange_rejects_market_data_only_venue(self) -> None:
        """Market-data-only venues (kraken_equities) are rejected.

        Given: target_execution_exchange='kraken_equities',
        When: BacktestConfig is built,
        Then: Pydantic raises ValidationError — OrderExchange excludes
            kraken_equities (which is feed-only, not order-capable).
            Closes R3-Q1 design decision.
        """
        with pytest.raises(ValidationError):
            BacktestConfig(**_valid_config(target_execution_exchange="kraken_equities"))


class TestTargetExecutionExchangeFingerprint:
    """Fingerprint stability vs. target_execution_exchange sensitivity."""

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_fingerprint_unaffected_by_none_target_exchange(self) -> None:
        """Default-None target exchange does not change the fingerprint.

        Given: two configs — one without field (default None), one with
            target_execution_exchange=None explicitly,
        When: fingerprints are computed,
        Then: both produce the byte-identical hash, matching pre-v1.2
            legacy runs (Phase 2c dedup cache remains valid).
        """
        c1 = BacktestConfig(**_valid_config())
        c2 = BacktestConfig(**_valid_config(target_execution_exchange=None))
        assert compute_fingerprint(c1) == compute_fingerprint(c2)

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_fingerprint_changes_when_target_exchange_set(self) -> None:
        """An explicit target_execution_exchange produces a different fingerprint.

        Given: a default config vs. a cross-asset config (target='kraken'),
        When: fingerprints are computed,
        Then: the two hashes differ — cross-asset runs are not deduplicated
            against legacy single-venue runs.
        """
        c_default = BacktestConfig(**_valid_config())
        c_cross = BacktestConfig(**_valid_config(target_execution_exchange="kraken"))
        assert compute_fingerprint(c_default) != compute_fingerprint(c_cross)

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_fingerprint_pairing_unaffected_by_none_target_exchange(self) -> None:
        """Pairing-mode fingerprint ignores default-None target exchange.

        Given: two configs with None target exchange,
        When: compute_fingerprint(..., for_pairing=True) is called,
        Then: hashes match — auto-pair detection at backtest_routes.py:348
            still groups legacy runs correctly. Closes R3.7.
        """
        c1 = BacktestConfig(**_valid_config())
        c2 = BacktestConfig(**_valid_config(target_execution_exchange=None))
        assert compute_fingerprint(c1, for_pairing=True) == compute_fingerprint(
            c2, for_pairing=True
        )

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    def test_fingerprint_pairing_tracks_explicit_target_exchange(self) -> None:
        """Pairing-mode fingerprint differs when target_execution_exchange is set.

        Given: a default config vs. a cross-asset config,
        When: compute_fingerprint(..., for_pairing=True) is called on each,
        Then: hashes differ — cross-asset runs do not auto-pair against
            single-venue baselines. Closes R3.7.
        """
        c_default = BacktestConfig(**_valid_config())
        c_cross = BacktestConfig(**_valid_config(target_execution_exchange="kraken"))
        assert compute_fingerprint(c_default, for_pairing=True) != compute_fingerprint(
            c_cross, for_pairing=True
        )
