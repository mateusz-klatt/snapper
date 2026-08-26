"""Tests for the shared latest-at-minute observation fold."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.portfolio.observation_stream import fold_observation_attempts
from snapper.data.repository_types import VenueAccountObservationAttemptRow

_WALLET = "wallet-1"
_T0 = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)


def _minute(offset: int) -> datetime:
    """Return the grid minute ``offset`` minutes after the window start."""
    return _T0 + timedelta(minutes=offset)


def _row(
    identifier: int,
    timestamp: datetime,
    exchange: str = "kraken",
    wallet: str = _WALLET,
    mode: str = "live",
) -> VenueAccountObservationAttemptRow:
    """Build one observation attempt row for the fold."""
    return {
        "id": identifier,
        "public_id": f"obs-{identifier}",
        "wallet_public_id": wallet,
        "exchange": exchange,
        "mode": mode,
        "attempt_status": "observed",
        "balance_status": "observed",
        "position_status": "not_applicable",
        "balances_json": "[]",
        "open_positions_json": None,
        "balance_observed_at": timestamp,
        "position_observed_at": None,
        "error": None,
        "timestamp": timestamp,
        "session_id": "session-1",
        "sequence_id": identifier,
    }


class TestFold:
    """The cursor fold answers exactly what the per-minute query answered."""

    def test_an_opening_seed_carries_forward_until_replaced(self) -> None:
        """A seed before the window serves every minute until a newer attempt."""
        rows = [
            _row(1, _minute(-3)),
            _row(2, _minute(2) - timedelta(seconds=30)),
        ]
        maps = fold_observation_attempts(
            rows, _WALLET, "live", frozenset({"kraken"}), [_minute(0), _minute(1), _minute(2)]
        )
        assert [snapshot["kraken"]["id"] for snapshot in maps] == [1, 1, 2]

    def test_an_attempt_exactly_on_the_minute_belongs_to_it(self) -> None:
        """The advance is ``<=``: a bus timestamp on the grid minute counts."""
        rows = [_row(1, _minute(1))]
        maps = fold_observation_attempts(
            rows, _WALLET, "live", frozenset({"kraken"}), [_minute(0), _minute(1)]
        )
        assert maps[0] == {}
        assert maps[1]["kraken"]["id"] == 1

    def test_equal_timestamps_resolve_by_immutable_row_id(self) -> None:
        """Two attempts on one bus instant pick the later insert, deterministically."""
        rows = [_row(2, _minute(1)), _row(1, _minute(1))]
        maps = fold_observation_attempts(rows, _WALLET, "live", frozenset({"kraken"}), [_minute(1)])
        assert maps[0]["kraken"]["id"] == 2

    def test_exchanges_cursor_independently(self) -> None:
        """Each venue's latest attempt advances without disturbing the others."""
        rows = [
            _row(1, _minute(0), exchange="kraken"),
            _row(2, _minute(1), exchange="walutomat"),
        ]
        maps = fold_observation_attempts(
            rows, _WALLET, "live", frozenset({"kraken", "walutomat"}), [_minute(0), _minute(1)]
        )
        assert set(maps[0]) == {"kraken"}
        assert set(maps[1]) == {"kraken", "walutomat"}


class TestScopeValidation:
    """A foreign row fails loudly instead of corrupting every later map."""

    def test_a_foreign_wallet_is_refused(self) -> None:
        """A row for another wallet is a query bug, never data."""
        rows = [_row(1, _minute(0), wallet="other")]
        with pytest.raises(ValueError, match="foreign wallet"):
            fold_observation_attempts(rows, _WALLET, "live", frozenset({"kraken"}), [_minute(0)])

    def test_a_foreign_mode_is_refused(self) -> None:
        """A paper-mode row cannot leak into a live reconstruction."""
        rows = [_row(1, _minute(0), mode="paper")]
        with pytest.raises(ValueError, match="foreign mode"):
            fold_observation_attempts(rows, _WALLET, "live", frozenset({"kraken"}), [_minute(0)])

    def test_an_unexpected_exchange_is_refused(self) -> None:
        """A venue outside the requested set is refused, not silently keyed."""
        rows = [_row(1, _minute(0), exchange="binance")]
        with pytest.raises(ValueError, match="unexpected exchange"):
            fold_observation_attempts(rows, _WALLET, "live", frozenset({"kraken"}), [_minute(0)])

    def test_a_row_beyond_the_window_is_refused(self) -> None:
        """A row after the last requested minute proves the stream is unbounded."""
        rows = [_row(1, _minute(5))]
        with pytest.raises(ValueError, match="beyond the requested window"):
            fold_observation_attempts(rows, _WALLET, "live", frozenset({"kraken"}), [_minute(0)])
