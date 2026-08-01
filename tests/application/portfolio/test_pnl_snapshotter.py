"""Tests for the Phase-5B portfolio equity/drawdown snapshotter service."""

import asyncio
import json
import unittest.mock as mock
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import cast
from uuid import UUID

import pytest

from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.application.portfolio.pnl_snapshotter import _extract_reason_codes
from snapper.application.portfolio.pnl_snapshotter import _observed_currencies
from snapper.application.portfolio.pnl_snapshotter import _parse_baseline
from snapper.application.portfolio.pnl_snapshotter import _sample_values_equal
from snapper.application.portfolio.pnl_snapshotter import _watermarks_json
from snapper.application.portfolio.pnl_snapshotter import is_futures_class_exchange
from snapper.application.portfolio.pnl_snapshotter import resolve_spot_venues
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReasonEntry
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayMetadata
from snapper.application.portfolio.pnl_timeline_service import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.process_manager.executor_topology import MINT_WALLET_PIN_SETTING_KEY
from snapper.application.process_manager.executor_topology import ExecutorTopology
from snapper.application.services.settings import SettingsService
from snapper.core.json_types import JsonValue
from snapper.data.repository import PortfolioPnlSampleConflictError
from snapper.data.repository import Repository
from snapper.data.repository_types import PortfolioPnlAnchorRow
from snapper.data.repository_types import PortfolioPnlSampleRow
from snapper.data.repository_types import ScopeGrantRow
from snapper.data.repository_types import VenueAccountObservationAttemptRow
from snapper.data.repository_types import WalletCredentialRow
from snapper.data.repository_types import WalletRow

_T0 = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_WALLET = "0000face-0000-7000-8000-0000000000a1"
_MINT_WALLET = "ffffface-0000-7000-8000-0000000000b2"
_EPOCH = "00000000-0000-7000-8000-000000000102"
_SESSION = "00000000-0000-7000-8000-000000000103"
_UNRESTRICTED_TOPOLOGY = ExecutorTopology("", frozenset())


def _minute(offset: int) -> datetime:
    """Return the grid minute ``offset`` minutes after ``t0``."""
    return _T0 + timedelta(minutes=offset)


def _minute_with_seconds(offset: int, seconds: int) -> datetime:
    """Return a minute offset plus a sub-minute seconds component."""
    return _T0 + timedelta(minutes=offset, seconds=seconds)


def _clock(now: datetime) -> Any:
    """Return a fixed-clock callable."""
    return lambda: now


def _fixed_uuid7() -> UUID:
    """Return one deterministic UUID for byte-identical sample comparisons."""
    return UUID("00000000-0000-7000-8000-000000000777")


def _cred(
    exchange: str,
    *,
    credential_type: str = "api_key_secret",
    wallet: str = _WALLET,
) -> WalletCredentialRow:
    """Build one active wallet credential row."""
    return {
        "public_id": f"cred-{wallet}-{exchange}",
        "wallet_public_id": wallet,
        "exchange": exchange,
        "credential_type": credential_type,
        "encrypted_payload": "x",
        "label": None,
        "timestamp": _T0,
        "session_id": _SESSION,
        "sequence_id": 1,
    }


def _wallet_row(wallet: str, label: str, *, is_paper: bool = False) -> WalletRow:
    """Build one active wallet-catalogue row for topology resolution."""
    return {
        "public_id": wallet,
        "label": label,
        "description": None,
        "is_paper": is_paper,
        "timestamp": _T0,
        "session_id": _SESSION,
        "sequence_id": 1,
    }


def _scope_grant(wallet: str) -> ScopeGrantRow:
    """Build one active trading scope grant for topology evidence."""
    return {
        "public_id": f"grant-{wallet}",
        "operator_public_id": "operator-1",
        "wallet_public_id": wallet,
        "granted_by_user_public_id": "user-1",
        "scope_kind": "underlying",
        "underlying_public_id": "underlying-1",
        "instrument_public_id": None,
        "note": None,
        "timestamp": _T0,
        "known_to": datetime.max.replace(tzinfo=UTC),
        "session_id": _SESSION,
        "sequence_id": 1,
    }


def _anchor() -> PortfolioPnlAnchorRow:
    """Build the active USD activation anchor row."""
    return {
        "public_id": "anchor-1",
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": _T0,
        "wallet_public_id": _WALLET,
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": _T0,
        "point_kind": "anchor",
        "epoch_public_id": _EPOCH,
        "calc_version": "5A.13",
        "valuation_status": "complete",
        "realized_pnl": 0.0,
        "fee_pnl": 0.0,
        "accrual_pnl": 0.0,
        "external_flow_adjustment": 0.0,
        "unrealized_pnl": 0.0,
        "cash_usd": None,
        "position_value_usd": None,
        "drawdown": None,
        "mark_source": "finalized_1m_candle_close",
        "mark_time": _T0,
        "watermarks_json": None,
        "opening_basket_json": None,
        "contributions_json": None,
    }


def _complete_point(point_time: datetime) -> PnlTimelinePoint:
    """Build a mark-complete P&L point."""
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=1.0,
        fee_pnl=-0.5,
        accrual_pnl=0.25,
        unrealized_pnl=5.0,
        net_pnl=5.75,
        valuation_status="complete",
        incompleteness_reasons=(),
        per_instrument=(),
        attribution=(),
    )


def _untrusted_point(point_time: datetime) -> PnlTimelinePoint:
    """Build an untrusted P&L point (no row)."""
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=None,
        fee_pnl=None,
        accrual_pnl=None,
        unrealized_pnl=None,
        net_pnl=None,
        valuation_status="incomplete",
        incompleteness_reasons=(
            PnlIncompletenessReasonEntry(
                reason="fill_evidence_gap",
                withholding_tier="untrusted",
                withholding_scope="global",
                trigger_instrument_public_id=None,
            ),
        ),
        per_instrument=(),
        attribution=(),
    )


def _attempt(minute: datetime, wallet: str = _WALLET) -> VenueAccountObservationAttemptRow:
    """Build one authoritative kraken USD observation attempt for a minute."""
    return {
        "id": 1,
        "public_id": "obs-1",
        "wallet_public_id": wallet,
        "exchange": "kraken",
        "mode": "live",
        "attempt_status": "observed",
        "balance_status": "observed",
        "position_status": "not_applicable",
        "balances_json": '[{"currency":"USD","total":1000.0},{"currency":"BTC","total":0.0}]',
        "open_positions_json": None,
        "balance_observed_at": minute - timedelta(seconds=30),
        "position_observed_at": None,
        "error": None,
        "timestamp": minute,
        "session_id": _SESSION,
        "sequence_id": 1,
    }


def _meta(seq: int | None = None, affected: datetime | None = None) -> PnlSeriesReplayMetadata:
    """Build one replay-metadata result with an optional watermark and boundary."""
    watermarks = {"kraken": seq} if seq is not None else {}
    return PnlSeriesReplayMetadata(
        max_scope_sequence_by_exchange=watermarks, earliest_affected_minute=affected
    )


def _series(
    from_time: datetime,
    to_time: datetime,
    *,
    metadata: PnlSeriesReplayMetadata | None = None,
    untrusted: bool = False,
) -> PnlWalletSeriesResult:
    """Build a canned series result covering ``[from_time .. to_time]``."""
    points: list[PnlTimelinePoint] = []
    cursor = from_time
    while cursor <= to_time:
        points.append(_untrusted_point(cursor) if untrusted else _complete_point(cursor))
        cursor += timedelta(minutes=1)
    return PnlWalletSeriesResult(
        points=tuple(points),
        granularity="1m",
        valuation_ccy="USD",
        rate_sources=(),
        replay_metadata=metadata,
    )


class _FakeRepo:
    """Hand-rolled async repository double recording the snapshotter's calls."""

    def __init__(
        self,
        *,
        credentials: Sequence[WalletCredentialRow] | None = None,
        anchor: PortfolioPnlAnchorRow | None = None,
        latest: PortfolioPnlSampleRow | None = None,
        persisted: Sequence[PortfolioPnlSampleRow] | None = None,
        balances_json: (
            str | None
        ) = '[{"currency":"USD","total":1000.0},{"currency":"BTC","total":0.0}]',
    ) -> None:
        """Wire the canned reads and empty call logs."""
        self._credentials = list(credentials or [])
        self._anchor = anchor
        self._latest = latest
        self._peak: float | None = None
        self.persisted = list(persisted or [])
        self._balances_json = balances_json
        self.crypto_rows: list[dict[str, Any]] = []
        self.position_versions: list[dict[str, Any]] = []
        self.fx_rows: list[dict[str, Any]] = []
        self.conflict_batch = False
        self.conflict_minutes: set[datetime] = set()
        self.recorded: list[list[PortfolioPnlSampleRow]] = []
        self.superseded: list[tuple[PortfolioPnlSampleRow, bool, str]] = []
        self.retracted: list[tuple[datetime, str, datetime]] = []
        self.peak_before: list[datetime] = []
        self.observation_cuts: list[datetime] = []
        self.observation_scopes: list[tuple[str, tuple[str, ...]]] = []
        self.sample_windows: list[tuple[datetime, datetime, str | None]] = []
        self.active_wallets: list[WalletRow] = []
        self.order_counts: dict[str, int] = {}
        self.active_scope_grants: dict[str, list[ScopeGrantRow]] = {}
        self.wallet_catalogue_requests: list[datetime] = []
        self.order_count_requests: list[tuple[str, ...]] = []
        self.scope_grant_requests: list[str] = []

    async def list_active_wallet_credentials(self, as_of: datetime) -> list[WalletCredentialRow]:
        """Return the canned active credentials."""
        del as_of
        return self._credentials

    async def list_active_wallets(self, as_of: datetime) -> list[WalletRow]:
        """Return the canned active wallet catalogue and record its horizon."""
        self.wallet_catalogue_requests.append(as_of)
        return self.active_wallets

    async def get_orders_total_count(
        self, as_of: datetime, wallet_public_ids: list[str] | None = None
    ) -> int:
        """Return canned order evidence for the single requested wallet."""
        del as_of
        requested = tuple(wallet_public_ids or [])
        self.order_count_requests.append(requested)
        return sum(self.order_counts.get(wallet, 0) for wallet in requested)

    async def list_active_scope_grants_for_wallet(
        self, wallet_public_id: str, as_of: datetime
    ) -> list[ScopeGrantRow]:
        """Return canned active grants and record the candidate wallet."""
        del as_of
        self.scope_grant_requests.append(wallet_public_id)
        return self.active_scope_grants.get(wallet_public_id, [])

    async def get_portfolio_pnl_anchor(
        self, wallet: str, mode: str, ccy: str, as_of: datetime | None
    ) -> PortfolioPnlAnchorRow | None:
        """Return the canned anchor for the scope."""
        del wallet, mode, ccy, as_of
        return self._anchor

    async def get_latest_portfolio_pnl_sample(self, query: Any) -> PortfolioPnlSampleRow | None:
        """Return the canned durable-progress sample."""
        del query
        return self._latest

    async def get_portfolio_pnl_sample_peak(self, query: Any, before: datetime) -> float | None:
        """Return the canned causal peak, recording the ``before`` bound."""
        del query
        self.peak_before.append(before)
        return self._peak

    async def get_portfolio_pnl_samples(
        self, query: Any, start: datetime, end: datetime, *, status: str | None = None
    ) -> list[PortfolioPnlSampleRow]:
        """Return persisted rows in the window filtered by status."""
        del query
        self.sample_windows.append((start, end, status))
        return [
            row
            for row in self.persisted
            if start <= row["point_time"] <= end
            and (status is None or row["valuation_status"] == status)
        ]

    async def get_venue_account_observation_attempts_at(
        self, wallet: str, exchanges: Sequence[str], mode: str, at: datetime
    ) -> dict[str, VenueAccountObservationAttemptRow]:
        """Return an authoritative kraken attempt at the requested minute."""
        del mode
        self.observation_cuts.append(at)
        self.observation_scopes.append((wallet, tuple(exchanges)))
        attempts: dict[str, VenueAccountObservationAttemptRow] = {}
        for exchange in exchanges:
            attempt = _attempt(at, wallet)
            attempt["balances_json"] = self._balances_json
            attempts[exchange] = attempt
        return attempts

    async def get_pnl_crypto_usd_plane_candles(
        self, currencies: Sequence[str], start: datetime, end: datetime, as_of: datetime
    ) -> list[Any]:
        """Return the canned crypto plane rows."""
        del currencies, start, end, as_of
        return self.crypto_rows

    async def get_pnl_scope_position_inventory_window(
        self, wallet: str, mode: str, window_start: datetime, window_end: datetime
    ) -> list[dict[str, Any]]:
        """Return the canned temporal spot-labelling position versions."""
        del wallet, mode, window_start, window_end
        return self.position_versions

    async def get_pnl_fx_rate_exchanges(
        self, pairs: Sequence[tuple[str, str]], start: datetime, end: datetime, as_of: datetime
    ) -> list[tuple[str, str, str]]:
        """Discover venue-qualified forex planes from the canned fiat rows."""
        del as_of
        requested = set(pairs)
        return sorted(
            {
                (row["base"], row["quote"], row["exchange"])
                for row in self.fx_rows
                if (row["base"], row["quote"]) in requested and start <= row["open_at"] <= end
            }
        )

    async def get_pnl_fx_rate_candles(
        self,
        planes: Sequence[tuple[str, str, str]],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[dict[str, Any]]:
        """Return the canned fiat candle rows for the requested venue planes."""
        del as_of
        requested = set(planes)
        return [
            row
            for row in self.fx_rows
            if (row["base"], row["quote"], row["exchange"]) in requested
            and start <= row["open_at"] <= end
        ]

    async def record_portfolio_pnl_samples(
        self, rows: Sequence[PortfolioPnlSampleRow], scope: Any
    ) -> dict[str, tuple[datetime, ...]]:
        """Record inserted rows; report a conflict when configured (B1)."""
        del scope
        self.recorded.append(list(rows))
        minutes = tuple(row["point_time"] for row in rows)
        if self.conflict_batch:
            return {"inserted": (), "already_present": (), "conflicts": minutes}
        self.persisted.extend(rows)
        return {"inserted": minutes, "already_present": (), "conflicts": ()}

    async def supersede_portfolio_pnl_sample(
        self,
        scope: Any,
        replacement: PortfolioPnlSampleRow,
        *,
        derived_suffix_reconciliation: bool,
        expected_public_id: str,
    ) -> PortfolioPnlSampleRow:
        """Record a supersede; raise a CAS conflict when configured (B3)."""
        del scope
        if replacement["point_time"] in self.conflict_minutes:
            raise PortfolioPnlSampleConflictError("synthetic CAS conflict")
        self.superseded.append((replacement, derived_suffix_reconciliation, expected_public_id))
        return replacement

    async def retract_portfolio_pnl_sample(
        self,
        scope: Any,
        point_time: datetime,
        *,
        expected_public_id: str,
        bus_time: datetime,
    ) -> None:
        """Record a retract; raise a CAS conflict when configured (N1)."""
        del scope
        if point_time in self.conflict_minutes:
            raise PortfolioPnlSampleConflictError("synthetic retract CAS conflict")
        self.retracted.append((point_time, expected_public_id, bus_time))


class _FakeSettingsService:
    """Fresh settings reader with mutable pin and optional failure."""

    def __init__(self, pin: JsonValue = "", failure: Exception | None = None) -> None:
        """Store the canned pin and failure mode."""
        self.pin = pin
        self.failure = failure
        self.requests: list[str] = []

    async def get_setting_fresh(self, key: str) -> JsonValue:
        """Return the current pin or raise the configured lookup failure."""
        self.requests.append(key)
        assert key == MINT_WALLET_PIN_SETTING_KEY
        if self.failure is not None:
            raise self.failure
        return self.pin


def _snapshotter(
    repo: _FakeRepo,
    now: datetime,
    *,
    mint_pin: JsonValue = "",
    settings_failure: Exception | None = None,
) -> PortfolioPnlSnapshotter:
    """Build an enabled snapshotter over a fake repo and a fixed clock."""
    settings_service = _FakeSettingsService(mint_pin, settings_failure)
    return PortfolioPnlSnapshotter(
        repo=cast(Repository, repo),
        interval_seconds=60,
        disabled=False,
        clock=_clock(now),
        settings_service=cast(SettingsService, settings_service),
    )


def _persisted_sample(
    point_time: datetime,
    *,
    status: str = "incomplete",
    reasons: tuple[str, ...] = ("missing_mark",),
    realized: float = 99.0,
    watermarks_json: str = "{}",
) -> PortfolioPnlSampleRow:
    """Build one persisted sample row for progress, recompute and self-heal reads."""
    complete = status == "complete"
    row: dict[str, Any] = {
        "public_id": f"sample-{point_time.isoformat()}",
        "session_id": _SESSION,
        "sequence_id": 2,
        "timestamp": point_time,
        "wallet_public_id": _WALLET,
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": point_time,
        "point_kind": "sample",
        "epoch_public_id": _EPOCH,
        "calc_version": "5B.1",
        "valuation_status": status,
        "realized_pnl": realized,
        "fee_pnl": -0.5,
        "accrual_pnl": 0.25,
        "external_flow_adjustment": 0.0,
        "unrealized_pnl": 5.0 if complete else None,
        "cash_usd": 1.0 if complete else None,
        "position_value_usd": 0.0 if complete else None,
        "drawdown": 0.0 if complete else None,
        "mark_source": "finalized_1m_candle_close" if complete else None,
        "mark_time": point_time if complete else None,
        "audit_json": json.dumps(
            {"valuation": [], "observations": [], "reason_codes": list(reasons)}
        ),
        "watermarks_json": watermarks_json,
    }
    return cast(PortfolioPnlSampleRow, row)


def _fx_row(base: str, quote: str, minute: int, close: float, exchange: str) -> dict[str, Any]:
    """Build one forex candle row closing at grid minute ``minute``."""
    return {
        "base": base,
        "quote": quote,
        "exchange": exchange,
        "open_at": _minute(minute - 1),
        "close": close,
        "native_symbol": f"{base}-{quote}",
        "instrument_public_id": f"ins-{base}{quote}-{exchange}",
        "candle_id": minute,
        "candle_public_id": f"cdl-{base}{quote}-{minute}",
        "candle_timestamp": _minute(minute - 1),
    }


def _crypto_row(base: str, minute: datetime, close: float) -> dict[str, Any]:
    """Build one crypto→USD plane row valuing the given grid minute."""
    return {
        "base": base,
        "quote": "USD",
        "exchange": "kraken",
        "native_symbol": f"{base}/USD",
        "instrument_public_id": f"inst-{base}",
        "candle_id": 11,
        "candle_public_id": f"cndl-{base}",
        "open_at": minute - timedelta(minutes=1),
        "close": close,
        "candle_timestamp": minute - timedelta(minutes=1),
    }


def _position_version(
    base_currency: str,
    quantity: float,
    *,
    exchange: str = "walutomat",
    is_spot_margin: bool = False,
) -> dict[str, Any]:
    """Build one temporal position-version row for the window read."""
    return {
        "exchange": exchange,
        "base_currency": base_currency,
        "quantity": quantity,
        "is_spot_margin": is_spot_margin,
        "valid_from": _T0 - timedelta(days=1),
        "valid_to": datetime(2099, 1, 1, tzinfo=UTC),
    }


def _patched_series(seq: int | None = 7) -> Any:
    """Return a context patching the module series function with a canned one."""

    def fake(*args: Any, **kwargs: Any) -> Any:
        del kwargs
        from_time, to_time = args[3], args[4]

        async def _run() -> PnlWalletSeriesResult:
            return _series(from_time, to_time, metadata=_meta(seq=seq))

        return _run()

    return mock.patch.object(pnl_snapshotter, "build_wallet_pnl_series", fake)


def _install_series(monkeypatch: pytest.MonkeyPatch, *, first: PnlSeriesReplayMetadata) -> None:
    """Patch the series function; the first call carries ``first`` metadata."""
    state = {"first": True}

    def fake(*args: Any, **kwargs: Any) -> Any:
        del kwargs
        from_time, to_time = args[3], args[4]
        attach = first if state["first"] else _meta()
        state["first"] = False

        async def _run() -> PnlWalletSeriesResult:
            return _series(from_time, to_time, metadata=attach)

        return _run()

    monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)


class TestVenueScope:
    """Spot-venue resolution and the futures signal."""

    def test_kraken_futures_is_futures_class(self) -> None:
        """The authoritative registry marks kraken_futures as futures."""
        assert is_futures_class_exchange("kraken_futures") is True

    def test_kraken_spot_is_not_futures_class(self) -> None:
        """Spot kraken is not a futures venue."""
        assert is_futures_class_exchange("kraken") is False

    def test_resolve_excludes_paper_and_futures(self) -> None:
        """Only non-paper, non-futures venues enter a wallet's denominator."""
        credentials = [
            _cred("kraken"),
            _cred("kraken_futures"),
            _cred("paper", credential_type="paper"),
        ]
        assert resolve_spot_venues(credentials, _UNRESTRICTED_TOPOLOGY) == {
            _WALLET: frozenset({"kraken"})
        }

    def test_resolve_futures_only_wallet_is_empty(self) -> None:
        """A wallet with only futures credentials resolves to no spot venues."""
        assert resolve_spot_venues([_cred("kraken_futures")], _UNRESTRICTED_TOPOLOGY) == {
            _WALLET: frozenset()
        }


class TestConstructorAndProperties:
    """Constructor wiring and disabled-mode parking."""

    def test_disabled_zeroes_repo(self) -> None:
        """Disabled mode drops the repository handle."""
        snap = PortfolioPnlSnapshotter(repo=cast(Repository, _FakeRepo()), disabled=True)
        assert snap.disabled is True
        assert snap.interval_seconds == 60

    def test_env_resolves_disabled_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With no env flag the snapshotter parks (disabled by default)."""
        monkeypatch.delenv("PNL_SNAPSHOTTER_ENABLED", raising=False)
        monkeypatch.delenv("PNL_SNAPSHOTTER_INTERVAL_SECONDS", raising=False)
        snap = PortfolioPnlSnapshotter(repo=cast(Repository, _FakeRepo()))
        assert snap.disabled is True

    def test_env_enables_and_reads_interval(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An enabling env flag keeps the repo and reads the interval."""
        monkeypatch.setenv("PNL_SNAPSHOTTER_ENABLED", "true")
        monkeypatch.setenv("PNL_SNAPSHOTTER_INTERVAL_SECONDS", "120")
        snap = PortfolioPnlSnapshotter(repo=cast(Repository, _FakeRepo()))
        assert snap.disabled is False
        assert snap.interval_seconds == 120


class TestLifecycle:
    """Start/stop/loop lifecycle mirroring the DbStats snapshotter."""

    @pytest.mark.asyncio
    async def test_disabled_start_is_noop(self) -> None:
        """Disabled start spawns no loop task."""
        snap = PortfolioPnlSnapshotter(repo=None, disabled=True)
        await snap.start()
        assert snap._loop_task is None
        await snap.stop()

    @pytest.mark.asyncio
    async def test_stop_idempotent_when_never_started(self) -> None:
        """Stop on a never-started snapshotter is a clean no-op."""
        snap = _snapshotter(_FakeRepo(), _minute(5))
        await snap.stop()
        assert snap._loop_task is None

    @pytest.mark.asyncio
    async def test_loop_runs_a_tick_then_stops(self) -> None:
        """A short interval runs at least one tick before a clean stop."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = PortfolioPnlSnapshotter(
            repo=cast(Repository, repo),
            interval_seconds=1,
            disabled=False,
            clock=_clock(_minute(5)),
        )
        with _patched_series():
            await snap.start()
            for _ in range(200):
                if repo.recorded:
                    break
                await asyncio.sleep(0.02)
            await snap.stop()
        assert repo.recorded

    @pytest.mark.asyncio
    async def test_loop_continues_after_tick_exception(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A tick exception is logged and the loop survives."""
        seen: list[int] = []

        async def flaky(self: PortfolioPnlSnapshotter) -> None:
            seen.append(1)
            raise RuntimeError("synthetic")

        monkeypatch.setattr(PortfolioPnlSnapshotter, "_tick_once", flaky)
        snap = PortfolioPnlSnapshotter(
            repo=cast(Repository, _FakeRepo()),
            interval_seconds=1,
            disabled=False,
            clock=_clock(_minute(5)),
        )
        with caplog.at_level("ERROR", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap.start()
            for _ in range(200):
                if len(seen) >= 2:
                    break
                await asyncio.sleep(0.02)
            await snap.stop()
        assert len(seen) >= 2
        assert any("_tick_once raised" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_loop_returns_when_stopping_preset(self) -> None:
        """A pre-set stop event exits the loop before the body."""
        repo = _FakeRepo()
        snap = _snapshotter(repo, _minute(5))
        snap._stopping.set()
        await snap._loop()
        assert repo.recorded == []

    @pytest.mark.asyncio
    async def test_loop_returns_when_stopping_set_between_sleep_and_tick(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Stopping set after the sleep timeout exits before ticking."""
        snap = _snapshotter(_FakeRepo(), _minute(5))

        async def fake_wait_for(awaitable: Any, *, timeout: float) -> bool:
            del timeout
            awaitable.close()
            snap._stopping.set()
            raise TimeoutError

        monkeypatch.setattr(asyncio, "wait_for", fake_wait_for)
        await snap._loop()

    @pytest.mark.asyncio
    async def test_loop_returns_when_stopping_set_during_sleep(self) -> None:
        """Stopping mid-sleep returns from the loop via the wait branch."""
        snap = _snapshotter(_FakeRepo(), _minute(5))
        snap._interval_seconds = 10
        await snap.start()
        await asyncio.sleep(0)
        snap._stopping.set()
        task = snap._loop_task
        assert task is not None
        await task
        snap._loop_task = None

    @pytest.mark.asyncio
    async def test_tick_once_disabled_raises(self) -> None:
        """Calling the tick in disabled mode is a programming error."""
        snap = PortfolioPnlSnapshotter(repo=None, disabled=True)
        with pytest.raises(RuntimeError, match="disabled mode"):
            await snap._tick_once()


class TestTickDiscoveryAndIsolation:
    """Scope discovery, failure isolation, log hygiene and pruning."""

    @pytest.mark.asyncio
    async def test_no_anchor_wallet_is_skipped_once(self, caplog: pytest.LogCaptureFixture) -> None:
        """A wallet without a USD anchor is skipped and logged once."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=None)
        snap = _snapshotter(repo, _minute(5))
        with caplog.at_level("INFO", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap._tick_once()
            await snap._tick_once()
        assert repo.recorded == []
        assert sum("no active USD anchor" in r.message for r in caplog.records) == 1

    @pytest.mark.asyncio
    async def test_futures_only_wallet_unsupported_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A futures-only wallet is unsupported and logged once."""
        repo = _FakeRepo(credentials=[_cred("kraken_futures")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        with caplog.at_level("INFO", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap._tick_once()
            await snap._tick_once()
        assert repo.recorded == []
        assert sum("futures-only" in r.message for r in caplog.records) == 1

    @pytest.mark.asyncio
    async def test_pinned_redundant_mint_wallet_persists_no_samples(self) -> None:
        """A labelled mint wallet covered by another Kraken wallet writes no point."""
        repo = _FakeRepo(
            credentials=[
                _cred("kraken"),
                _cred("kraken", wallet=_MINT_WALLET),
            ],
            anchor=_anchor(),
        )
        repo.active_wallets = [
            _wallet_row(_WALLET, "main"),
            _wallet_row(_MINT_WALLET, "market-data"),
        ]
        snap = _snapshotter(repo, _minute(5), mint_pin="label:market-data")
        with _patched_series():
            await snap._tick_once()
        mint_rows = [row for row in repo.persisted if row["wallet_public_id"] == _MINT_WALLET]
        trading_rows = [row for row in repo.persisted if row["wallet_public_id"] == _WALLET]
        assert mint_rows == []
        assert len(trading_rows) == 3
        assert all(wallet != _MINT_WALLET for wallet, _venues in repo.observation_scopes)
        assert repo.order_count_requests == [(_MINT_WALLET,)]
        assert repo.scope_grant_requests == [_MINT_WALLET]

    @pytest.mark.asyncio
    async def test_pinned_only_holder_keeps_full_venue_and_samples(self) -> None:
        """A pinned sole Kraken holder remains executor-backed and sampled."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5), mint_pin=_WALLET)
        with _patched_series():
            await snap._tick_once()
        wallet_rows = [row for row in repo.persisted if row["wallet_public_id"] == _WALLET]
        assert len(wallet_rows) == 3
        assert repo.observation_scopes
        assert all(venues == ("kraken",) for _wallet, venues in repo.observation_scopes)
        assert repo.order_count_requests == [(_WALLET,)]
        assert repo.scope_grant_requests == [_WALLET]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("order_count", "has_scope_grant"), [(1, False), (0, True)])
    async def test_trading_evidence_refuses_mint_exclusion(
        self, order_count: int, has_scope_grant: bool
    ) -> None:
        """Order history or an active scope grant keeps the pinned scope sampled."""
        repo = _FakeRepo(
            credentials=[
                _cred("kraken"),
                _cred("kraken", wallet=_MINT_WALLET),
            ],
            anchor=_anchor(),
        )
        repo.order_counts[_WALLET] = order_count
        if has_scope_grant:
            repo.active_scope_grants[_WALLET] = [_scope_grant(_WALLET)]
        snap = _snapshotter(repo, _minute(5), mint_pin=_WALLET)
        with _patched_series():
            await snap._tick_once()
        trading_rows = [row for row in repo.persisted if row["wallet_public_id"] == _WALLET]
        trading_observations = [
            venues for wallet, venues in repo.observation_scopes if wallet == _WALLET
        ]
        assert len(trading_rows) == 3
        assert trading_observations
        assert all(venues == ("kraken",) for venues in trading_observations)
        assert repo.order_count_requests == [(_WALLET,)]
        assert repo.scope_grant_requests == [_WALLET]

    @pytest.mark.asyncio
    async def test_trading_wallet_output_is_byte_identical_with_pin_active(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Mint exclusion changes no field or observation input for the survivor."""
        credentials = [
            _cred("kraken"),
            _cred("kraken", wallet=_MINT_WALLET),
        ]
        control_repo = _FakeRepo(credentials=credentials, anchor=_anchor())
        pinned_repo = _FakeRepo(credentials=credentials, anchor=_anchor())
        monkeypatch.setattr(pnl_snapshotter, "uuid7", _fixed_uuid7)
        control = _snapshotter(control_repo, _minute(5))
        pinned = _snapshotter(pinned_repo, _minute(5), mint_pin=_MINT_WALLET)
        with _patched_series():
            await control._tick_once()
            await pinned._tick_once()
        control_rows = [row for row in control_repo.persisted if row["wallet_public_id"] == _WALLET]
        pinned_rows = [row for row in pinned_repo.persisted if row["wallet_public_id"] == _WALLET]
        control_observations = [
            venues for wallet, venues in control_repo.observation_scopes if wallet == _WALLET
        ]
        pinned_observations = [
            venues for wallet, venues in pinned_repo.observation_scopes if wallet == _WALLET
        ]
        assert pinned_rows == control_rows
        assert pinned_observations == control_observations
        assert pinned_repo.order_count_requests == [(_MINT_WALLET,)]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mint_pin", ["", None])
    async def test_empty_or_absent_pin_preserves_every_wallet(self, mint_pin: JsonValue) -> None:
        """An empty or absent declaration leaves both Kraken scopes unchanged."""
        repo = _FakeRepo(
            credentials=[
                _cred("kraken"),
                _cred("kraken", wallet=_MINT_WALLET),
            ],
            anchor=_anchor(),
        )
        snap = _snapshotter(repo, _minute(5), mint_pin=mint_pin)
        settings_service = cast(_FakeSettingsService, snap._settings_service)
        with _patched_series():
            await snap._tick_once()
        assert sum(row["wallet_public_id"] == _WALLET for row in repo.persisted) == 3
        assert sum(row["wallet_public_id"] == _MINT_WALLET for row in repo.persisted) == 3
        assert all(venues == ("kraken",) for _wallet, venues in repo.observation_scopes)
        assert repo.wallet_catalogue_requests == []
        assert repo.order_count_requests == []
        assert repo.scope_grant_requests == []
        assert settings_service.requests == [MINT_WALLET_PIN_SETTING_KEY]

    @pytest.mark.asyncio
    async def test_pin_change_is_honored_on_the_next_tick(self) -> None:
        """A fresh setting read applies a newly declared mint identity immediately."""
        repo = _FakeRepo(
            credentials=[
                _cred("kraken"),
                _cred("kraken", wallet=_MINT_WALLET),
            ],
            anchor=_anchor(),
        )
        snap = _snapshotter(repo, _minute(5))
        settings_service = cast(_FakeSettingsService, snap._settings_service)
        with _patched_series():
            await snap._tick_once()
            settings_service.pin = _MINT_WALLET
            await snap._tick_once()
        assert sum(row["wallet_public_id"] == _WALLET for row in repo.persisted) == 6
        assert sum(row["wallet_public_id"] == _MINT_WALLET for row in repo.persisted) == 3
        assert settings_service.requests == [
            MINT_WALLET_PIN_SETTING_KEY,
            MINT_WALLET_PIN_SETTING_KEY,
        ]

    @pytest.mark.asyncio
    async def test_mint_pin_and_futures_only_notices_are_distinct_and_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The same wallet logs each unsupported reason once as topology changes."""
        repo = _FakeRepo(
            credentials=[
                _cred("kraken"),
                _cred("kraken", wallet=_MINT_WALLET),
            ],
            anchor=_anchor(),
        )
        snap = _snapshotter(repo, _minute(5), mint_pin=_WALLET)
        with (
            _patched_series(),
            caplog.at_level("INFO", logger="snapper.application.portfolio.pnl_snapshotter"),
        ):
            await snap._tick_once()
            repo._credentials = [
                _cred("kraken_futures"),
                _cred("kraken", wallet=_MINT_WALLET),
            ]
            await snap._tick_once()
            await snap._tick_once()
        wallet_reasons = [
            getattr(record, "scope_skip_reason", None)
            for record in caplog.records
            if _WALLET in record.message
        ]
        assert wallet_reasons.count("mint_identity_executor_topology") == 1
        assert wallet_reasons.count("no_spot_venues") == 1

    @pytest.mark.asyncio
    async def test_fresh_pin_lookup_failure_fails_open(self) -> None:
        """A fresh setting query failure leaves the full venue sampled."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(
            repo,
            _minute(5),
            settings_failure=RuntimeError("settings unavailable"),
        )
        with _patched_series():
            await snap._tick_once()
        assert len(repo.persisted) == 3
        assert repo.observation_scopes
        assert repo.order_count_requests == []

    @pytest.mark.asyncio
    async def test_scope_failure_logs_once_then_aggregates(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A failing scope logs once per streak plus one aggregate per tick (B4)."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))

        async def boom(self: PortfolioPnlSnapshotter, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("scope boom")

        monkeypatch.setattr(PortfolioPnlSnapshotter, "_process_wallet", boom)
        with caplog.at_level("WARNING", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap._tick_once()
            await snap._tick_once()
        assert sum("sample failed" in r.message for r in caplog.records) == 1
        assert sum("scope(s) failed this tick" in r.message for r in caplog.records) == 2

    @pytest.mark.asyncio
    async def test_failure_log_resets_on_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A scope that recovers clears every failure-class streak (B4/C2)."""
        del monkeypatch
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        snap._failure_logged.add((_WALLET, "sample_failed"))
        snap._failure_logged.add((_WALLET, "supersede_cas"))
        with _patched_series():
            await snap._tick_once()
        assert all(wallet != _WALLET for wallet, _cls in snap._failure_logged)

    @pytest.mark.asyncio
    async def test_prunes_absent_scope_state(self) -> None:
        """In-memory state for a wallet absent from discovery is pruned (B5)."""
        repo = _FakeRepo(credentials=[], anchor=None)
        snap = _snapshotter(repo, _minute(5))
        snap._no_anchor_logged.add("gone-wallet")
        snap._mint_identity_logged.add("gone-wallet")
        snap._baselines["gone-wallet|live|USD|epoch"] = {"kraken": 3}
        await snap._tick_once()
        assert snap._no_anchor_logged == set()
        assert snap._mint_identity_logged == set()
        assert snap._baselines == {}


class TestCatchupAndCorrections:
    """Catch-up insert, causal recompute-forward, CAS and baseline durability."""

    @pytest.mark.asyncio
    async def test_first_catchup_inserts_with_watermarks(self) -> None:
        """A fresh scope inserts finalized minutes carrying the advanced watermark."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        with _patched_series(seq=7):
            await snap._tick_once()
        inserted = repo.recorded[0]
        assert [row["point_time"] for row in inserted] == [_minute(1), _minute(2), _minute(3)]
        assert all(row["valuation_status"] == "complete" for row in inserted)
        assert inserted[0]["cash_usd"] == 1000.0
        assert inserted[0]["watermarks_json"] == '{"kraken":7}'
        assert snap._baselines[f"{_WALLET}|live|USD|{_EPOCH}"] == {"kraken": 7}
        assert _minute(1) in repo.peak_before

    @pytest.mark.asyncio
    async def test_durable_baseline_drives_the_series(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The latest sample's persisted watermark map seeds the engine call (B2)."""
        latest = _persisted_sample(_minute(2), status="complete", watermarks_json='{"kraken":4}')
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=[latest]
        )
        snap = _snapshotter(repo, _minute(6))
        seen: list[dict[str, int]] = []

        def fake(*args: Any, **kwargs: Any) -> Any:
            seen.append(dict(kwargs["options"].baseline_watermarks))
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                return _series(from_time, to_time, metadata=_meta(seq=4))

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        assert seen[0] == {"kraken": 4}

    @pytest.mark.asyncio
    async def test_late_fill_recomputes_forward_under_cas(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A late fill recomputes forward and supersedes changed minutes under CAS."""
        latest = _persisted_sample(_minute(3))
        persisted = [_persisted_sample(_minute(2)), latest]
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=persisted
        )
        snap = _snapshotter(repo, _minute(6))
        _install_series(monkeypatch, first=_meta(seq=9, affected=_minute(2)))
        await snap._tick_once()
        superseded = {(row["point_time"], derived, pid) for row, derived, pid in repo.superseded}
        assert (_minute(2), True, "sample-" + _minute(2).isoformat()) in superseded
        assert (_minute(3), True, "sample-" + _minute(3).isoformat()) in superseded
        assert _minute(2) in repo.peak_before

    @pytest.mark.asyncio
    async def test_self_heal_recomputes_forward_through_tip(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A retryable incomplete minute recomputes forward through the tip (A1)."""
        latest = _persisted_sample(_minute(3))
        persisted = [_persisted_sample(_minute(2)), latest]
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=persisted
        )
        snap = _snapshotter(repo, _minute(5))
        _install_series(monkeypatch, first=_meta(seq=4))
        await snap._tick_once()
        assert repo.recorded == []
        healed = {row["point_time"] for row, _derived, _pid in repo.superseded}
        assert _minute(2) in healed

    @pytest.mark.asyncio
    async def test_final_reason_minute_opens_no_self_heal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A span whose only incomplete minute is terminal opens no self-heal.

        The companion to the retryable case above: without it the recompute
        assertion there would also pass if self-heal opened unconditionally.
        """
        latest = _persisted_sample(_minute(3), reasons=("non_finite",))
        persisted = [_persisted_sample(_minute(2), reasons=("non_finite",)), latest]
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=persisted
        )
        snap = _snapshotter(repo, _minute(5))
        _install_series(monkeypatch, first=_meta(seq=4))
        await snap._tick_once()
        assert repo.superseded == []

    @pytest.mark.asyncio
    async def test_unknown_reason_minute_still_self_heals(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A persisted unrecognised reason code still opens self-heal, end to end.

        The forward-compatibility property the two-release rollout rests on,
        driven through the real seam rather than either half of it: the persisted
        row's audit envelope, :func:`_extract_reason_codes`, ``_self_heal_start``
        and :func:`plan_self_heal_minutes`, all the way to an actual supersede.
        Pinning only the reader and the gate separately leaves the composition
        untested — a reader that narrowed the token back to a terminal code would
        satisfy both unit tests' neighbours and still strand the minute here.

        The positive counterpart of the terminal case above: that one proves a
        final code opens nothing, this one proves an unknown code is not treated
        as final.
        """
        latest = _persisted_sample(_minute(3), reasons=("a_future_code",))
        persisted = [_persisted_sample(_minute(2), reasons=("a_future_code",)), latest]
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=persisted
        )
        snap = _snapshotter(repo, _minute(5))
        _install_series(monkeypatch, first=_meta(seq=4))
        await snap._tick_once()
        assert repo.recorded == []
        healed = {row["point_time"] for row, _derived, _pid in repo.superseded}
        assert _minute(2) in healed

    @pytest.mark.asyncio
    async def test_self_heal_start_uses_the_shared_lookback(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The self-heal read window is the planner's constant, not a local copy.

        Pins the deduplication: the service used to hardcode its own 15 minutes
        beside :data:`SELF_HEAL_LOOKBACK`, so the two could drift apart silently.
        """
        latest = _persisted_sample(_minute(3))
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), latest=latest)
        snap = _snapshotter(repo, _minute(5))
        monkeypatch.setattr(pnl_snapshotter, "SELF_HEAL_LOOKBACK", timedelta(minutes=7))
        _install_series(monkeypatch, first=_meta(seq=4))
        await snap._tick_once()
        assert (_minute(5) - timedelta(minutes=7), _minute(3), "incomplete") in repo.sample_windows

    @pytest.mark.asyncio
    async def test_identical_recompute_is_skip_idempotent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A recompute matching the persisted rows is never superseded (skip-idempotent)."""
        latest = _persisted_sample(_minute(3))
        persisted = [_persisted_sample(_minute(2)), latest]
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=persisted
        )
        snap = _snapshotter(repo, _minute(6))
        monkeypatch.setattr(pnl_snapshotter, "_sample_values_equal", lambda existing, other: True)
        _install_series(monkeypatch, first=_meta(seq=9, affected=_minute(2)))
        await snap._tick_once()
        assert repo.superseded == []

    @pytest.mark.asyncio
    async def test_restored_minute_is_inserted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A formerly-untrusted minute the recompute restores is inserted, not discarded (P1)."""
        latest = _persisted_sample(_minute(3))
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=[latest]
        )
        snap = _snapshotter(repo, _minute(6))
        _install_series(monkeypatch, first=_meta(seq=9, affected=_minute(2)))
        await snap._tick_once()
        inserted = {row["point_time"] for chunk in repo.recorded for row in chunk}
        assert _minute(2) in inserted
        superseded_minutes = {row["point_time"] for row, _l, _p in repo.superseded}
        assert superseded_minutes == {_minute(3)}

    @pytest.mark.asyncio
    async def test_recompute_insert_conflict_is_logged(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A conflicting recompute-insert logs and retries next tick (P1/B1)."""
        latest = _persisted_sample(_minute(3))
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=[latest]
        )
        repo.conflict_batch = True
        snap = _snapshotter(repo, _minute(6))
        _install_series(monkeypatch, first=_meta(seq=9, affected=_minute(2)))
        with caplog.at_level("WARNING", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap._tick_once()
        assert any("recompute-insert conflicts" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_catchup_without_metadata_clears_baseline(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A catch-up run reporting no metadata advances the baseline to empty."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                return _series(from_time, to_time, metadata=None)

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        assert snap._baselines[f"{_WALLET}|live|USD|{_EPOCH}"] == {}
        assert repo.recorded[0][0]["watermarks_json"] == "{}"

    @pytest.mark.asyncio
    async def test_supersede_cas_conflict_aborts_catchup(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A lost supersede CAS aborts the scope BEFORE catch-up (C1).

        No catch-up rows are written and the durable baseline is left untouched, so
        the late fill stays above the baseline and the whole flow retries next tick.
        """
        latest = _persisted_sample(_minute(3))
        persisted = [_persisted_sample(_minute(2)), latest]
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=persisted
        )
        repo.conflict_minutes = {_minute(2)}
        snap = _snapshotter(repo, _minute(6))
        _install_series(monkeypatch, first=_meta(seq=9, affected=_minute(2)))
        with caplog.at_level("WARNING", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap._tick_once()
        assert repo.superseded == []
        assert repo.recorded == []
        assert snap._baselines == {}
        assert any("supersede CAS lost" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_conflict_warning_is_logged_once_per_streak(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A persistent supersede conflict logs once per streak, not every tick (C2)."""
        latest = _persisted_sample(_minute(3))
        persisted = [_persisted_sample(_minute(2)), latest]
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=persisted
        )
        repo.conflict_minutes = {_minute(2)}
        snap = _snapshotter(repo, _minute(6))

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                return _series(from_time, to_time, metadata=_meta(seq=9, affected=_minute(2)))

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        with caplog.at_level("WARNING", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap._tick_once()
            await snap._tick_once()
        assert sum("supersede CAS lost" in r.message for r in caplog.records) == 1
        assert sum("scope(s) failed this tick" in r.message for r in caplog.records) == 2

    @pytest.mark.asyncio
    async def test_untrusted_recompute_retracts_the_stale_row(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A minute whose recompute falls to untrusted is retracted, not left stale (N1)."""
        latest = _persisted_sample(_minute(3), status="complete")
        persisted = [_persisted_sample(_minute(2), status="complete"), latest]
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=persisted
        )
        snap = _snapshotter(repo, _minute(6))

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                if from_time == _minute(2):
                    return _series(from_time, to_time, metadata=_meta(), untrusted=True)
                return _series(from_time, to_time, metadata=_meta(seq=9, affected=_minute(2)))

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        retracted = {point_time for point_time, _pid, _bt in repo.retracted}
        assert _minute(2) in retracted
        assert repo.superseded == []

    @pytest.mark.asyncio
    async def test_retract_cas_conflict_is_logged_and_skipped(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A lost retract CAS logs and leaves the minute for a clean retry (N1)."""
        latest = _persisted_sample(_minute(3), status="complete")
        persisted = [_persisted_sample(_minute(2), status="complete"), latest]
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=persisted
        )
        repo.conflict_minutes = {_minute(2)}
        snap = _snapshotter(repo, _minute(6))

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                if from_time == _minute(2):
                    return _series(from_time, to_time, metadata=_meta(), untrusted=True)
                return _series(from_time, to_time, metadata=_meta(seq=9, affected=_minute(2)))

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        with caplog.at_level("WARNING", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap._tick_once()
        assert all(point_time != _minute(2) for point_time, _pid, _bt in repo.retracted)
        assert any("retract CAS lost" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_catchup_stops_at_the_first_chunk_conflict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A conflicting catch-up chunk stops the write so no later chunk crosses a gap (C1)."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        repo.conflict_batch = True
        snap = _snapshotter(repo, _minute(5))
        monkeypatch.setattr(
            pnl_snapshotter,
            "plan_catchup_chunks",
            lambda *args, **kwargs: (
                ChunkWindow(_minute(1), _minute(1)),
                ChunkWindow(_minute(2), _minute(2)),
            ),
        )
        with _patched_series(seq=7):
            await snap._tick_once()
        assert len(repo.recorded) == 1
        assert all(row["point_time"] != _minute(2) for chunk in repo.recorded for row in chunk)
        assert snap._baselines == {}

    @pytest.mark.asyncio
    async def test_batch_conflict_does_not_advance_baseline(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A catch-up batch conflict withholds the baseline advance (B1)."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        repo.conflict_batch = True
        snap = _snapshotter(repo, _minute(5))
        with (
            caplog.at_level("WARNING", logger="snapper.application.portfolio.pnl_snapshotter"),
            _patched_series(seq=7),
        ):
            await snap._tick_once()
        assert snap._baselines == {}
        assert any("not advancing baseline" in r.message for r in caplog.records)


class TestBudgetSplitAndHelpers:
    """Budget-safe splitting and the pure service helpers."""

    def test_split_chunk_halves(self) -> None:
        """A multi-minute chunk splits into two covering halves."""
        chunk = ChunkWindow(start=_minute(1), end=_minute(4))
        left, right = PortfolioPnlSnapshotter._split_chunk(chunk)
        assert left.start == _minute(1)
        assert right.end == _minute(4)
        assert right.start == left.end + timedelta(minutes=1)

    def test_split_single_minute_raises(self) -> None:
        """A single-minute chunk cannot be split further."""
        chunk = ChunkWindow(start=_minute(1), end=_minute(1))
        with pytest.raises(PnlTimelineWorkBudgetError, match="single-minute"):
            PortfolioPnlSnapshotter._split_chunk(chunk)

    @pytest.mark.asyncio
    async def test_catchup_subdivides_on_budget_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A chunk that trips the budget is subdivided and still persisted."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        calls: list[tuple[datetime, datetime]] = []

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                calls.append((from_time, to_time))
                if from_time == _minute(1) and to_time == _minute(3):
                    raise PnlTimelineWorkBudgetError("too big")
                return _series(from_time, to_time, metadata=_meta())

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        assert (_minute(1), _minute(3)) in calls
        persisted = [row["point_time"] for chunk in repo.recorded for row in chunk]
        assert set(persisted) == {_minute(1), _minute(2), _minute(3)}

    @pytest.mark.asyncio
    async def test_recompute_forward_subdivides_on_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A late-fill recompute window that trips the budget is subdivided."""
        latest = _persisted_sample(_minute(5))
        persisted = [_persisted_sample(_minute(offset)) for offset in (1, 2, 3, 4, 5)]
        repo = _FakeRepo(
            credentials=[_cred("kraken")], anchor=_anchor(), latest=latest, persisted=persisted
        )
        snap = _snapshotter(repo, _minute(8))
        state = {"first": True}

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]
            attach = _meta(seq=9, affected=_minute(1)) if state["first"] else _meta()
            state["first"] = False

            async def _run() -> PnlWalletSeriesResult:
                if from_time == _minute(1) and to_time == _minute(5):
                    raise PnlTimelineWorkBudgetError("recompute too big")
                return _series(from_time, to_time, metadata=attach)

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        assert any(row["point_time"] == _minute(1) for row, _l, _p in repo.superseded)

    def test_watermarks_json_serializes_canonically(self) -> None:
        """The advanced watermark map serializes to canonical JSON."""
        assert _watermarks_json(_meta(seq=3)) == '{"kraken":3}'
        assert _watermarks_json(None) == "{}"

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ('{"kraken":5}', {"kraken": 5}),
            ("{bad", {}),
            ("[]", {}),
            ('{"kraken":true}', {}),
        ],
    )
    def test_parse_baseline(self, raw: str, expected: dict[str, int]) -> None:
        """The durable baseline parser is fail-safe on any malformed payload."""
        assert _parse_baseline(raw) == expected

    def test_sample_values_equal_ignores_provenance(self) -> None:
        """Value equality ignores the write provenance identity columns."""
        left = _persisted_sample(_minute(1))
        right = {**left, "public_id": "other", "timestamp": _minute(9)}
        assert _sample_values_equal(left, cast(PortfolioPnlSampleRow, right)) is True
        changed = {**left, "realized_pnl": 1.0}
        assert _sample_values_equal(left, cast(PortfolioPnlSampleRow, changed)) is False

    def test_observed_currencies_collects_non_usd(self) -> None:
        """Observed non-USD currencies are collected for the crypto load."""
        assert _observed_currencies(_attempt(_minute(1))) == {"BTC"}

    @pytest.mark.parametrize("balances_json", ["{bad", '{"currency":"USD"}', "[123]", None])
    def test_observed_currencies_tolerates_malformed(self, balances_json: str | None) -> None:
        """A malformed payload or non-object entry contributes no currencies."""
        attempt = _attempt(_minute(1))
        attempt["balances_json"] = balances_json
        assert _observed_currencies(attempt) == set()

    def test_observed_currencies_ignores_unobserved(self) -> None:
        """An unobserved attempt contributes no currencies."""
        attempt = _attempt(_minute(1))
        attempt["balance_status"] = "error"
        assert _observed_currencies(attempt) == set()

    def test_extract_reason_codes_parses_incomplete(self) -> None:
        """Reason codes are read from an incomplete sample's audit envelope."""
        audit = json.dumps({"valuation": [], "observations": [], "reason_codes": ["missing_mark"]})
        assert _extract_reason_codes(audit) == frozenset({"missing_mark"})

    @pytest.mark.parametrize(
        ("audit", "expected"),
        [
            ("{bad", frozenset()),
            ("[1,2]", frozenset()),
            ('{"reason_codes":"x"}', frozenset()),
            ('{"reason_codes":[1]}', frozenset()),
            ('{"reason_codes":["basket_stale",7]}', frozenset({"basket_stale"})),
        ],
    )
    def test_extract_reason_codes_tolerates_malformed(
        self, audit: str, expected: frozenset[str]
    ) -> None:
        """A malformed envelope yields nothing; a mixed list keeps its string tokens.

        The last parameter is the positive control: without it every case asserts
        an empty result and an unconditional ``frozenset()`` return would pass.
        """
        assert _extract_reason_codes(audit) == expected

    def test_extract_reason_codes_preserves_unknown_token(self) -> None:
        """An unrecognised persisted token is returned verbatim, never narrowed.

        Narrowing it to a canonical member here is what previously stamped an
        unknown code as terminal ``non_finite`` and stranded every minute a newer
        writer produced. The eligibility verdict belongs to the planner's FINAL
        deny-list, which reads this token and finds it absent from the deny-list.
        """
        assert _extract_reason_codes(json.dumps({"reason_codes": ["mystery"]})) == frozenset(
            {"mystery"}
        )


class TestEvidenceAndPartition:
    """Crypto plane, fiat forex plane, and the temporal position partition."""

    @pytest.mark.asyncio
    async def test_usd_only_basket_skips_crypto_load(self) -> None:
        """A USD-only basket collects no currencies and loads no crypto plane."""
        repo = _FakeRepo(
            credentials=[_cred("kraken")],
            anchor=_anchor(),
            balances_json='[{"currency":"USD","total":500.0}]',
        )
        snap = _snapshotter(repo, _minute(5))
        with _patched_series():
            await snap._tick_once()
        assert repo.recorded[0][0]["cash_usd"] == 500.0

    @pytest.mark.asyncio
    async def test_crypto_plane_prices_a_held_currency(self) -> None:
        """A crypto balance is priced off the loaded plane into equity."""
        balances = '[{"currency":"USD","total":100.0},{"currency":"BTC","total":2.0}]'
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), balances_json=balances)
        repo.crypto_rows = [_crypto_row("BTC", _minute(offset), 50.0) for offset in (1, 2, 3)]
        snap = _snapshotter(repo, _minute(5))
        with _patched_series():
            await snap._tick_once()
        assert repo.recorded[0][0]["cash_usd"] == 200.0

    @pytest.mark.asyncio
    async def test_untrusted_series_writes_no_rows(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A catch-up window of untrusted points records no samples."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                return _series(from_time, to_time, metadata=_meta(), untrusted=True)

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        assert repo.recorded == []

    @pytest.mark.asyncio
    async def test_eur_pln_basket_prices_to_complete_equity(self) -> None:
        """EUR + PLN balances value off forex planes into one complete equity."""
        balances = '[{"currency":"EUR","total":10000.0},{"currency":"PLN","total":400.0}]'
        repo = _FakeRepo(credentials=[_cred("walutomat")], anchor=_anchor(), balances_json=balances)
        repo.fx_rows = [
            row
            for minute in (1, 2, 3)
            for row in (
                _fx_row("EUR", "USD", minute, 1.1, "kraken"),
                _fx_row("USD", "PLN", minute, 4.0, "walutomat"),
            )
        ]
        snap = _snapshotter(repo, _minute(5))
        with _patched_series():
            await snap._tick_once()
        first = repo.recorded[0][0]
        assert first["valuation_status"] == "complete"
        assert first["cash_usd"] == pytest.approx(11100.0)
        assert first["position_value_usd"] == 0.0

    @pytest.mark.asyncio
    async def test_small_eur_position_labels_partition(self) -> None:
        """A 20-EUR spot position labels 20 EUR as position, the rest as cash."""
        balances = '[{"currency":"EUR","total":10000.0}]'
        repo = _FakeRepo(credentials=[_cred("walutomat")], anchor=_anchor(), balances_json=balances)
        repo.fx_rows = [_fx_row("EUR", "USD", minute, 1.1, "kraken") for minute in (1, 2, 3)]
        repo.position_versions = [_position_version("EUR", 20.0)]
        snap = _snapshotter(repo, _minute(5))
        with _patched_series():
            await snap._tick_once()
        first = repo.recorded[0][0]
        assert first["position_value_usd"] == pytest.approx(22.0)
        assert first["cash_usd"] == pytest.approx(10978.0)

    @pytest.mark.asyncio
    async def test_unproven_position_stays_cash(self) -> None:
        """A position not proven spot (is_spot_margin) never labels position value."""
        balances = '[{"currency":"EUR","total":10000.0}]'
        repo = _FakeRepo(credentials=[_cred("walutomat")], anchor=_anchor(), balances_json=balances)
        repo.fx_rows = [_fx_row("EUR", "USD", minute, 1.1, "kraken") for minute in (1, 2, 3)]
        repo.position_versions = [_position_version("EUR", 20.0, is_spot_margin=True)]
        snap = _snapshotter(repo, _minute(5))
        with _patched_series():
            await snap._tick_once()
        first = repo.recorded[0][0]
        assert first["position_value_usd"] == 0.0
        assert first["cash_usd"] == pytest.approx(11000.0)
        assert json.loads(first["audit_json"])["coverage"]["leveraged_inventory_excluded"] is True
