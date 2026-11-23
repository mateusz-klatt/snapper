"""Tests for the Phase-5B portfolio equity/drawdown snapshotter service."""

import asyncio
import json
import unittest.mock as mock
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from typing import cast
from uuid import UUID

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import SQLAlchemyError

from snapper.application.portfolio import pnl_correction_planner
from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio import pnl_snapshotter_config
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    PnlCheckpointWriteRefusalError,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    checkpoint_write_refusal,
)
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_snapshot_planner import WithheldMinute
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.application.portfolio.pnl_snapshotter import _extract_reason_codes
from snapper.application.portfolio.pnl_snapshotter import _observed_currencies
from snapper.application.portfolio.pnl_snapshotter import _watermarks_json
from snapper.application.portfolio.pnl_snapshotter import is_futures_class_exchange
from snapper.application.portfolio.pnl_snapshotter import resolve_spot_venues
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReasonEntry
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline_service import PnlFrozenReplayEvidenceChangedError
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayMetadata
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.process_manager.executor_topology import MINT_WALLET_PIN_SETTING_KEY
from snapper.application.process_manager.executor_topology import ExecutorTopology
from snapper.application.services.settings import SettingsService
from snapper.core.json_types import JsonValue
from snapper.data.pnl_correction_state import IdAggregate
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionEdit
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.pnl_correction_state import PnlCorrectionRequest
from snapper.data.pnl_correction_state import PnlCorrectionSampleVersion
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.pnl_correction_state import PnlOpeningCheckpointWrite
from snapper.data.pnl_correction_state import PortfolioPnlCorrectionConflictError
from snapper.data.pnl_correction_state import advance_correction_frontier
from snapper.data.pnl_correction_state import start_correction_frontier
from snapper.data.repository import PortfolioPnlSampleQuery
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PnlFxRatePlane
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
_EXECUTION_MANIFEST_DIGEST = "a" * 64
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


def _unrepresentable_point(point_time: datetime) -> PnlTimelinePoint:
    """Build a point withheld ONLY because no float can express a sound sum."""
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
                reason="instrument_sum_unrepresentable",
                withholding_tier="untrusted",
                withholding_scope="global",
                trigger_instrument_public_id=None,
            ),
        ),
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


def _meta(
    seq: int | None = None, affected: datetime | None = None, *, has_fill_gap: bool = False
) -> PnlSeriesReplayMetadata:
    """Build one replay-metadata result with an optional watermark and boundary."""
    watermarks = {"kraken": seq} if seq is not None else {}
    return PnlSeriesReplayMetadata(
        max_scope_sequence_by_exchange=watermarks,
        earliest_affected_minute=affected,
        raw_execution_watermarks=watermarks.copy(),
        execution_manifest_digest=_EXECUTION_MANIFEST_DIGEST,
        has_fill_gap=has_fill_gap,
        fill_witness_cut={"watermarks": {"kraken:spot": 10}, "digest": "c" * 64},
    )


def _series(
    from_time: datetime,
    to_time: datetime,
    *,
    metadata: PnlSeriesReplayMetadata | None = None,
    untrusted: bool = False,
    unrepresentable: bool = False,
) -> PnlWalletSeriesResult:
    """Build a canned series result covering ``[from_time .. to_time]``."""
    points: list[PnlTimelinePoint] = []
    cursor = from_time
    while cursor <= to_time:
        if unrepresentable:
            points.append(_unrepresentable_point(cursor))
        else:
            points.append(_untrusted_point(cursor) if untrusted else _complete_point(cursor))
        cursor += timedelta(minutes=1)
    return PnlWalletSeriesResult(
        points=tuple(points),
        granularity="1m",
        valuation_ccy="USD",
        rate_sources=(),
        replay_metadata=metadata,
    )


def _sample_scope(wallet: str = _WALLET) -> PortfolioPnlSampleScope:
    """Create the anchor-bound scope with the same identity as the scheduler."""
    return PortfolioPnlSampleScope(wallet, "live", "USD", _EPOCH, _T0)


class _FakeRepo:
    """Hand-rolled async repository double recording the snapshotter's calls."""

    def __init__(
        self,
        *,
        credentials: Sequence[WalletCredentialRow] | None = None,
        anchor: PortfolioPnlAnchorRow | None = None,
        persisted: Sequence[PortfolioPnlSampleRow] | None = None,
        balances_json: (
            str | None
        ) = '[{"currency":"USD","total":1000.0},{"currency":"BTC","total":0.0}]',
    ) -> None:
        """Wire the canned reads and empty call logs."""
        self._credentials = list(credentials or [])
        self._anchor = anchor
        self._frontiers: dict[PnlCorrectionKey, PnlCorrectionFrontier] = {}
        self._opening_checkpoints: dict[PnlCorrectionKey, PnlOpeningCheckpointRecord] = {}
        self._samples = {
            self._sample_key(row): PnlCorrectionSampleVersion(index, row.copy())
            for index, row in enumerate(persisted or (), start=1)
        }
        self._next_row_id = len(self._samples) + 1
        self._balances_json = balances_json
        self.crypto_rows: list[dict[str, Any]] = []
        self.position_versions: list[dict[str, Any]] = []
        self.fx_rows: list[dict[str, Any]] = []
        self.conflict_batch = False
        self.conflict_minutes: set[datetime] = set()
        self.recorded: list[list[PortfolioPnlSampleRow]] = []
        self.attempted_chunks: list[PnlCorrectionChunk] = []
        self.committed_chunks: list[PnlCorrectionChunk] = []
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

    @property
    def persisted(self) -> list[PortfolioPnlSampleRow]:
        """Return the active physical versions after successful transactions."""
        return [version.row for version in self._samples.values()]

    @property
    def superseded(self) -> list[PnlCorrectionEdit]:
        """Return the committed CAS replacements."""
        return [
            edit
            for chunk in self.committed_chunks
            for edit in chunk.edits
            if edit.expected_row is not None and edit.replacement is not None
        ]

    @property
    def retracted(self) -> list[PnlCorrectionEdit]:
        """Return the committed CAS retractions."""
        return [
            edit
            for chunk in self.committed_chunks
            for edit in chunk.edits
            if edit.expected_row is not None and edit.replacement is None
        ]

    @staticmethod
    def _sample_key(row: PortfolioPnlSampleRow) -> tuple[str, str, str, datetime]:
        """Match the repository's active unique minute identity."""
        return row["wallet_public_id"], row["mode"], row["valuation_ccy"], row["point_time"]

    @staticmethod
    def _correction_key(scope: PortfolioPnlSampleScope) -> PnlCorrectionKey:
        """Bind the frontier to the same five-part identity as the production writer."""
        return PnlCorrectionKey(
            scope.wallet_public_id,
            scope.mode,
            scope.valuation_ccy,
            scope.epoch_public_id,
            PNL_SAMPLE_CALC_VERSION,
        )

    @staticmethod
    def _scope_rows(
        rows: Sequence[PnlCorrectionSampleVersion],
        scope: PortfolioPnlSampleScope | PortfolioPnlSampleQuery,
    ) -> list[PnlCorrectionSampleVersion]:
        """Read all active versions for a wallet, mode and valuation currency."""
        return [
            version
            for version in rows
            if (
                version.row["wallet_public_id"],
                version.row["mode"],
                version.row["valuation_ccy"],
            )
            == (scope.wallet_public_id, scope.mode, scope.valuation_ccy)
        ]

    def _peak_before(
        self,
        scope: PortfolioPnlSampleScope,
        versions: Sequence[PnlCorrectionSampleVersion],
        before: datetime,
    ) -> float | None:
        """Calculate the exact-epoch, exact-algorithm complete-sample causal peak."""
        equities = [
            cash + position
            for version in self._scope_rows(versions, scope)
            if scope.anchor_point_time < version.row["point_time"] < before
            and version.row["epoch_public_id"] == scope.epoch_public_id
            and version.row["calc_version"] == PNL_SAMPLE_CALC_VERSION
            and version.row["valuation_status"] == "complete"
            and (cash := version.row["cash_usd"]) is not None
            and (position := version.row["position_value_usd"]) is not None
        ]
        return max(equities, default=None)

    def _evidence(
        self, scope: PortfolioPnlSampleScope, start: datetime, end: datetime
    ) -> PnlCorrectionEvidence:
        """Snapshot complete physical evidence across epochs and versions."""
        samples = list(self._samples.values())
        versions = tuple(
            PnlCorrectionSampleVersion(version.row_id, version.row.copy())
            for version in sorted(
                self._scope_rows(samples, scope), key=lambda item: item.row["point_time"]
            )
            if start <= version.row["point_time"] <= end
        )
        return PnlCorrectionEvidence(
            self._correction_key(scope),
            start,
            end,
            versions,
            self._peak_before(scope, samples, start),
        )

    def frontier(self, wallet: str = _WALLET) -> PnlCorrectionFrontier:
        """Return the exact durable state for the test's active scope."""
        return self._frontiers[self._correction_key(_sample_scope(wallet))]

    def seed_completed_replay(self, tip: datetime, *, seq: int = 4) -> None:
        """Set a settled cursor independent of the existing financial sample rows."""
        scope = _sample_scope()
        evidence = self._evidence(scope, tip, tip)
        watermark = {"kraken": seq}
        initial = start_correction_frontier(
            evidence.key,
            PnlCorrectionRequest(
                tip,
                tip,
                tip + timedelta(minutes=2),
                watermark,
                watermark,
                evidence.prior_peak,
                raw_execution_watermarks=watermark,
                execution_manifest_digest=_EXECUTION_MANIFEST_DIGEST,
                has_fill_gap=False,
                expected_venues=frozenset({"kraken"}),
                venue_event_watermarks={"kraken:spot": 10},
                fill_witness_digest="c" * 64,
            ),
            None,
        )
        final_peak = self._peak_before(
            scope, list(self._samples.values()), tip + timedelta(minutes=1)
        )
        self._frontiers[evidence.key] = advance_correction_frontier(
            initial, PnlCorrectionChunk(tip, (), final_peak, evidence)
        )

    async def get_portfolio_pnl_correction_frontier(
        self, scope: PortfolioPnlSampleScope
    ) -> PnlCorrectionFrontier | None:
        """Return the exact scope's durable checkpoint."""
        return self._frontiers.get(self._correction_key(scope))

    async def invalidate_portfolio_pnl_correction(
        self,
        scope: PortfolioPnlSampleScope,
        request: PnlCorrectionRequest,
        expected: PnlCorrectionFrontier | None,
    ) -> PnlCorrectionFrontier:
        """Create or rewind under the exact checkpoint and causal-peak guards."""
        key = self._correction_key(scope)
        if self._frontiers.get(key) != expected:
            raise PortfolioPnlCorrectionConflictError("synthetic stale frontier")
        frontier = start_correction_frontier(key, request, expected)
        if self._peak_before(scope, list(self._samples.values()), frontier.window_start) != (
            frontier.prior_peak
        ):
            raise PortfolioPnlCorrectionConflictError("synthetic stale initial peak")
        self._frontiers[key] = frontier
        return frontier

    async def get_portfolio_pnl_correction_evidence(
        self, scope: PortfolioPnlSampleScope, start: datetime, end: datetime
    ) -> PnlCorrectionEvidence:
        """Return exact physical versions and record the causal prefix boundary."""
        self.peak_before.append(start)
        return self._evidence(scope, start, end)

    async def get_portfolio_pnl_samples(
        self,
        query: PortfolioPnlSampleQuery,
        start: datetime,
        end: datetime,
        *,
        status: str | None = None,
    ) -> list[PortfolioPnlSampleRow]:
        """Return exact-scope active samples filtered to the requested status."""
        self.sample_windows.append((start, end, status))
        return [
            version.row
            for version in self._scope_rows(list(self._samples.values()), query)
            if start <= version.row["point_time"] <= end
            and version.row["epoch_public_id"] == query.epoch_public_id
            and version.row["calc_version"] == query.calc_version
            and (status is None or version.row["valuation_status"] == status)
        ]

    async def get_venue_account_observation_attempt_stream(
        self,
        wallet: str,
        exchanges: Sequence[str],
        mode: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[VenueAccountObservationAttemptRow]:
        """Emit each exchange's opening seed and later attempts in bus order."""
        rows: list[VenueAccountObservationAttemptRow] = []
        minute = window_start
        venues = tuple(sorted(exchanges))
        minute_duration = timedelta(minutes=1)
        while minute <= window_end:
            self.observation_cuts.append(minute)
            self.observation_scopes.append((wallet, venues))
            row_id = ((minute - _T0) // minute_duration) * 100
            for index, exchange in enumerate(venues, start=1):
                attempt = _attempt(minute, wallet)
                attempt["id"] = row_id + index
                attempt["exchange"] = exchange
                attempt["mode"] = mode
                attempt["balances_json"] = self._balances_json
                rows.append(attempt)
            minute += minute_duration
        return rows

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
        self,
        pairs: Sequence[tuple[str, str]],
        start: datetime,
        end: datetime,
        as_of: datetime,
        *,
        open_intervals: Sequence[tuple[datetime, datetime]] = (),
    ) -> list[tuple[str, str, str]]:
        """Discover venue-qualified forex planes from the canned fiat rows."""
        del as_of
        requested = set(pairs)
        return sorted(
            {
                (row["base"], row["quote"], row["exchange"])
                for row in self.fx_rows
                if (row["base"], row["quote"]) in requested
                and start <= row["open_at"] <= end
                and (
                    not open_intervals
                    or any(lower <= row["open_at"] <= upper for lower, upper in open_intervals)
                )
            }
        )

    async def get_pnl_fx_rate_exchanges_at(
        self,
        pairs: Sequence[tuple[str, str]],
        open_ats: Sequence[datetime],
        as_of: datetime,
    ) -> list[tuple[str, str, str]]:
        """Discover venue-qualified forex planes from canned rows at exact opens."""
        del as_of
        requested = set(pairs)
        wanted = set(open_ats)
        return sorted(
            {
                (row["base"], row["quote"], row["exchange"])
                for row in self.fx_rows
                if (row["base"], row["quote"]) in requested and row["open_at"] in wanted
            }
        )

    async def get_pnl_fx_rate_candles_at(
        self,
        planes: Sequence[tuple[str, str, str]],
        open_ats: Sequence[datetime],
        as_of: datetime,
    ) -> list[dict[str, object]]:
        """Return the canned fiat candle rows for the requested planes at exact opens."""
        del as_of
        requested = set(planes)
        wanted = set(open_ats)
        return [
            row
            for row in self.fx_rows
            if (row["base"], row["quote"], row["exchange"]) in requested
            and row["open_at"] in wanted
        ]

    async def get_pnl_fx_rate_candles(
        self,
        planes: Sequence[tuple[str, str, str]],
        start: datetime,
        end: datetime,
        as_of: datetime,
        *,
        open_intervals: Sequence[tuple[datetime, datetime]] = (),
    ) -> list[dict[str, Any]]:
        """Return the canned fiat candle rows for the requested venue planes."""
        del as_of
        requested = set(planes)
        return [
            row
            for row in self.fx_rows
            if (row["base"], row["quote"], row["exchange"]) in requested
            and start <= row["open_at"] <= end
            and (
                not open_intervals
                or any(lower <= row["open_at"] <= upper for lower, upper in open_intervals)
            )
        ]

    def consume_last_pnl_checkpoint_write_refusal(self) -> str | None:
        """Return and clear the synthetic savepoint write refusal if any."""
        refusal = getattr(self, "_last_pnl_checkpoint_write_refusal", None)
        self._last_pnl_checkpoint_write_refusal = None
        return refusal

    def _upsert_synthetic_checkpoint(
        self,
        key: PnlCorrectionKey,
        checkpoint: PnlOpeningCheckpointWrite,
    ) -> None:
        """Store one synthetic opening checkpoint or raise configured upsert failure."""
        failure = getattr(self, "upsert_failure", None)
        if failure is not None:
            raise failure
        self._opening_checkpoints[key] = PnlOpeningCheckpointRecord(
            checkpoint.certificate,
            checkpoint.state_json,
            datetime.now(UTC),
        )

    def _record_synthetic_checkpoint(
        self,
        key: PnlCorrectionKey,
        frontier: PnlCorrectionFrontier,
        chunk: PnlCorrectionChunk,
        checkpoint: PnlOpeningCheckpointWrite | None,
    ) -> None:
        """Validate and persist a staged synthetic opening checkpoint inside a savepoint."""
        if checkpoint is None:
            return
        refusal = checkpoint_write_refusal(frontier, chunk, checkpoint.certificate)
        if refusal is not None:
            raise PnlCheckpointWriteRefusalError(refusal)
        try:
            self._upsert_synthetic_checkpoint(key, checkpoint)
        except SQLAlchemyError:
            self._last_pnl_checkpoint_write_refusal = "capture_upsert_failed"

    def _stage_chunk_edits(
        self,
        scope: PortfolioPnlSampleScope,
        chunk: PnlCorrectionChunk,
    ) -> tuple[
        dict[tuple[str, str, str, datetime], PnlCorrectionSampleVersion],
        int,
        list[PortfolioPnlSampleRow],
    ]:
        """Validate and apply chunk edits onto a copy of synthetic samples."""
        staged = self._samples.copy()
        row_id = self._next_row_id
        inserted: list[PortfolioPnlSampleRow] = []
        for edit in chunk.edits:
            if edit.point_time in self.conflict_minutes or (
                self.conflict_batch and edit.expected_row is None
            ):
                raise PortfolioPnlCorrectionConflictError("synthetic sample CAS conflict")
            sample_key = (scope.wallet_public_id, scope.mode, scope.valuation_ccy, edit.point_time)
            previous = edit.expected_row
            if staged.get(sample_key) != previous:
                raise PortfolioPnlCorrectionConflictError("synthetic stale physical row")
            replacement = edit.replacement
            if replacement is None:
                del staged[sample_key]
                continue
            row = replacement.copy()
            if previous is None:
                inserted.append(row)
            else:
                row["public_id"] = previous.row["public_id"]
                row["timestamp"] = max(row["timestamp"], previous.row["timestamp"])
            staged[sample_key] = PnlCorrectionSampleVersion(row_id, row)
            row_id += 1
        return staged, row_id, inserted

    async def commit_portfolio_pnl_correction_chunk(
        self,
        scope: PortfolioPnlSampleScope,
        expected: PnlCorrectionFrontier,
        chunk: PnlCorrectionChunk,
        checkpoint: PnlOpeningCheckpointWrite | None = None,
    ) -> PnlCorrectionFrontier:
        """Publish exact-version sample edits and their cursor together."""
        self.attempted_chunks.append(chunk)
        self._last_pnl_checkpoint_write_refusal = None
        key = self._correction_key(scope)
        evidence = self._evidence(scope, chunk.evidence.start, chunk.end)
        if (
            self._frontiers.get(key) != expected
            or evidence != chunk.evidence
            or evidence.prior_peak != expected.prior_peak
        ):
            raise PortfolioPnlCorrectionConflictError("synthetic stale chunk evidence")
        frontier = advance_correction_frontier(expected, chunk)
        staged, row_id, inserted = self._stage_chunk_edits(scope, chunk)
        peak = self._peak_before(scope, list(staged.values()), chunk.end + timedelta(minutes=1))
        if peak != frontier.prior_peak:
            raise PortfolioPnlCorrectionConflictError("synthetic resulting peak disagreement")
        self._record_synthetic_checkpoint(key, frontier, chunk, checkpoint)
        self._samples = staged
        self._next_row_id = row_id
        self._frontiers[key] = frontier
        self.committed_chunks.append(chunk)
        if inserted:
            self.recorded.append(inserted)
        return frontier

    async def get_portfolio_pnl_opening_checkpoint(
        self, scope: PortfolioPnlSampleScope
    ) -> PnlOpeningCheckpointRecord | None:
        """Return any stored synthetic opening checkpoint for ``scope``."""
        return self._opening_checkpoints.get(self._correction_key(scope))

    async def delete_portfolio_pnl_opening_checkpoint(
        self, scope: PortfolioPnlSampleScope, expected_generation: int
    ) -> bool:
        """Delete stored synthetic opening checkpoint when generation matches."""
        key = self._correction_key(scope)
        record = self._opening_checkpoints.get(key)
        if record is None or record.certificate.source_generation != expected_generation:
            return False
        del self._opening_checkpoints[key]
        return True

    async def get_pnl_accrual_prefix_fingerprint(
        self,
        wallet_public_id: str,
        mode: str,
        after_exclusive: datetime,
        through: datetime,
        as_of: datetime,
    ) -> IdAggregate:
        """Return empty accrual aggregate for synthetic snapshotter tests."""
        del wallet_public_id, mode, after_exclusive, through, as_of
        return IdAggregate(0, 0, 0, None)

    async def get_pnl_event_fx_candle_fingerprint(
        self,
        planes: Sequence[PnlFxRatePlane],
        open_ats: Sequence[datetime],
        as_of: datetime,
    ) -> IdAggregate:
        """Return empty event-FX candle aggregate for synthetic snapshotter tests."""
        del planes, open_ats, as_of
        return IdAggregate(0, 0, 0, None)

    async def get_pnl_event_fx_candle_fingerprints_by_plane(
        self,
        planes: Sequence[PnlFxRatePlane],
        open_ats: Sequence[datetime],
        as_of: datetime,
    ) -> dict[PnlFxRatePlane, IdAggregate]:
        """Return empty per-plane event-FX aggregates for synthetic snapshotter tests."""
        del planes, open_ats, as_of
        return {}

    async def get_pnl_execution_prefix_counts(
        self,
        wallet_public_id: str,
        mode: str,
        watermarks: dict[str, int],
    ) -> dict[str, tuple[int, int]]:
        """Return zeroed per-exchange counts for synthetic snapshotter tests."""
        del wallet_public_id, mode
        return dict.fromkeys(sorted(watermarks), (0, 0))


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
        "calc_version": PNL_SAMPLE_CALC_VERSION,
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
        "candle_session_id": _SESSION,
        "candle_sequence_id": minute,
        "candle_timestamp": _minute(minute - 1),
        "candle_known_to": datetime.max.replace(tzinfo=UTC),
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

    async def fake(*args: object, **kwargs: object) -> PnlWalletSeriesResult:
        """Return the same execution prefix when the checkpoint is resumed."""
        del kwargs
        from_time, to_time = args[3], args[4]
        assert isinstance(from_time, datetime) and isinstance(to_time, datetime)
        attach = first if state["first"] else replace(first, earliest_affected_minute=None)
        state["first"] = False
        return _series(from_time, to_time, metadata=attach)

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

    def test_fx_shadow_kill_switch_defaults_off_and_requires_truthy_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Snapshot writes do not collect shadow pins without the separate opt-in."""
        monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
        parked = PortfolioPnlSnapshotter(repo=cast(Repository, _FakeRepo()), disabled=False)
        assert parked._fx_shadow_pinning_enabled is False
        monkeypatch.setenv("PNL_FX_SHADOW_PINNING_ENABLED", "true")
        enabled = PortfolioPnlSnapshotter(repo=cast(Repository, _FakeRepo()), disabled=False)
        assert enabled._fx_shadow_pinning_enabled is True

    def test_checkpoint_mode_defaults_off_and_is_read_only(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unset checkpoint mode resolves to ``off`` and exposes a read-only property.

        Given: ``PNL_SNAPSHOTTER_CHECKPOINT_MODE`` is unset in the environment.
        When: ``PortfolioPnlSnapshotter`` is constructed.
        Then: ``checkpoint_mode`` is ``"off"`` and cannot be reassigned.
        """
        monkeypatch.delenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", raising=False)
        snap = PortfolioPnlSnapshotter(repo=cast(Repository, _FakeRepo()))
        assert snap.checkpoint_mode == "off"
        attr = "checkpoint_mode"
        with pytest.raises(AttributeError):
            setattr(snap, attr, "active")

    def test_checkpoint_mode_resolves_configured_value(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Configured valid checkpoint modes activate directly and obey kill-switch narrowing.

        Given: ``PNL_SNAPSHOTTER_CHECKPOINT_MODE`` set to ``" Shadow "``.
        When: ``PortfolioPnlSnapshotter`` is constructed under default and narrowed flags.
        Then: Both configured and effective modes are ``"shadow"`` under defaults, while a
            narrowed ``CHECKPOINT_MODES_ENABLED`` forces ``checkpoint_mode`` back to ``"off"``.
        """
        monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", " Shadow ")
        enabled = PortfolioPnlSnapshotter(repo=cast(Repository, _FakeRepo()))
        assert enabled.configured_checkpoint_mode == "shadow"
        assert enabled.checkpoint_mode == "shadow"

        monkeypatch.setattr(
            pnl_snapshotter_config,
            "CHECKPOINT_MODES_ENABLED",
            frozenset({"off"}),
        )
        gated = PortfolioPnlSnapshotter(repo=cast(Repository, _FakeRepo()))
        assert gated.configured_checkpoint_mode == "shadow"
        assert gated.checkpoint_mode == "off"

    def test_checkpoint_mode_rejects_invalid_value_at_startup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unrecognised checkpoint mode fails fast at constructor startup.

        Given: ``PNL_SNAPSHOTTER_CHECKPOINT_MODE`` set to ``"bogus"``.
        When: ``PortfolioPnlSnapshotter`` is constructed.
        Then: ``ValueError`` is raised naming ``PNL_SNAPSHOTTER_CHECKPOINT_MODE``.
        """
        monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "bogus")
        with pytest.raises(ValueError, match="PNL_SNAPSHOTTER_CHECKPOINT_MODE"):
            PortfolioPnlSnapshotter(repo=cast(Repository, _FakeRepo()))

    @pytest.mark.asyncio
    async def test_shadow_env_under_kill_switch_reads_no_checkpoint_and_plans_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Shadow env under a narrowed kill switch matches off: zero checkpoint reads, one plan.

        Given: ``PNL_SNAPSHOTTER_CHECKPOINT_MODE=shadow`` with ``CHECKPOINT_MODES_ENABLED``
            narrowed to ``{"off"}``.
        When: ``_tick_once`` replays and commits a chunk against a spied repository.
        Then: ``get_portfolio_pnl_opening_checkpoint`` is unawaited, ``_plan_chunk`` runs once,
            and committed rows including ``sequence_id`` match an ``off`` run.
        """
        monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "off")
        off_repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        off_snap = _snapshotter(off_repo, _minute(3))
        with _patched_series(seq=4):
            await off_snap._tick_once()

        monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
        monkeypatch.setattr(
            pnl_snapshotter_config,
            "CHECKPOINT_MODES_ENABLED",
            frozenset({"off"}),
        )
        shadow_repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        checkpoint_read = mock.AsyncMock(wraps=shadow_repo.get_portfolio_pnl_opening_checkpoint)
        monkeypatch.setattr(shadow_repo, "get_portfolio_pnl_opening_checkpoint", checkpoint_read)
        shadow_snap = _snapshotter(shadow_repo, _minute(3))
        plan_spy = mock.AsyncMock(wraps=shadow_snap._plan_chunk)
        monkeypatch.setattr(shadow_snap, "_plan_chunk", plan_spy)

        with _patched_series(seq=4):
            await shadow_snap._tick_once()

        assert shadow_snap.checkpoint_mode == "off"
        assert shadow_snap.configured_checkpoint_mode == "shadow"
        checkpoint_read.assert_not_awaited()
        assert plan_spy.await_count == 1
        assert len(shadow_repo.committed_chunks) == 1
        ignored = frozenset({"public_id", "session_id", "timestamp"})
        off_rows = [
            {k: v for k, v in row.items() if k not in ignored} for row in off_repo.persisted
        ]
        shadow_rows = [
            {k: v for k, v in row.items() if k not in ignored} for row in shadow_repo.persisted
        ]
        assert shadow_rows == off_rows

    @pytest.mark.asyncio
    async def test_fake_repo_matches_checkpoint_read_signatures(self) -> None:
        """Keep hand-rolled fake aligned with plane-based FX and horizon-free execution counts.

        Given: A fresh ``_FakeRepo`` double satisfying ``Repository``.
        When: Keyword arguments match the updated abstract repository signatures.
        Then: ``get_pnl_event_fx_candle_fingerprint`` accepts ``planes`` and
            ``get_pnl_execution_prefix_counts`` omits ``as_of``.
        """
        repo = _FakeRepo()
        assert await repo.get_pnl_event_fx_candle_fingerprint(
            planes=[("EUR", "USD", "kraken")],
            open_ats=[],
            as_of=_T0,
        ) == IdAggregate(0, 0, 0, None)
        assert (
            await repo.get_pnl_event_fx_candle_fingerprints_by_plane(
                planes=[("EUR", "USD", "kraken")],
                open_ats=[],
                as_of=_T0,
            )
            == {}
        )
        assert await repo.get_pnl_execution_prefix_counts(
            wallet_public_id="wallet-1",
            mode="live",
            watermarks={"kraken": 2},
        ) == {"kraken": (0, 0)}


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

    @pytest.mark.asyncio
    async def test_tick_timeout_cancels_blocked_read(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The tick budget cancels a suspended repository read.

        Given: A repository read that waits indefinitely for a result.
        When: An actual zero-second asyncio budget expires.
        Then: The read receives cancellation, and the tick returns without a write.

        Args:
            monkeypatch: Test fixture for replacing the blocked read.
            caplog: Capture of the budget-exhausted diagnostic.
        """
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        snap._interval_seconds = 0
        begun = asyncio.Event()
        cancelled = asyncio.Event()
        unblock = asyncio.Event()

        async def blocking_read(as_of: datetime) -> list[WalletCredentialRow]:
            """Wait until the tick deadline cancels repository I/O."""
            del as_of
            begun.set()
            try:
                await unblock.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return []

        monkeypatch.setattr(repo, "list_active_wallet_credentials", blocking_read)
        with caplog.at_level("WARNING", logger=pnl_snapshotter.__name__):
            await asyncio.wait_for(snap._tick_once(), timeout=5)
        assert begun.is_set()
        assert cancelled.is_set()
        assert not repo.attempted_chunks
        assert not repo.persisted
        assert "replay tick exceeded its time budget" in caplog.text

    @pytest.mark.asyncio
    async def test_slow_scope_reduces_its_chunk_and_yields_to_next_wallet(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retry slow work at a smaller size without starving the next wallet.

        Given: Two eligible wallets and a budget that expires during the first.
        When: The service retries the schedule with the earlier wallet reduced.
        Then: It tries the other wallet first and resumes the slow one at half size.

        Args:
            monkeypatch: Test fixture for controlling the work in each scope.
        """
        repo = _FakeRepo(
            credentials=[_cred("kraken", wallet="wallet-a"), _cred("kraken", wallet="wallet-b")],
            anchor=_anchor(),
        )
        snap = _snapshotter(repo, _minute(5))
        snap._interval_seconds = 0
        monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 4)
        scheduled: list[tuple[str, int]] = []
        blocked = asyncio.Event()

        async def process(
            unused_repo: Repository,
            wallet: str,
            unused_venues: frozenset[str],
            unused_as_of: datetime,
            unused_mint_excluded: bool,
        ) -> bool:
            """Suspend the first scope only at its original chunk size."""
            limit = snap._replay_chunk_limit(wallet)
            scheduled.append((wallet, limit))
            if wallet == "wallet-a" and limit == 4:
                await blocked.wait()
            return False

        monkeypatch.setattr(snap, "_process_wallet", process)
        await asyncio.wait_for(snap._tick_once(), timeout=5)
        assert scheduled == [("wallet-a", 4)]
        await asyncio.wait_for(snap._tick_once(), timeout=5)
        assert scheduled == [("wallet-a", 4), ("wallet-b", 4), ("wallet-a", 2)]

    @pytest.mark.parametrize(("sqlstate", "next_limit"), [("57014", 4), ("XX000", 8)])
    @pytest.mark.asyncio
    async def test_query_deadline_reduces_the_next_attempt(
        self, monkeypatch: pytest.MonkeyPatch, sqlstate: str, next_limit: int
    ) -> None:
        """Shrink replay after PostgreSQL times out ahead of the outer tick budget.

        Given: A scope with a bounded chunk and a failed database statement.
        When: The scheduler sees query cancellation or an unrelated driver error.
        Then: Only the query timeout reduces the retry size, with no sample write.
        """
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 8)
        driver_error = RuntimeError("synthetic query failure")
        monkeypatch.setattr(driver_error, "sqlstate", sqlstate, raising=False)
        failure = DBAPIError("SELECT synthetic", {}, driver_error, False)
        monkeypatch.setattr(snap, "_process_wallet", mock.AsyncMock(side_effect=failure))
        await snap._tick_once()
        assert snap._replay_chunk_limit(_WALLET) == next_limit
        assert not repo.attempted_chunks

    @pytest.mark.asyncio
    async def test_tick_once_flushes_enabled_shadow_context(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An enabled durable tick flushes its collected shadow work afterward."""
        repo = _FakeRepo()
        snap = _snapshotter(repo, _minute(5))
        snap._fx_shadow_pinning_enabled = True
        flushed: list[object] = []

        async def valuation(unused_repo: object, unused_as_of: datetime) -> None:
            return None

        async def flush(context: object, flushed_repo: object) -> None:
            flushed.append(flushed_repo)

        monkeypatch.setattr(snap, "_tick_valuation", valuation)
        monkeypatch.setattr(pnl_snapshotter.FxShadowPinContext, "flush_bounded", flush)
        await snap._tick_once()
        assert flushed == [repo]


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
            snap._clock = _clock(_minute(8))
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
        snap._failure_logged.add((_WALLET, "catchup_conflict"))
        with _patched_series():
            await snap._tick_once()
        assert all(wallet != _WALLET for wallet, _cls in snap._failure_logged)

    @pytest.mark.asyncio
    async def test_prunes_absent_scope_state(self) -> None:
        """In-memory state for a wallet absent from discovery is pruned (B5)."""
        repo = _FakeRepo(credentials=[], anchor=None)
        snap = _snapshotter(repo, _minute(5))
        snap._no_anchor_logged.add(_WALLET)
        snap._mint_identity_logged.add(_WALLET)
        repo.seed_completed_replay(_minute(3), seq=3)
        await snap._tick_once()
        assert snap._no_anchor_logged == set()
        assert snap._mint_identity_logged == set()
        assert repo.frontier().baseline_watermarks == {"kraken": 3}


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
        assert repo.frontier().baseline_watermarks == {"kraken": 7}
        assert repo.frontier().next_minute is None
        assert repo.frontier().repair_from is None
        assert _minute(1) in repo.peak_before

    @pytest.mark.asyncio
    async def test_durable_baseline_drives_the_series(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A replacement service seeds its replay from the durable checkpoint."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        repo.seed_completed_replay(_minute(2), seq=4)
        snap = _snapshotter(repo, _minute(6))
        seen: list[dict[str, int]] = []

        async def fake(*args: object, **kwargs: object) -> PnlWalletSeriesResult:
            """Record both the detection and correction baseline."""
            options = kwargs["options"]
            assert isinstance(options, PnlSeriesReplayOptions)
            seen.append(dict(options.baseline_watermarks))
            from_time, to_time = args[3], args[4]
            assert isinstance(from_time, datetime) and isinstance(to_time, datetime)
            return _series(from_time, to_time, metadata=_meta(seq=5))

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        assert seen == [{"kraken": 4}, {"kraken": 4}]
        assert repo.frontier().baseline_watermarks == {"kraken": 5}

    @pytest.mark.asyncio
    async def test_finalized_tip_at_anchor_waits_for_a_post_anchor_minute(self) -> None:
        """Wait for the first finalized post-anchor minute.

        Given: The finalized tip equals the wallet's activation minute.
        When: The snapshotter processes the wallet.
        Then: It opens no replay and creates no checkpoint.
        """
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _T0 + pnl_snapshotter.FINALIZATION_LAG)
        with mock.patch.object(pnl_snapshotter, "build_wallet_pnl_series") as series:
            await snap._tick_once()

        series.assert_not_called()
        assert await repo.get_portfolio_pnl_correction_frontier(_sample_scope()) is None
        assert not repo.attempted_chunks

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("next_minute", "venues"), [(None, frozenset({"kraken"})), (_minute(2), None)]
    )
    async def test_advance_replay_requires_pending_minute_and_certified_venues(
        self, next_minute: datetime | None, venues: frozenset[str] | None
    ) -> None:
        """Reject a replay that cannot certify its pending window.

        Given: The checkpoint lacks its next minute or its certified venues.
        When: The scheduler attempts to advance it.
        Then: It reports a conflict before attempting any financial write.

        Args:
            next_minute: Pending minute, or absence for a completed cursor.
            venues: Certified venue set, or absence for an uncertified cursor.
        """
        repo = _FakeRepo()
        repo.seed_completed_replay(_minute(2))
        original = repo.frontier()
        snap = _snapshotter(repo, original.as_of)
        ctx = snap._build_scope_context(_WALLET, _anchor(), frozenset({"kraken"}), original.as_of)
        frontier = replace(original, next_minute=next_minute, expected_venues=venues)

        with pytest.raises(PortfolioPnlCorrectionConflictError, match="no certified pending"):
            await snap._advance_replay(cast(Repository, repo), ctx, frontier)

        assert not repo.attempted_chunks
        assert repo.frontier() == original

    @pytest.mark.asyncio
    async def test_advance_replay_rejects_missing_fill_witness_cut(self) -> None:
        """Reject a pending checkpoint whose witnesses cannot be reopened."""
        repo = _FakeRepo()
        repo.seed_completed_replay(_minute(2))
        original = repo.frontier()
        snap = _snapshotter(repo, original.as_of)
        ctx = snap._build_scope_context(_WALLET, _anchor(), frozenset({"kraken"}), original.as_of)
        frontier = replace(
            original,
            next_minute=_minute(2),
            venue_event_watermarks=None,
            fill_witness_digest=None,
        )
        with pytest.raises(PortfolioPnlCorrectionConflictError, match="no certified fill-witness"):
            await snap._advance_replay(cast(Repository, repo), ctx, frontier)
        assert not repo.attempted_chunks
        assert repo.frontier() == original

    @pytest.mark.asyncio
    async def test_uncertified_legacy_pending_cut_is_restarted_from_anchor(self) -> None:
        """Refresh a legacy checkpoint that had no durable venue witness proof."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        repo.seed_completed_replay(_minute(3))
        original = repo.frontier()
        repo._frontiers[original.key] = replace(
            original, next_minute=_minute(2), venue_event_watermarks=None, fill_witness_digest=None
        )
        snap = _snapshotter(repo, original.as_of)

        with _patched_series(seq=4):
            await snap._tick_once()

        certified = repo.frontier()
        assert certified.generation == original.generation + 1
        assert certified.window_start == _minute(1)
        assert certified.venue_event_watermarks == {"kraken:spot": 10}

    @pytest.mark.asyncio
    async def test_clock_before_frontier_defers_replay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A backwards clock leaves the durable frontier and samples intact.

        Given: A settled scope with a later recorded knowledge horizon.
        When: A replacement service ticks at an earlier instant.
        Then: It skips detection, invalidation, and publication for that scope.

        Args:
            monkeypatch: Observe whether the replay engine is called.
        """
        old = _persisted_sample(_minute(3), status="complete")
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=[old])
        repo.seed_completed_replay(_minute(3))
        frontier = repo.frontier()
        snap = _snapshotter(repo, frontier.as_of - timedelta(seconds=1))
        series = mock.AsyncMock(return_value=_series(_minute(3), _minute(3), metadata=_meta(seq=4)))
        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", series)

        await snap._tick_once()

        series.assert_not_awaited()
        assert repo.frontier() == frontier
        assert repo.persisted == [old]
        assert not repo.attempted_chunks

    @pytest.mark.parametrize("change", ["gap", "manifest"])
    @pytest.mark.asyncio
    async def test_stationary_probe_rewinds_when_certified_history_changes(
        self, monkeypatch: pytest.MonkeyPatch, change: str
    ) -> None:
        """A changed historical fact rewinds from the first sample minute.

        Given: A settled tip, the same execution sequence, and no earlier affected minute.
        When: Its stationary probe reveals a fill gap or a changed manifest.
        Then: A new generation recomputes from the first minute after activation.

        Args:
            monkeypatch: Supply the changed evidence at a stationary tip.
            change: The certified fact which changed between generations.
        """
        old = _persisted_sample(_minute(3), status="complete")
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=[old])
        repo.seed_completed_replay(_minute(3))
        frontier = repo.frontier()
        snap = _snapshotter(repo, frontier.as_of)
        windows: list[tuple[datetime, datetime]] = []

        async def fake(*args: object, **kwargs: object) -> PnlWalletSeriesResult:
            """Keep execution coordinates fixed while changing the certified evidence."""
            start, end = args[3], args[4]
            assert isinstance(start, datetime) and isinstance(end, datetime)
            windows.append((start, end))
            options = kwargs["options"]
            assert isinstance(options, PnlSeriesReplayOptions)
            assert options.verify_replay_probe is (start == end == _minute(3))
            is_gap = change == "gap"
            metadata = _meta(seq=4, has_fill_gap=is_gap)
            if change == "manifest":
                metadata = replace(metadata, execution_manifest_digest="b" * 64)
            return _series(start, end, metadata=metadata, untrusted=is_gap)

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()

        assert windows == [(_minute(3), _minute(3)), (_minute(1), _minute(3))]
        assert repo.frontier().generation == frontier.generation + 1
        assert repo.frontier().window_start == _minute(1)
        assert repo.frontier().baseline_watermarks == {"kraken": 4}
        assert repo.committed_chunks[0].evidence.start == _minute(1)

    @pytest.mark.asyncio
    async def test_resumed_chunk_uses_the_generation_venue_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A restart does not reinterpret the plan's basket denominator.

        Given: A partial replay started with two eligible spot venues.
        When: The next service discovers only one eligible venue.
        Then: Its remaining chunk uses both venues saved on the generation.
        """
        monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
        repo = _FakeRepo(credentials=[_cred("kraken"), _cred("walutomat")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(6))
        with _patched_series(seq=4):
            await snap._tick_once()
        before = repo.frontier()
        assert before.expected_venues == frozenset({"kraken", "walutomat"})
        assert before.next_minute == _minute(3)

        repo._credentials = [_cred("kraken")]
        repo.observation_scopes.clear()
        resumed = _snapshotter(repo, _minute(7))
        with _patched_series(seq=4):
            await resumed._tick_once()
        assert repo.frontier().generation == before.generation
        assert repo.frontier().expected_venues == before.expected_venues
        assert repo.frontier().next_minute is None
        assert repo.observation_scopes
        assert all(venues == ("kraken", "walutomat") for _, venues in repo.observation_scopes)

    @pytest.mark.asyncio
    async def test_frozen_manifest_drift_rewinds_without_publishing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A changed execution manifest restarts replay before publishing more samples.

        Given: A partially committed generation and a drifted frozen execution manifest.
        When: The next chunk cannot recertify the earlier manifest.
        Then: The current tick changes only the durable cursor, and the following
              tick recomputes from the first minute using the fresh manifest.

        Args:
            monkeypatch: Force a two-minute bound and inject the evidence drift.
        """
        monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(6))
        with _patched_series(seq=4):
            await snap._tick_once()
        before = repo.frontier()
        samples = repo.persisted
        assert before.next_minute == _minute(3)
        changed_digest = "b" * 64
        calls: list[tuple[datetime, datetime, str | None]] = []

        async def fake(*args: object, **kwargs: object) -> PnlWalletSeriesResult:
            """Reject the old frozen manifest, then certify the replacement."""
            start, end = args[3], args[4]
            assert isinstance(start, datetime) and isinstance(end, datetime)
            options = kwargs["options"]
            assert isinstance(options, PnlSeriesReplayOptions)
            digest = options.frozen_execution_manifest_digest
            calls.append((start, end, digest))
            if digest == _EXECUTION_MANIFEST_DIGEST:
                raise PnlFrozenReplayEvidenceChangedError("synthetic frozen manifest drift")
            metadata = replace(_meta(seq=4), execution_manifest_digest=changed_digest)
            return _series(start, end, metadata=metadata)

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()

        assert calls == [
            (_minute(3), _minute(4), _EXECUTION_MANIFEST_DIGEST),
            (_minute(4), _minute(4), None),
        ]
        assert repo.persisted == samples
        assert len(repo.committed_chunks) == len(repo.attempted_chunks) == 1
        rewind = repo.frontier()
        assert rewind.generation == before.generation + 1
        assert rewind.next_minute == _minute(1)
        assert rewind.execution_manifest_digest == changed_digest
        assert rewind.baseline_watermarks == before.baseline_watermarks

        await snap._tick_once()

        assert calls[-1] == (_minute(1), _minute(2), changed_digest)
        assert len(repo.committed_chunks) == 2
        assert repo.frontier().next_minute == _minute(3)

    @pytest.mark.asyncio
    async def test_stale_version_recomputes_from_t0_through_supersede(self) -> None:
        """A stale active tip rebuilds the entire epoch through SCD2 supersedes."""
        stale_rows = [_persisted_sample(_minute(offset), status="complete") for offset in (1, 2)]
        for row in stale_rows:
            row["calc_version"] = "5B.1"
        repo = _FakeRepo(
            credentials=[_cred("kraken")],
            anchor=_anchor(),
            persisted=stale_rows,
        )
        snap = _snapshotter(repo, _minute(4))
        with _patched_series(seq=7):
            await snap._tick_once()
        assert [edit.point_time for edit in repo.superseded] == [
            _minute(1),
            _minute(2),
        ]
        assert all(row["calc_version"] == PNL_SAMPLE_CALC_VERSION for row in repo.persisted)

    @pytest.mark.asyncio
    async def test_stale_transition_retains_the_foreign_epoch_sample(self) -> None:
        """A foreign row stays untouched and records a durable repair boundary."""
        stale = _persisted_sample(_minute(1), status="complete")
        stale["calc_version"] = "5B.1"
        foreign = _persisted_sample(_minute(3), status="complete")
        foreign["epoch_public_id"] = "00000000-0000-7000-8000-000000000999"
        repo = _FakeRepo(
            credentials=[_cred("kraken")],
            anchor=_anchor(),
            persisted=[stale, foreign],
        )
        snap = _snapshotter(repo, _minute(5))
        with _patched_series(seq=7):
            await snap._tick_once()
        assert [edit.point_time for edit in repo.superseded] == [_minute(1)]
        assert foreign in repo.persisted
        assert repo.frontier().repair_from == _minute(3)

    @pytest.mark.asyncio
    async def test_5c3_recomputation_recovers_an_interior_missing_minute(self) -> None:
        """Recompute versioned rows and an interior hole through ordinary writes.

        Given: Two old 5C.3 rows separated by a formerly withheld minute,
        When: The enabled writer actually recomputes the stale epoch,
        Then: The hole is inserted and only recomputed replacements carry the current version.
        """
        stale_rows = [_persisted_sample(_minute(offset), status="complete") for offset in (1, 3)]
        for row in stale_rows:
            row["calc_version"] = "5C.3"
        repo = _FakeRepo(
            credentials=[_cred("kraken")],
            anchor=_anchor(),
            persisted=stale_rows,
        )
        snap = _snapshotter(repo, _minute(5))

        with _patched_series(seq=7):
            await snap._tick_once()

        assert [edit.point_time for edit in repo.superseded] == [_minute(1), _minute(3)]
        inserted = [row for batch in repo.recorded for row in batch]
        assert _minute(2) in {row["point_time"] for row in inserted}
        assert all(row["calc_version"] == PNL_SAMPLE_CALC_VERSION for row in inserted)
        assert all(row["calc_version"] == PNL_SAMPLE_CALC_VERSION for row in repo.persisted)
        assert all(row["calc_version"] == "5C.3" for row in stale_rows)

    @pytest.mark.asyncio
    async def test_disabled_writer_does_not_heal_or_restamp_5c3_rows(self) -> None:
        """Retain old evidence while the writer is disabled.

        Given: A stale 5C.3 sample and a disabled snapshotter,
        When: Its ordinary lifecycle starts and stops,
        Then: No insertion, supersede or historical version rewrite occurs.
        """
        stale = _persisted_sample(_minute(1), status="complete")
        stale["calc_version"] = "5C.3"
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=[stale])
        snap = PortfolioPnlSnapshotter(repo=cast(Repository, repo), disabled=True)

        await snap.start()
        await snap.stop()

        assert repo.recorded == []
        assert repo.superseded == []
        assert repo.retracted == []
        assert stale["calc_version"] == "5C.3"

    @pytest.mark.asyncio
    async def test_late_fill_recomputes_forward_under_cas(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A late fill recomputes forward and supersedes changed minutes under CAS."""
        latest = _persisted_sample(_minute(3))
        persisted = [_persisted_sample(_minute(2)), latest]
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=persisted)
        repo.seed_completed_replay(_minute(3))
        snap = _snapshotter(repo, _minute(6))
        _install_series(monkeypatch, first=_meta(seq=9, affected=_minute(2)))
        await snap._tick_once()
        assert [edit.point_time for edit in repo.superseded] == [_minute(2), _minute(3)]
        for edit in repo.superseded:
            assert edit.expected_row is not None
            assert edit.expected_row.row["public_id"] == "sample-" + edit.point_time.isoformat()
        assert repo.committed_chunks[0].evidence.start == _minute(2)
        assert repo.frontier().baseline_watermarks == {"kraken": 9}

    @pytest.mark.asyncio
    async def test_self_heal_recomputes_forward_through_tip(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A retryable incomplete minute recomputes forward through the tip (A1)."""
        latest = _persisted_sample(_minute(3))
        persisted = [_persisted_sample(_minute(2)), latest]
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=persisted)
        repo.seed_completed_replay(_minute(3))
        snap = _snapshotter(repo, _minute(5))
        _install_series(monkeypatch, first=_meta(seq=4))
        await snap._tick_once()
        assert repo.recorded == []
        healed = {edit.point_time for edit in repo.superseded}
        assert _minute(2) in healed

    @pytest.mark.asyncio
    async def test_retried_self_heal_logs_the_same_window_once(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Report an unchanged self-heal window once across retries.

        Given: A retryable incomplete minute and an admission conflict.
        When: Two consecutive ticks try to reopen its window.
        Then: Both select that minute and only the first logs the notice.

        Args:
            monkeypatch: Simulate a losing checkpoint admission.
            caplog: Capture the self-heal window notices.
        """
        incomplete = _persisted_sample(_minute(2))
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=[incomplete])
        repo.seed_completed_replay(_minute(3))
        original = repo.frontier()
        snap = _snapshotter(repo, original.as_of)
        conflict = PortfolioPnlCorrectionConflictError("stale revision")
        admission = mock.AsyncMock(side_effect=conflict)
        monkeypatch.setattr(repo, "invalidate_portfolio_pnl_correction", admission)

        with _patched_series(seq=4), caplog.at_level("INFO", logger=pnl_snapshotter.__name__):
            await snap._tick_once()
            await snap._tick_once()

        starts = [call.args[1].start for call in admission.await_args_list]
        assert starts == [incomplete["point_time"], incomplete["point_time"]]
        assert sum("self-heal window starts" in record.message for record in caplog.records) == 1
        assert repo.frontier() == original

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
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=persisted)
        repo.seed_completed_replay(_minute(3))
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
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=persisted)
        repo.seed_completed_replay(_minute(3))
        snap = _snapshotter(repo, _minute(5))
        _install_series(monkeypatch, first=_meta(seq=4))
        await snap._tick_once()
        assert repo.recorded == []
        healed = {edit.point_time for edit in repo.superseded}
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
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=[latest])
        repo.seed_completed_replay(_minute(3))
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
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(6))
        with _patched_series(seq=9):
            await snap._tick_once()
        original = repo.persisted
        _install_series(monkeypatch, first=_meta(seq=9, affected=_minute(2)))
        await snap._tick_once()
        assert len(repo.committed_chunks) == 2
        assert repo.committed_chunks[-1].edits == ()
        assert repo.persisted == original
        assert repo.superseded == []

    @pytest.mark.asyncio
    async def test_restored_minute_is_inserted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A formerly-untrusted minute the recompute restores is inserted, not discarded (P1)."""
        latest = _persisted_sample(_minute(3))
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=[latest])
        repo.seed_completed_replay(_minute(3))
        snap = _snapshotter(repo, _minute(6))
        _install_series(monkeypatch, first=_meta(seq=9, affected=_minute(2)))
        await snap._tick_once()
        inserted = {row["point_time"] for chunk in repo.recorded for row in chunk}
        assert _minute(2) in inserted
        superseded_minutes = {edit.point_time for edit in repo.superseded}
        assert superseded_minutes == {_minute(3)}

    @pytest.mark.asyncio
    async def test_recompute_insert_conflict_is_logged(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A conflicting recompute-insert logs and retries next tick (P1/B1)."""
        latest = _persisted_sample(_minute(3))
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=[latest])
        repo.conflict_batch = True
        repo.seed_completed_replay(_minute(3))
        snap = _snapshotter(repo, _minute(6))
        _install_series(monkeypatch, first=_meta(seq=9, affected=_minute(2)))
        with caplog.at_level("WARNING", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap._tick_once()
        assert not repo.committed_chunks
        assert repo.persisted == [latest]
        assert repo.frontier().next_minute == _minute(2)
        assert repo.frontier().baseline_watermarks == {"kraken": 4}
        assert any("catch-up conflicts" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failure", ["probe", "replay", "fill_witness"])
    async def test_missing_replay_metadata_holds_the_durable_baseline(
        self, monkeypatch: pytest.MonkeyPatch, failure: str
    ) -> None:
        """Missing anchor evidence defers publication and does not consume the baseline."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        repo.seed_completed_replay(_minute(2))
        snap = _snapshotter(repo, _minute(6))

        async def fake(*args: object, **kwargs: object) -> PnlWalletSeriesResult:
            """Disclose absent anchor evidence during either the probe or the replay."""
            del kwargs
            from_time, to_time = args[3], args[4]
            assert isinstance(from_time, datetime) and isinstance(to_time, datetime)
            phase = "probe" if from_time == to_time == _minute(4) else "replay"
            metadata = _meta(seq=7)
            if failure == "fill_witness":
                metadata = replace(metadata, fill_witness_cut=None)
            return _series(from_time, to_time, metadata=None if phase == failure else metadata)

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        assert repo.frontier().baseline_watermarks == {"kraken": 4}
        assert repo.frontier().next_minute == (_minute(3) if failure == "replay" else None)
        assert not repo.committed_chunks
        assert not repo.persisted

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
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=persisted)
        repo.conflict_minutes = {_minute(3)}
        repo.seed_completed_replay(_minute(3))
        snap = _snapshotter(repo, _minute(6))
        _install_series(monkeypatch, first=_meta(seq=9, affected=_minute(2)))
        with caplog.at_level("WARNING", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap._tick_once()
        assert repo.superseded == []
        assert repo.recorded == []
        assert repo.persisted == persisted
        assert repo.frontier().next_minute == _minute(2)
        assert repo.frontier().baseline_watermarks == {"kraken": 4}
        assert repo.frontier().pending_watermarks == {"kraken": 9}
        assert any("catch-up conflicts" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_conflict_warning_is_logged_once_per_streak(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A persistent supersede conflict logs once per streak, not every tick (C2)."""
        latest = _persisted_sample(_minute(3))
        persisted = [_persisted_sample(_minute(2)), latest]
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=persisted)
        repo.conflict_minutes = {_minute(2)}
        repo.seed_completed_replay(_minute(3))
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
        assert sum("catch-up conflicts" in r.message for r in caplog.records) == 1
        assert sum("scope(s) failed this tick" in r.message for r in caplog.records) == 2

    @pytest.mark.asyncio
    async def test_untrusted_recompute_retracts_the_stale_row(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A minute whose recompute falls to untrusted is retracted, not left stale (N1)."""
        latest = _persisted_sample(_minute(3), status="complete")
        persisted = [_persisted_sample(_minute(2), status="complete"), latest]
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=persisted)
        repo.seed_completed_replay(_minute(3))
        snap = _snapshotter(repo, _minute(6))

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                if from_time == _minute(2):
                    return _series(from_time, to_time, metadata=_meta(seq=9), untrusted=True)
                return _series(from_time, to_time, metadata=_meta(seq=9, affected=_minute(2)))

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        assert [edit.point_time for edit in repo.retracted] == [_minute(2), _minute(3)]
        assert repo.persisted == []
        assert repo.frontier().repair_from == _minute(2)
        assert repo.frontier().baseline_watermarks == {"kraken": 4}
        assert repo.superseded == []

    @pytest.mark.asyncio
    async def test_unrepresentable_recompute_keeps_the_persisted_row(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A sum no float can express must not cost a published minute its row.

        The amounts agree, but cannot be assigned to floating-point buckets.
        Keep the published sample while the durable repair remains pending.
        """
        latest = _persisted_sample(_minute(3), status="complete")
        persisted = [_persisted_sample(_minute(2), status="complete"), latest]
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=persisted)
        repo.seed_completed_replay(_minute(3))
        snap = _snapshotter(repo, _minute(6))

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                if from_time == _minute(2):
                    return _series(from_time, to_time, metadata=_meta(seq=9), unrepresentable=True)
                return _series(from_time, to_time, metadata=_meta(seq=9, affected=_minute(2)))

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        assert repo.retracted == []
        assert repo.persisted == persisted
        assert repo.frontier().repair_from == _minute(2)
        assert repo.frontier().baseline_watermarks == {"kraken": 4}

    @pytest.mark.asyncio
    async def test_retract_cas_conflict_is_logged_and_skipped(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A lost retract CAS logs and leaves the minute for a clean retry (N1)."""
        latest = _persisted_sample(_minute(3), status="complete")
        persisted = [_persisted_sample(_minute(2), status="complete"), latest]
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=persisted)
        repo.conflict_minutes = {_minute(2)}
        repo.seed_completed_replay(_minute(3))
        snap = _snapshotter(repo, _minute(6))

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                if from_time == _minute(2):
                    return _series(from_time, to_time, metadata=_meta(seq=9), untrusted=True)
                return _series(from_time, to_time, metadata=_meta(seq=9, affected=_minute(2)))

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        with caplog.at_level("WARNING", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap._tick_once()
        assert not repo.retracted
        assert repo.persisted == persisted
        assert repo.frontier().next_minute == _minute(2)
        assert repo.frontier().baseline_watermarks == {"kraken": 4}
        assert any("catch-up conflicts" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_catchup_stops_at_the_first_chunk_conflict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed chunk leaves its cursor at the earliest uncommitted minute."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        repo.conflict_batch = True
        snap = _snapshotter(repo, _minute(5))
        monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 1)
        with _patched_series(seq=7):
            await snap._tick_once()
        assert [chunk.end for chunk in repo.attempted_chunks] == [_minute(1)]
        assert not repo.committed_chunks
        assert repo.frontier().next_minute == _minute(1)
        assert not repo.frontier().baseline_watermarks

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
        assert repo.persisted == []
        assert repo.frontier().next_minute == _minute(1)
        assert not repo.frontier().baseline_watermarks
        assert any("catch-up conflicts" in r.message for r in caplog.records)


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
        assert persisted == [_minute(1)]
        assert repo.frontier().next_minute == _minute(2)
        assert not repo.frontier().baseline_watermarks
        await snap._tick_once()
        assert [chunk.end for chunk in repo.committed_chunks] == [_minute(1), _minute(3)]
        assert repo.frontier().next_minute is None

    @pytest.mark.asyncio
    async def test_recompute_forward_subdivides_on_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A late-fill recompute window that trips the budget is subdivided."""
        persisted = [_persisted_sample(_minute(offset)) for offset in (1, 2, 3, 4, 5)]
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor(), persisted=persisted)
        repo.seed_completed_replay(_minute(5))
        snap = _snapshotter(repo, _minute(8))
        state = {"first": True}

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]
            attach = _meta(seq=9, affected=_minute(1) if state["first"] else None)
            state["first"] = False

            async def _run() -> PnlWalletSeriesResult:
                if from_time == _minute(1) and to_time == _minute(6):
                    raise PnlTimelineWorkBudgetError("recompute too big")
                return _series(from_time, to_time, metadata=attach)

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        assert [edit.point_time for edit in repo.superseded] == [_minute(i) for i in (1, 2, 3)]
        assert repo.frontier().next_minute == _minute(4)
        assert repo.frontier().baseline_watermarks == {"kraken": 4}
        await snap._tick_once()
        assert [chunk.end for chunk in repo.committed_chunks] == [_minute(3), _minute(6)]
        assert repo.frontier().baseline_watermarks == {"kraken": 9}

    def test_watermarks_json_serializes_canonically(self) -> None:
        """A sample records its own causal watermark rather than the chunk envelope."""
        minute = _minute(1)
        metadata = _meta(seq=3)
        assert _watermarks_json(metadata, minute) == '{"kraken":3}'
        pointwise = replace(metadata, sample_watermarks_by_minute={minute: {"kraken": 2}})
        assert _watermarks_json(pointwise, minute) == '{"kraken":2}'
        assert _watermarks_json(None, minute) == "{}"

    def test_sample_values_equal_ignores_provenance(self) -> None:
        """Value equality ignores the write provenance identity columns."""
        left = _persisted_sample(_minute(1))
        right = {**left, "public_id": "other", "timestamp": _minute(9)}
        assert pnl_correction_planner._same_sample(left, cast(PortfolioPnlSampleRow, right)) is True
        changed = {**left, "realized_pnl": 1.0}
        assert (
            pnl_correction_planner._same_sample(left, cast(PortfolioPnlSampleRow, changed)) is False
        )

    def test_observed_currencies_collects_non_usd(self) -> None:
        """Observed non-USD currencies are collected for the crypto load."""
        assert _observed_currencies(_attempt(_minute(1))) == {"BTC"}

    @pytest.mark.parametrize(
        ("positions_json", "expected"),
        [
            ('[{"symbol":"BTC-EUR"}]', {"BTC", "EUR"}),
            ('[{"symbol":"BTC-USD"}]', {"BTC"}),
            ("not-json", {"BTC"}),
            ('{"symbol":"BTC-EUR"}', {"BTC"}),
            ('[1,{"symbol":2},{"symbol":"bad"}]', {"BTC"}),
        ],
    )
    def test_observed_currencies_collects_only_canonical_position_quotes(
        self, positions_json: str, expected: set[str]
    ) -> None:
        """Position quotes extend evidence currencies without certifying bad JSON."""
        attempt = _attempt(_minute(1))
        attempt["position_status"] = "observed"
        attempt["open_positions_json"] = positions_json
        assert _observed_currencies(attempt) == expected

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

    @pytest.mark.parametrize("sqlstate", ["57014", "55P03", "XX000"])
    @pytest.mark.parametrize("recompute", [False, True])
    async def test_crypto_query_failure_preserves_rows_and_baseline(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        sqlstate: str,
        recompute: bool,
    ) -> None:
        """A failed evidence read cannot publish catch-up or advance its detector.

        Given either a fresh catch-up or a detected late fill over an older sample
        When the crypto evidence statement is cancelled, times out on a lock or otherwise fails
        Then durable rows and baselines stay unchanged and each tick reports failure,
            and only the two budget SQLSTATEs log the once-per-streak timeout diagnostic.

        Args:
            monkeypatch: Isolate the repository read failure.
            caplog: Capture scope and chunk diagnostics.
            sqlstate: PostgreSQL statement timeout, lock timeout or unrelated failure.
            recompute: Exercise suffix correction before catch-up publication.
        """

        class DriverError(Exception):
            """Represent a driver-boundary PostgreSQL error."""

            sqlstate: str

        driver_error = DriverError("synthetic database failure")
        driver_error.sqlstate = sqlstate
        failure = DBAPIError("SELECT synthetic", {}, driver_error, False)
        old = _persisted_sample(_minute(3))
        old["calc_version"] = "previous-calculation"
        repo = _FakeRepo(
            credentials=[_cred("kraken")],
            anchor=_anchor(),
            persisted=[old] if recompute else [],
            balances_json='[{"currency":"BTC","total":2.0}]',
        )
        if recompute:
            repo.seed_completed_replay(_minute(3))
        before = repo.persisted
        snap = _snapshotter(repo, _minute(7))
        monkeypatch.setattr(
            repo, "get_pnl_crypto_usd_plane_candles", mock.AsyncMock(side_effect=failure)
        )
        _install_series(monkeypatch, first=_meta(seq=7, affected=_minute(1) if recompute else None))
        with caplog.at_level("WARNING", logger=pnl_snapshotter.__name__):
            await snap._tick_once()
            first_attempt = repo.frontier()
            await snap._tick_once()
        assert repo.persisted == before
        assert repo.recorded == []
        assert repo.superseded == []
        assert repo.retracted == []
        assert repo.committed_chunks == []
        assert repo.frontier() == first_attempt
        assert first_attempt.next_minute == _minute(1)
        assert first_attempt.baseline_watermarks == ({"kraken": 4} if recompute else {})
        assert first_attempt.pending_watermarks == {"kraken": 7}
        assert sum("scope(s) failed this tick" in record.message for record in caplog.records) == 2
        timeouts = [
            record.message
            for record in caplog.records
            if "crypto-plane query timed out" in record.message
        ]
        assert len(timeouts) == (1 if sqlstate in {"57014", "55P03"} else 0)
        if timeouts:
            assert _WALLET in timeouts[0]
            assert str(_minute(1)) in timeouts[0]
            assert str(_minute(5)) in timeouts[0]

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


class TestVersionRestamping:
    """A row the current algorithm reproduces must not stay on an older version."""

    def test_matching_values_on_an_older_version_are_not_settled(self) -> None:
        """Otherwise every read filtering on calc_version stops seeing the row.

        The current overlay filters by calculation version and would omit it
        even when its values are equal to the new recomputation.
        """
        existing = _persisted_sample(_minute(1), status="complete")
        existing["calc_version"] = "5B.1"
        replacement = _persisted_sample(_minute(1), status="complete")
        assert not pnl_correction_planner._same_sample(existing, replacement)

    def test_matching_values_on_the_same_version_are_settled(self) -> None:
        """The ordinary case: an identical recompute writes nothing."""
        existing = _persisted_sample(_minute(1), status="complete")
        replacement = _persisted_sample(_minute(1), status="complete")
        assert pnl_correction_planner._same_sample(existing, replacement)

    def test_changed_values_are_never_settled(self) -> None:
        """A real value change supersedes regardless of version agreement."""
        existing = _persisted_sample(_minute(1), status="complete", realized=1.0)
        replacement = _persisted_sample(_minute(1), status="complete", realized=2.0)
        assert not pnl_correction_planner._same_sample(existing, replacement)


class TestRowPreservingWithholds:
    """Which withhold causes may keep an already-persisted minute's row."""

    def test_only_representation_limits_preserve_a_row(self) -> None:
        """A sum no float can express is the only cause that spares the row."""
        preserved = pnl_correction_planner.preserves_persisted_row(
            WithheldMinute(
                point_time=_minute(1),
                reasons=("instrument_sum_unrepresentable[global]",),
                reason_codes=("instrument_sum_unrepresentable",),
            )
        )
        assert preserved

    def test_a_disagreement_about_the_money_still_retracts(self) -> None:
        """A reconciliation failure means the two sides disagree; N1 must fire."""
        preserved = pnl_correction_planner.preserves_persisted_row(
            WithheldMinute(
                point_time=_minute(1),
                reasons=("instrument_reconciliation_failed[global]",),
                reason_codes=("instrument_reconciliation_failed",),
            )
        )
        assert not preserved

    def test_a_mixed_cause_set_still_retracts(self) -> None:
        """One representable-sum cause does not neutralise a co-occurring fault."""
        preserved = pnl_correction_planner.preserves_persisted_row(
            WithheldMinute(
                point_time=_minute(1),
                reasons=("fill_evidence_gap[global]", "instrument_sum_unrepresentable[global]"),
                reason_codes=("fill_evidence_gap", "instrument_sum_unrepresentable"),
            )
        )
        assert not preserved

    def test_an_unexplained_withhold_still_retracts(self) -> None:
        """No recorded cause is not evidence that the money is sound."""
        preserved = pnl_correction_planner.preserves_persisted_row(
            WithheldMinute(point_time=_minute(1), reasons=(), reason_codes=())
        )
        assert not preserved


class TestWithheldMinuteAlarm:
    """The alarm for a scope that plans minutes and can value none of them.

    This is the signal whose absence let a live wallet stay dark for twenty-two
    hours: a withheld minute persists nothing, the catch-up loop skips an empty
    row list with ``continue``, and no failure class covers "the window was open
    and I committed nothing". The outage was noticed on a chart.
    """

    @pytest.mark.asyncio
    async def test_a_fully_withheld_tick_names_count_earliest_and_reason(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A tick that writes nothing says so, with the causal reason attached."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                return _series(from_time, to_time, metadata=_meta(), untrusted=True)

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        with caplog.at_level("WARNING", logger="snapper.application.portfolio.pnl_snapshotter"):
            await snap._tick_once()
        assert repo.recorded == []
        withheld = [r for r in caplog.records if "withheld" in r.message]
        assert len(withheld) == 1
        assert "fill_evidence_gap[global]" in withheld[0].getMessage()
        assert "wrote_rows=False" in withheld[0].getMessage()

    def test_an_unchanged_cause_stays_quiet_across_ticks(self) -> None:
        """A growing window under one unchanged cause is ONE incident, not many.

        The streak deliberately keys on (earliest hole, reason set) rather than on
        the count, because the count grows every tick while the scope falls
        behind. Keying on it would re-alarm forever and train the operator to
        ignore the line.
        """
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        holes = (
            WithheldMinute(
                point_time=_minute(1),
                reasons=("a[global]",),
                reason_codes=("instrument_reconciliation_failed",),
            ),
        )
        more = (
            WithheldMinute(
                point_time=_minute(1),
                reasons=("a[global]",),
                reason_codes=("instrument_reconciliation_failed",),
            ),
            WithheldMinute(
                point_time=_minute(2),
                reasons=("a[global]",),
                reason_codes=("instrument_reconciliation_failed",),
            ),
        )
        with mock.patch.object(pnl_snapshotter.logger, "warning") as warn:
            snap._report_withheld(_WALLET, holes, False)
            snap._report_withheld(_WALLET, more, False)
        assert warn.call_count == 1

    def test_a_new_cause_speaks_even_mid_streak(self) -> None:
        """A new reason is a new incident and must not be swallowed by the streak."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        first = (
            WithheldMinute(
                point_time=_minute(1),
                reasons=("a[global]",),
                reason_codes=("instrument_reconciliation_failed",),
            ),
        )
        second = (
            WithheldMinute(
                point_time=_minute(1),
                reasons=("b[global]",),
                reason_codes=("instrument_reconciliation_failed",),
            ),
        )
        with mock.patch.object(pnl_snapshotter.logger, "warning") as warn:
            snap._report_withheld(_WALLET, first, False)
            snap._report_withheld(_WALLET, second, False)
        assert warn.call_count == 2

    def test_a_clean_committed_tick_rearms_the_alarm(self) -> None:
        """Recovery clears the streak so a RECURRENCE alarms again.

        Without this a scope that recovers and relapses stays silent forever,
        because the signature it relapses with was already logged.
        """
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        holes = (
            WithheldMinute(
                point_time=_minute(1),
                reasons=("a[global]",),
                reason_codes=("instrument_reconciliation_failed",),
            ),
        )
        with mock.patch.object(pnl_snapshotter.logger, "warning") as warn:
            snap._report_withheld(_WALLET, holes, False)
            snap._report_withheld(_WALLET, (), True)
            snap._report_withheld(_WALLET, holes, False)
        assert warn.call_count == 2

    def test_a_quiet_scope_with_nothing_to_plan_never_alarms(self) -> None:
        """No planned minutes is not a hole — it is an idle scope."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        with mock.patch.object(pnl_snapshotter.logger, "warning") as warn:
            snap._report_withheld(_WALLET, (), False)
        assert warn.call_count == 0

    def test_a_partial_tick_reports_its_holes_and_says_it_wrote(self) -> None:
        """Committing SOME rows does not excuse the minutes that produced none."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        holes = (
            WithheldMinute(
                point_time=_minute(3),
                reasons=("a[global]",),
                reason_codes=("instrument_reconciliation_failed",),
            ),
        )
        with mock.patch.object(pnl_snapshotter.logger, "warning") as warn:
            snap._report_withheld(_WALLET, holes, True)
        assert warn.call_count == 1
        assert "wrote_rows=True" in warn.call_args.args[0] % warn.call_args.args[1:]

    def test_a_departed_wallet_drops_its_streak_state(self) -> None:
        """Scope state is pruned with the wallet, as every other log-once set is."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        holes = (
            WithheldMinute(
                point_time=_minute(1),
                reasons=("a[global]",),
                reason_codes=("instrument_reconciliation_failed",),
            ),
        )
        snap._report_withheld(_WALLET, holes, False)
        assert _WALLET in snap._withheld_logged
        snap._prune_absent_scopes(set())
        assert snap._withheld_logged == {}

    @pytest.mark.asyncio
    async def test_a_withheld_minute_holds_the_late_fill_baseline(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unwritten minute must not let the late-fill detector move past it.

        Advancing the baseline would consume executions whose correction
        still needs repair. The scan may complete while retaining the first hole.
        """
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))

        def fake(*args: Any, **kwargs: Any) -> Any:
            del kwargs
            from_time, to_time = args[3], args[4]

            async def _run() -> PnlWalletSeriesResult:
                return _series(from_time, to_time, metadata=_meta(seq=7), untrusted=True)

            return _run()

        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", fake)
        await snap._tick_once()
        assert repo.recorded == []
        assert repo.frontier().next_minute is None
        assert repo.frontier().repair_from == _minute(1)
        assert not repo.frontier().baseline_watermarks
        assert repo.frontier().pending_watermarks == {"kraken": 7}

    @pytest.mark.asyncio
    async def test_a_fully_written_tick_still_advances_the_baseline(self) -> None:
        """The hold is scoped to holes: a clean tick keeps its late-fill progress."""
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(5))
        with _patched_series():
            await snap._tick_once()
        assert repo.recorded
        assert repo.frontier().baseline_watermarks == {"kraken": 7}

    @pytest.mark.asyncio
    async def test_snapshotter_keeps_public_read_disabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify the snapshotter never opts into the public-read ``(a″)`` fallback.

        Given: A snapshotter instance and a spy replacing ``build_wallet_pnl_series``.
        When: ``_series`` evaluates a chunk window.
        Then: The forwarded ``PnlSeriesReadPolicy`` keeps ``public_read is False``.
        """
        spy = mock.AsyncMock(return_value=_series(_minute(1), _minute(3), metadata=_meta()))
        monkeypatch.setattr(pnl_snapshotter, "build_wallet_pnl_series", spy)
        repo = _FakeRepo(credentials=[_cred("kraken")], anchor=_anchor())
        snap = _snapshotter(repo, _minute(3))
        ctx = SimpleNamespace(wallet_public_id=_WALLET, as_of=_minute(5))
        await snap._series(
            cast(Repository, repo),
            cast(pnl_snapshotter._ScopeContext, ctx),
            ChunkWindow(_minute(1), _minute(3)),
            PnlSeriesReplayOptions(),
        )
        assert spy.await_args is not None
        assert spy.await_args.kwargs["policy"].public_read is False
