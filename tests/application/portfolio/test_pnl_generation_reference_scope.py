"""Exercise exact instrument reference proofs across different replay partitions."""

from collections.abc import Mapping
from datetime import datetime
from typing import TypedDict
from typing import Unpack

import pytest

from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineFillWitnessCut
from tests.application.portfolio import test_pnl_timeline_service as fixtures

_POLICY = PnlSeriesReadPolicy(allow_anchor_creation=False)


class _AtWatermarksOptions(TypedDict, total=False):
    """Typed optional keyword arguments for watermarked execution-prefix cuts."""

    fill_witness_cut: PnlTimelineFillWitnessCut | None
    capture_fill_witness_cut: bool


class _FixedCutRepo(fixtures.FakeRepo):
    """Serve already-certified rows from the generation's original cut."""

    async def get_pnl_timeline_execution_prefix_at_watermarks(
        self,
        wallet_public_id: str,
        mode: str,
        watermarks: Mapping[str, int],
        as_of: datetime,
        **options: Unpack[_AtWatermarksOptions],
    ) -> PnlTimelineExecutionPrefix:
        """Refuse a test call asking to change the captured coordinates."""
        fill_witness_cut = options.get("fill_witness_cut")
        capture_fill_witness_cut = options.get("capture_fill_witness_cut", False)
        if capture_fill_witness_cut and fill_witness_cut is not None:
            raise ValueError("capture_fill_witness_cut cannot be combined with fill_witness_cut")
        assert (wallet_public_id, mode) == (fixtures._W1, "live")
        assert fill_witness_cut is None
        assert not capture_fill_witness_cut
        prefix = self._execution_prefix_at(as_of)
        assert prefix["watermarks"] == watermarks
        return prefix


async def _series(
    repo: _FixedCutRepo,
    start: int,
    end: int,
    options: PnlSeriesReplayOptions,
) -> PnlWalletSeriesResult:
    """Read a physical chunk at one fixed generation knowledge horizon."""
    return await build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(start),
        fixtures._m(end),
        "1m",
        fixtures._m(5),
        policy=_POLICY,
        options=options,
    )


async def test_venue_revision_withholds_the_same_fee_in_split_and_wide_replay() -> None:
    """Withhold a fee whose source cannot be proved for its execution span.

    Given two fills with the same instrument ID on opposite sides of a venue re-key.
    When a replay values one or several chunks using the same generation tip.
    Then both withhold the earlier fee while the narrow public query retains it.
    """
    instrument = fixtures._I1
    repo = _FixedCutRepo(
        executions=[
            fixtures._exec_row(instrument, 1, 1, "buy", 1.0, 100.0, 1.0, "EUR"),
            fixtures._exec_row(instrument, 2, 3, "sell", 1.0, 110.0, 0.0, "USD"),
        ],
        refs=[
            fixtures._ref(instrument, "BTC-USD", "USD", valid_to=fixtures._m(2)),
            fixtures._ref(
                instrument,
                "XBT-USD",
                "USD",
                exchange="coinbase",
                valid_from=fixtures._m(2),
                base="BTC",
            ),
        ],
        candles=[fixtures._candle(fixtures._m(0), 100.0)],
        fx_rows=[fixtures._fx_row("EUR", "USD", 1, 1.25)],
    )
    options = PnlSeriesReplayOptions(
        frozen_execution_watermarks={"kraken": 2},
        replay_generation_tip=fixtures._m(3),
        pointwise_exact_fx=True,
    )
    wide = await _series(repo, 1, 3, options)
    first = await _series(repo, 1, 1, options)
    rest = await _series(repo, 2, 3, options)
    public = await _series(repo, 1, 1, PnlSeriesReplayOptions())

    assert first.points + rest.points == wide.points
    assert all(point.fee_pnl is None for point in wide.points)
    assert (
        "execution_price_provenance_unproven",
        "untrusted",
        "instrument",
        instrument,
    ) in fixtures._reason_rows(first.points[0])
    assert public.points[0].fee_pnl == -1.25


async def test_opening_pool_reference_proof_covers_the_generation_tip() -> None:
    """Prove opening-pool references through the finalized generation tip.

    Given an opening position whose symbol and venue change within the generation.
    When the writer values a chunk ending before the change.
    Then it withholds the same mark as a replay spanning the change.
    """
    instrument = fixtures._I1
    repo = _FixedCutRepo(
        anchor=fixtures._one_pool_anchor(),
        refs=[
            fixtures._ref(instrument, "BTC-USD", "USD", valid_to=fixtures._m(3)),
            fixtures._ref(
                instrument,
                "XBT-USD",
                "USD",
                exchange="coinbase",
                valid_from=fixtures._m(3),
                base="BTC",
            ),
        ],
        candles=[fixtures._candle(fixtures._m(1), 120.0)],
    )
    options = PnlSeriesReplayOptions(
        frozen_execution_watermarks={}, replay_generation_tip=fixtures._m(4)
    )
    wide = await _series(repo, 2, 4, options)
    first = await _series(repo, 2, 2, options)
    rest = await _series(repo, 3, 4, options)
    public = await _series(repo, 2, 2, PnlSeriesReplayOptions())

    assert first.points + rest.points == wide.points
    assert first.points[0].unrealized_pnl is None
    assert public.points[0].unrealized_pnl == 10.0


async def test_generation_display_proof_includes_later_accruals_without_applying_them() -> None:
    """Use the same display identity in each chunk while preserving the accrual clock.

    Given a round trip before a venue revision and an accrual after it.
    When replay values an earlier chunk of the same generation.
    Then display identity lacks a continuous proof and the accrual still applies later.
    """
    instrument = fixtures._I1
    repo = _FixedCutRepo(
        executions=[
            fixtures._exec_row(instrument, 1, 1, "buy", 1.0, 100.0, 0.0, "USD"),
            fixtures._exec_row(instrument, 2, 1, "sell", 1.0, 110.0, 0.0, "USD"),
        ],
        accruals=[fixtures._accrual_row(instrument, 3, 2.0, "USD")],
        refs=[
            fixtures._ref(instrument, "BTC-USD", "USD", valid_to=fixtures._m(2)),
            fixtures._ref(
                instrument,
                "XBT-USD",
                "USD",
                exchange="coinbase",
                valid_from=fixtures._m(2),
                base="BTC",
            ),
        ],
    )
    options = PnlSeriesReplayOptions(
        frozen_execution_watermarks={"kraken": 2}, replay_generation_tip=fixtures._m(3)
    )
    wide = await _series(repo, 1, 3, options)
    early = await _series(repo, 1, 1, options)
    public = await _series(repo, 1, 1, PnlSeriesReplayOptions())

    assert early.points[0] == wide.points[0]
    assert early.points[0].per_instrument[0].native_symbol is None
    assert early.points[0].accrual_pnl == 0.0
    assert wide.points[-1].accrual_pnl == -2.0
    assert public.points[0].per_instrument[0].native_symbol == "BTC-USD"


@pytest.mark.parametrize(
    ("tip", "cut", "message"),
    [
        (3, None, "require a captured raw execution cut"),
        (1, {}, "must cover the chunk"),
        (6, {}, "within its knowledge horizon"),
    ],
)
async def test_generation_reference_tip_requires_a_coherent_frozen_window(
    tip: int, cut: dict[str, int] | None, message: str
) -> None:
    """Require an exact generation reference window.

    Given a missing frozen cut or a tip outside the admissible replay horizon.
    When the writer requests one chunk of the invalid generation.
    Then the service refuses the request before reading its evidence.
    """
    options = PnlSeriesReplayOptions(
        frozen_execution_watermarks=cut,
        replay_generation_tip=fixtures._m(tip),
    )
    with pytest.raises(ValueError, match=message):
        await _series(_FixedCutRepo(), 2, 2, options)
