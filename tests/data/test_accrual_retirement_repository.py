"""Repository tests for retiring current accrual versions by SCD2."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccrualLedger
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AccrualLedgerInsertRow
from snapper.data.repository_types import AccrualLedgerRow
from snapper.data.repository_types import AccrualRetirementScope

_WALLET = "00000000-0000-7000-8000-000000000001"
_OTHER_WALLET = "00000000-0000-7000-8000-000000000002"
_INSTRUMENT = "00000000-0000-7000-8000-0000000000b1"
_OTHER_INSTRUMENT = "00000000-0000-7000-8000-0000000000b2"
_FIRST_BOUNDARY = datetime(2026, 11, 8, 20, 0, tzinfo=UTC)
_RETIRED_AT = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
_SCOPE = AccrualRetirementScope(
    wallet_public_id=_WALLET,
    instrument_public_id=_INSTRUMENT,
    mode="live",
)


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide an empty SQLite repository owned by one test."""
    created = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'accruals.db'}")
    await created.create_all()
    try:
        yield created
    finally:
        await created.engine.dispose()


async def _insert(
    repository: SQLAlchemyRepository,
    sequence: int,
    amount: float,
    scope: AccrualRetirementScope = _SCOPE,
) -> None:
    """Store one rollover accrual whose boundary follows from its sequence."""
    boundary = _FIRST_BOUNDARY + timedelta(hours=4 * sequence)
    await repository.insert_accrual(
        AccrualLedgerInsertRow(
            instrument_public_id=scope["instrument_public_id"],
            wallet_public_id=scope["wallet_public_id"],
            mode=scope["mode"],
            accrual_type="rollover",
            accrued_at=boundary,
            amount=amount,
            amount_asset="EUR",
            rate=0.0002,
            notional=3.96,
            position_quantity_at_accrual=4.07,
            exchange="kraken",
            session_id="accrual-retirement",
            sequence_id=sequence,
            timestamp=boundary,
        )
    )


async def _stored_knowledge_ends(repository: SQLAlchemyRepository) -> list[datetime]:
    """Return the knowledge end of every stored accrual version, oldest boundary first."""
    async with repository.session() as session:
        result = await session.execute(
            sa.select(AccrualLedger.known_to).order_by(AccrualLedger.accrued_at, AccrualLedger.id)
        )
        return [row.known_to for row in result.all()]


async def test_candidates_are_the_current_rows_of_one_scope_in_economic_order(
    repository: SQLAlchemyRepository,
) -> None:
    """The preview reads exactly one shard's current accruals.

    Given: Three accruals of one wallet, instrument and mode stored out of
        order, and one accrual each for another wallet, instrument and mode.
    When: The retirement candidates of the first scope are read.
    Then: Only its three rows are returned, ordered by economic time.
    """
    await _insert(repository, 3, -0.003)
    await _insert(repository, 1, -0.001)
    await _insert(repository, 2, -0.002)
    await _insert(repository, 4, -0.9, AccrualRetirementScope(**{**_SCOPE, "mode": "paper"}))
    await _insert(
        repository, 5, -0.9, AccrualRetirementScope(**{**_SCOPE, "wallet_public_id": _OTHER_WALLET})
    )
    await _insert(
        repository,
        6,
        -0.9,
        AccrualRetirementScope(**{**_SCOPE, "instrument_public_id": _OTHER_INSTRUMENT}),
    )

    candidates = await repository.get_accrual_retirement_candidates(_SCOPE)

    assert [row["amount"] for row in candidates] == [-0.001, -0.002, -0.003]
    assert {row["instrument_public_id"] for row in candidates} == {_INSTRUMENT}
    assert {row["mode"] for row in candidates} == {"live"}


async def test_retirement_closes_the_previewed_versions_and_keeps_their_history(
    repository: SQLAlchemyRepository,
) -> None:
    """Closed accruals stop counting from the closure and stay readable before it.

    Given: Two current accruals of one live shard and one of a paper sibling.
    When: The previewed live rows are retired at one instant.
    Then: Both versions close at that instant and none is deleted; recovery
        certification and the recovery proof no longer see them; the P&L
        reader still returns them for a knowledge instant before the closure
        and omits them from the closure onward; the sibling stays current.
    """
    paper = AccrualRetirementScope(**{**_SCOPE, "mode": "paper"})
    await _insert(repository, 1, -0.001)
    await _insert(repository, 2, -0.002)
    await _insert(repository, 3, -0.9, paper)
    preview = await repository.get_accrual_retirement_candidates(_SCOPE)
    assert await repository.shard_has_any_accruals(_WALLET, _INSTRUMENT, "live", _RETIRED_AT)

    closed = await repository.retire_accruals(_SCOPE, preview, _RETIRED_AT)

    assert closed == 2
    assert await _stored_knowledge_ends(repository) == [_RETIRED_AT, _RETIRED_AT, KNOWN_TO_MAX]
    assert await repository.get_accrual_retirement_candidates(_SCOPE) == []
    assert not await repository.shard_has_any_accruals(_WALLET, _INSTRUMENT, "live", _RETIRED_AT)
    assert await repository.get_all_shard_accruals(_WALLET, _INSTRUMENT, "live") == []
    before = await repository.get_accruals_for_pnl(
        _WALLET, "live", _RETIRED_AT - timedelta(microseconds=1)
    )
    after = await repository.get_accruals_for_pnl(_WALLET, "live", _RETIRED_AT)
    assert [row["amount"] for row in before] == [-0.001, -0.002]
    assert after == []
    assert len(await repository.get_accrual_retirement_candidates(paper)) == 1


async def test_retirement_refuses_a_current_row_the_preview_did_not_show(
    repository: SQLAlchemyRepository,
) -> None:
    """A row that appeared after the preview stops the write.

    Given: A preview of one current accrual and a second one stored afterwards.
    When: The writer reselects the scope under lock.
    Then: It refuses, names both sets, and leaves every version current.
    """
    await _insert(repository, 1, -0.001)
    preview = await repository.get_accrual_retirement_candidates(_SCOPE)
    await _insert(repository, 2, -0.002)

    with pytest.raises(ValueError, match="accrual versions changed after preview") as refusal:
        await repository.retire_accruals(_SCOPE, preview, _RETIRED_AT)

    assert preview[0]["public_id"] in str(refusal.value)
    assert await _stored_knowledge_ends(repository) == [KNOWN_TO_MAX, KNOWN_TO_MAX]


async def test_retirement_cannot_be_repeated_from_a_stale_preview(
    repository: SQLAlchemyRepository,
) -> None:
    """A second apply of the same preview finds nothing current and refuses.

    Given: One accrual already retired from its preview.
    When: The same preview is applied again later.
    Then: The writer refuses and the first closure instant is unchanged.
    """
    await _insert(repository, 1, -0.001)
    preview = await repository.get_accrual_retirement_candidates(_SCOPE)
    await repository.retire_accruals(_SCOPE, preview, _RETIRED_AT)

    with pytest.raises(ValueError, match="accrual versions changed after preview"):
        await repository.retire_accruals(_SCOPE, preview, _RETIRED_AT + timedelta(hours=1))

    assert await _stored_knowledge_ends(repository) == [_RETIRED_AT]


async def test_retirement_refuses_a_version_whose_charge_differs_from_the_preview(
    repository: SQLAlchemyRepository,
) -> None:
    """The preview binds the charge, not only the identity.

    Given: One current accrual and a preview row carrying a different amount.
    When: The writer compares the locked version with the preview.
    Then: It refuses although the public id matches.
    """
    await _insert(repository, 1, -0.001)
    preview = await repository.get_accrual_retirement_candidates(_SCOPE)
    altered = [AccrualLedgerRow(**{**preview[0], "amount": -0.5})]

    with pytest.raises(ValueError, match="accrual versions changed after preview"):
        await repository.retire_accruals(_SCOPE, altered, _RETIRED_AT)

    assert await _stored_knowledge_ends(repository) == [KNOWN_TO_MAX]


async def test_margin_signal_keeps_the_retired_accrual_as_durable_evidence(
    repository: SQLAlchemyRepository,
) -> None:
    """Retirement does not erase the durable evidence that margin was used.

    Given: A live rollover accrual, which the reconciliation method signal
        counts across all history by design.
    When: The accrual is retired.
    Then: The signal stays set, so retirement cannot reclassify the wallet.
    """
    await _insert(repository, 1, -0.001)
    preview = await repository.get_accrual_retirement_candidates(_SCOPE)

    await repository.retire_accruals(_SCOPE, preview, _RETIRED_AT)

    assert await repository.has_spot_margin_reconciliation_signal(
        _WALLET, "kraken", "live", _RETIRED_AT + timedelta(hours=1)
    )


async def test_retirement_rolls_back_a_count_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    """The writer refuses when its conditional update closes fewer rows.

    Given: One locked previewed version and an update reporting zero affected rows.
    When: The repository performs its guarded close.
    Then: It raises the count mismatch before any commit.
    """
    repository = object.__new__(SQLAlchemyRepository)
    stored = SimpleNamespace(
        id=7,
        public_id="00000000-0000-7000-8000-0000000000a1",
        instrument_public_id=_INSTRUMENT,
        mode="live",
        accrual_type="rollover",
        accrued_at=_FIRST_BOUNDARY,
        amount=-0.001,
        amount_asset="EUR",
        rate=0.0002,
        notional=3.96,
        position_quantity_at_accrual=4.07,
        exchange="kraken",
        timestamp=_FIRST_BOUNDARY,
        session_id="accrual-retirement",
        sequence_id=1,
    )
    selected = MagicMock()
    selected.scalars.return_value.all.return_value = [stored]
    session = MagicMock()
    session.execute = AsyncMock(side_effect=[selected, MagicMock(rowcount=0)])
    session.commit = AsyncMock()

    @asynccontextmanager
    async def mocked_session(current: SQLAlchemyRepository) -> AsyncIterator[MagicMock]:
        assert current is repository
        yield session

    monkeypatch.setattr(SQLAlchemyRepository, "session", mocked_session)
    preview = [SQLAlchemyRepository._accrual_row_to_dict(stored)]

    with pytest.raises(ValueError, match="previewed=1 closed=0"):
        await repository.retire_accruals(_SCOPE, preview, _RETIRED_AT)

    session.commit.assert_not_awaited()
