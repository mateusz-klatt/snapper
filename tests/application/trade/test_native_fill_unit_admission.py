"""Actual native fill/unit producer and reader against original signed wire and real admission rows."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import update

from snapper.application.trade.account_bound_submission_admission import (
    capture_account_submission_admission,
)
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_fill_unit_admission import NativeFillUnitAdmissionEvidence
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference
from snapper.data.account_bound_submission_admission_store import (
    get_bound_account_submission_admission,
)
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from tests.helpers.native_cash_accounting_v2 import fresh_native_cash_authority_v2
from tests.helpers.native_fill_unit_admission import NativeFillSide
from tests.helpers.native_fill_unit_admission import NativeProfileChange
from tests.helpers.native_fill_unit_admission import capture_native_fill_unit
from tests.helpers.native_fill_unit_admission import native_fill_unit_scenario
from tests.helpers.native_fill_unit_admission import read_native_fill_unit
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_identity


@pytest.mark.parametrize("side", ["buy", "sell"])
async def test_actual_original_side_and_exact_decimals_survive_unit_admission_and_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, side: NativeFillSide
) -> None:
    """The new producer and reader retain actual signed direction without fabricating an original unit.

    Given: Real original side-specific command, authenticated fill, Execution and v3 admission,
    When: The actual new unit producer runs twice and its returned envelope is rebound,
    Then: Original signed quantity, exact fee, source bytes and first unit identity remain unchanged.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original SDK/local HTTP fixture lifetime.
        side: Direction present in the source command and encoded authenticated wire from inception.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch, side) as scenario:
        first = await capture_native_fill_unit(scenario)
        repeated = await capture_native_fill_unit(scenario)
        bound = await read_native_fill_unit(scenario, first.evidence)
        assert first.result.created and not repeated.result.created
        assert first.result.public_id == repeated.result.public_id
        assert first.result.content_digest == repeated.result.content_digest
        assert first.payload == repeated.payload == bound.unit
        assert first.evidence == repeated.evidence == bound.evidence
        assert bound.unit.dimensions.signed_quantity == Decimal(1 if side == "buy" else -1)
        assert bound.unit.dimensions.fee == Decimal("0.10000000000000001")
        assert bound.unit.receipt.material == bound.source.raw.material
        assert bound.admission == scenario.admission
        async with scenario.source.repository.session() as session:
            assert (
                await session.scalar(select(func.count()).select_from(ExecutionUnitSnapshot)) == 2
            )
            assert (
                await session.scalar(select(func.count()).select_from(ContractAccountingEvent)) == 0
            )


@pytest.mark.parametrize("side", ["buy", "sell"])
async def test_actual_historical_catalogs_remain_valid_after_all_current_rows_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, side: NativeFillSide
) -> None:
    """Historical source binding retains original catalog rows after later physical closure.

    Given: A real captured native fill unit and its original account admission,
    When: The actual catalog rows close after their original knowledge and the envelope is reread,
    Then: The historical reader returns exactly the original unit and admission without current rows.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original source fixture lifetime.
        side: Original buy or sell direction.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch, side) as scenario:
        captured = await capture_native_fill_unit(scenario)
        closed_at = datetime.now(UTC)
        async with scenario.source.repository.session() as session:
            for model in (Instrument, Symbol, InstrumentSpec):
                await session.execute(
                    update(model).where(model.known_to == KNOWN_TO_MAX).values(known_to=closed_at)
                )
            await session.commit()
        bound = await read_native_fill_unit(scenario, captured.evidence, datetime.now(UTC))
        assert bound.unit == captured.payload
        assert bound.admission == scenario.admission


@pytest.mark.parametrize("profile_change", ["unchanged", "balance", "account_type", "settlement"])
async def test_actual_same_uid_and_key_preserve_profile_structure_but_allow_changed_balances(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, profile_change: NativeProfileChange
) -> None:
    """Original native profile structure cannot be replaced by a same-name later source.

    Given: Real admission and fill authorities from signed responses using the same UID and key,
    When: Actual later profile bytes preserve structure or change type or settlement currency,
    Then: Balance-only changes bind successfully while structural profile changes are refused.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original actual signing and account-source fixture lifetime.
        profile_change: Supported actual REST response variation, without replacing its issuer.
    """
    async with native_fill_unit_scenario(
        tmp_path, monkeypatch, profile_change=profile_change
    ) as scenario:
        original = scenario.admission.authority.payload
        assert isinstance(original, NativeAccountAuthorityPayload)
        later = scenario.fill_authority
        assert original.venue_account_id == later.venue_account_id
        assert original.credential_public_id == later.credential_public_id
        assert original.scope == later.scope
        if profile_change in {"account_type", "settlement"}:
            with pytest.raises(SnapshotBindingError, match="profile|account"):
                await capture_native_fill_unit(scenario)
        else:
            captured = await capture_native_fill_unit(scenario)
            assert (
                await read_native_fill_unit(scenario, captured.evidence)
            ).unit == captured.payload
            assert (
                original.native_profile.content_digest != later.native_profile.content_digest
            ) == (profile_change == "balance")


@pytest.mark.parametrize("edge", ["submission_admission", "unit_snapshot"])
@pytest.mark.parametrize("field", ["public_id", "content_digest", "recorded_at"])
async def test_actual_retained_reference_identity_digest_and_clock_cannot_be_substituted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    edge: Literal["submission_admission", "unit_snapshot"],
    field: Literal["public_id", "content_digest", "recorded_at"],
) -> None:
    """Every original retained edge is rebound from actual rows rather than trusted as public shape.

    Given: A genuine captured unit and v3 account admission from original authenticated source,
    When: One reference identity, digest or original recording clock is changed,
    Then: The actual historical reader refuses the substituted retained edge.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original actual source fixture lifetime.
        edge: Original durable envelope reference to corrupt.
        field: Exact retained field changed without modifying its real source row.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch) as scenario:
        captured = await capture_native_fill_unit(scenario)
        reference = (
            captured.evidence.submission_admission
            if edge == "submission_admission"
            else captured.evidence.unit_snapshot
        )
        replacements: dict[str, object] = {
            "public_id": admission_id(7999),
            "content_digest": "f" * 64,
            "recorded_at": reference.recorded_at + timedelta(microseconds=1),
        }
        changed = NativeUnitRecordReference.model_validate(
            {**reference.model_dump(mode="python"), field: replacements[field]}
        )
        evidence = NativeFillUnitAdmissionEvidence.model_validate(
            {**captured.evidence.model_dump(mode="python"), edge: changed}
        )
        with pytest.raises(SnapshotBindingError):
            await read_native_fill_unit(scenario, evidence)


async def test_actual_other_command_admission_cannot_be_attached_to_this_original_fill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real earlier admission for another command cannot supply this fill's intent evidence.

    Given: Two actual durable commands and a genuine admission for the other one before the fill,
    When: The new producer receives that original but unrelated admission reference,
    Then: Exact durable command material fails before a fill-unit snapshot is inserted.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original actual source fixture lifetime.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch, wrong_command=True) as scenario:
        with pytest.raises(SnapshotBindingError, match="command_material"):
            await capture_native_fill_unit(scenario)


async def test_actual_later_admission_does_not_retroactively_cover_the_original_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A later genuine account admission cannot substitute for original execution-time knowledge.

    Given: An actual original fill and later fresh signed account source with a new real admission,
    When: The producer receives that exact later record for the earlier Execution,
    Then: It refuses admission knowledge that did not exist at the actual execution recording clock.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original actual signing and source fixture lifetime.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch) as scenario:
        reference, authority = await fresh_native_cash_authority_v2(scenario.source, 7801)
        route = ExecutionRouteReference(
            authority_public_id=reference.public_id,
            authority_content_digest=reference.content_digest,
            scope=authority.scope,
            route_revision=authority.route_revision,
        )
        result = await capture_account_submission_admission(
            scenario.source.repository,
            scenario.order,
            route,
            admission_identity(7802),
            scenario.source.policy,
        )
        async with scenario.source.repository.session() as session:
            later = await get_bound_account_submission_admission(session, result.public_id)
            execution = (
                await session.execute(
                    select(Execution).where(
                        Execution.public_id == scenario.request.execution_public_id
                    )
                )
            ).scalar_one()
        assert later.recorded_at > execution.timestamp
        request = scenario.request.model_copy(
            update={
                "submission_admission": NativeUnitRecordReference(
                    public_id=later.public_id,
                    content_digest=later.content_digest,
                    recorded_at=later.recorded_at,
                )
            }
        )
        with pytest.raises(SnapshotBindingError, match="not_known_at_execution"):
            await capture_native_fill_unit(scenario, request)


async def test_missing_actual_admission_is_refused_before_fill_unit_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A reference with no actual retained admission cannot become fill financial evidence.

    Given: Genuine original source and targets with a missing admission identity,
    When: The new producer independently resolves the named admission,
    Then: It refuses the absent actual row before creating fill-unit evidence.

    Args:
        tmp_path: Isolated actual SQLite namespace.
        monkeypatch: Original actual source fixture lifetime.
    """
    async with native_fill_unit_scenario(tmp_path, monkeypatch) as scenario:
        reference = scenario.request.submission_admission.model_copy(
            update={"public_id": admission_id(7999)}
        )
        request = scenario.request.model_copy(update={"submission_admission": reference})
        with pytest.raises(SnapshotBindingError, match="admission_missing"):
            await capture_native_fill_unit(scenario, request)
