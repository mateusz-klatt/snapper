"""Exercise actual financial ownership and retained-source refusal boundaries."""

import base64
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.data.contract_accounting_capture_session as capture_session
import snapper.data.private_source_ownership_store as ownership
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicyReference
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.data.contract_accounting_capture_session import StagedAccountingObservationError
from snapper.data.contract_accounting_capture_session import capture_bound_accounting_observation
from snapper.data.contract_accounting_capture_session import require_staged_accounting_observation
from snapper.data.contract_accounting_integrity import _NativeKind
from snapper.data.contract_accounting_store import RawAccountingWriteResult
from snapper.data.contract_accounting_store import _ObservationInsert
from snapper.data.financial_account_owner_store import _require_supported_history
from snapper.data.financial_owner_policy_clock import _require_retained_row
from snapper.data.financial_owner_policy_clock import require_financial_owner_policy_close_clock
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import PrivateSourceOwnershipReceipt
from snapper.data.models import Setting
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import NativeAccountIdentityBindingError
from snapper.data.private_source_ownership_store import PrivateSourceOwnershipBindingError
from snapper.data.private_source_ownership_store import capture_private_source_ownership
from snapper.data.private_source_ownership_store import capture_recovered_private_source_ownership
from tests.data.test_financial_account_owner_store import _alias
from tests.data.test_financial_account_owner_store import _authority
from tests.data.test_private_source_ownership_store import _capture
from tests.data.test_private_source_ownership_store import _capture_input
from tests.data.test_private_source_ownership_store import _identity
from tests.data.test_private_source_ownership_store import _received_frame
from tests.helpers.contract_account_projection import (
    projection_scenario as shared_projection_scenario,
)
from tests.helpers.contract_account_projection import record_projection_cash
from tests.helpers.contract_account_projection import record_projection_position
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.contract_position_accounting import seed_contract_position_scenario
from tests.helpers.contract_position_accounting import stage_contract_position
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid

projection_scenario = shared_projection_scenario


def _policy_reference(at: datetime) -> FinancialAccountOwnerPolicyReference:
    """Supply a well-formed irrelevant policy reference without claiming configuration.

    Args:
        at: Explicit isolated policy knowledge clock.

    Returns:
        A reference that no legacy financial event can claim as an owner decision.
    """
    return FinancialAccountOwnerPolicyReference(
        public_id=native_authority_uuid(600),
        timestamp=at,
        session_id=native_authority_uuid(9000),
        sequence_id=600,
        policy_content_digest="0" * 64,
    )


@pytest.mark.parametrize("version", (True, "2", 2.0))
def test_restore_discriminator_refuses_noninteger_schema_aliases(version: object) -> None:
    """Keep immutable payload selection distinct from coerced schema aliases.

    Given: A retained native-cash discriminator uses a bool, text or float version.
    When: Startup selects the canonical financial payload family.
    Then: Strict validation refuses before any source reader is selected.

    Args:
        version: Unsupported representation of an otherwise supported schema number.
    """
    with pytest.raises(ValidationError, match="must be an integer"):
        _NativeKind.model_validate({"kind": "native_cash", "schema_version": version})


@pytest.mark.parametrize("legacy_book", (False, True))
async def test_unassigned_owner_history_uses_actual_uid_aliases_and_all_financial_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, legacy_book: bool
) -> None:
    """Distinguish a genuinely empty UID history from a supported identity with legacy books.

    Given: Real signed source evidence links the external UID to its actual wallet aliases.
    When: Initial ownership checks run without an assigned owner wallet.
    Then: Empty financial tables pass while an actual alias book refuses without a setting write.

    Args:
        tmp_path: Isolated physical source database directory.
        monkeypatch: Actual source and clock fixture lifetime.
        legacy_book: Whether a real paired legacy position and source claim exist.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        _, native = await _authority(source)
        if legacy_book:
            legacy = await seed_contract_position_scenario(source.repository, monkeypatch)
            async with source.repository.session() as session:
                await source.repository._begin_execution_insert_transaction(session)
                await stage_contract_position(session, legacy)
                await session.commit()
            alias = await _alias(source, 3)
            await _authority(alias, 501)
        key = financial_account_owner_key_from_authority(native)
        async with source.repository.session() as session:
            if legacy_book:
                with pytest.raises(
                    FinancialAccountOwnerUnavailableError, match="native_history_or_migration"
                ):
                    await _require_supported_history(session, key, source.clock.current)
            else:
                await _require_supported_history(session, key, source.clock.current)
            assert await session.scalar(select(func.count()).select_from(Setting)) == 0


async def test_policy_clock_rebinds_real_legacy_cash_and_positions_without_owner_decisions(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Keep legacy validation separate from versioned native owner decisions.

    Given: Actual source-bound cash and position events are committed with immutable guards.
    When: A reserved policy close enumerates and rebinds that retained history.
    Then: Both legacy families validate without inventing a decision for the unrelated policy.

    Args:
        projection_scenario: Real isolated admitted source and financial journal fixture.
    """
    await record_projection_position(projection_scenario)
    await record_projection_cash(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await require_financial_owner_policy_close_clock(
            session, _policy_reference(datetime.now(UTC)), datetime.now(UTC)
        )
        assert await session.scalar(select(func.count()).select_from(ContractAccountingEvent)) == 2


async def test_policy_clock_requires_transaction_and_refuses_closed_retained_envelopes(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Refuse missing transaction ownership and a contradictory retained row envelope.

    Given: A real immutable legacy cash event and an initially inactive caller session.
    When: The close-clock boundary receives no transaction or a locally closed row envelope.
    Then: Neither can be used as proof of safe retained history and durable storage stays unchanged.

    Args:
        projection_scenario: Real isolated admitted source and financial journal fixture.
    """
    await record_projection_cash(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    async with repository.session() as session:
        with pytest.raises(RuntimeError, match="requires_caller_transaction"):
            await require_financial_owner_policy_close_clock(
                session, _policy_reference(datetime.now(UTC)), datetime.now(UTC)
            )
        row = (await session.scalars(select(ContractAccountingEvent))).one()
        assert row.known_to == KNOWN_TO_MAX
        row.known_to = datetime.now(UTC) + timedelta(days=1)
        with pytest.raises(
            FinancialAccountOwnerUnavailableError, match="retained_history_unproven"
        ):
            _require_retained_row(row)
        row.known_to = KNOWN_TO_MAX
        row.canonical_payload = "{invalid-retained-financial-json"
        with (
            session.no_autoflush,
            pytest.raises(FinancialAccountOwnerUnavailableError, match="retained_history_unproven"),
        ):
            await require_financial_owner_policy_close_clock(
                session, _policy_reference(datetime.now(UTC)), datetime.now(UTC)
            )
        await session.rollback()
    async with repository.session() as session:
        assert (
            await session.scalars(select(ContractAccountingEvent.known_to))
        ).one() == KNOWN_TO_MAX


async def test_staged_capture_requires_actual_root_and_preserves_first_created_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep repeated captures from replacing actual ownership of newly staged raw.

    Given: A genuine original frame and an inactive real repository session.
    When: The caller first proves its root transaction and repeats the same raw capture there.
    Then: Missing ownership refuses and the retained first created marker survives the duplicate view.

    Args:
        tmp_path: Isolated physical source database directory.
        monkeypatch: Actual source and clock fixture lifetime.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        request = _capture_input(frame, 1001, "/fills/0")
        async with source.repository.session() as session:
            with pytest.raises(StagedAccountingObservationError, match="active raw capture"):
                require_staged_accounting_observation(session, request.stamp.public_id)
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, source.selection.wallet_public_id
            )
            first = await capture_bound_accounting_observation(session, request.raw, request.stamp)
            repeated = await capture_bound_accounting_observation(
                session, request.raw, request.stamp
            )
            assert first.created and not repeated.created
            retained = require_staged_accounting_observation(session, first.public_id)
            assert retained.created and retained.public_id == repeated.public_id
            assert retained is not first


async def test_capture_detects_real_root_transaction_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse registration after a lower capture boundary changes actual transaction ownership.

    Given: Original raw and hold writes are produced by the actual unchanged SQL capture.
    When: A boundary fault commits and replaces the caller's root before returning those facts.
    Then: The wrapper refuses the replacement and cannot register old raw in its new transaction.

    Args:
        tmp_path: Isolated physical source database directory.
        monkeypatch: Scoped real lower-capture fault wrapper and source lifetime.
    """
    original = capture_session._capture_under_fence

    async def replace_transaction(
        actual: AsyncSession, data: _ObservationInsert
    ) -> RawAccountingWriteResult:
        """Return genuine captured rows only after replacing their actual root transaction.

        Args:
            actual: Real caller session holding the wallet fence.
            data: Validated original source and insertion envelope.

        Returns:
            The actual lower writer result without a fabricated SQL result set.
        """
        result = await original(actual, data)
        await actual.commit()
        await actual.begin()
        return result

    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        request = _capture_input(frame, 1001, "/fills/0")
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, source.selection.wallet_public_id
            )
            monkeypatch.setattr(capture_session, "_capture_under_fence", replace_transaction)
            with pytest.raises(StagedAccountingObservationError, match="changed its actual"):
                await capture_bound_accounting_observation(session, request.raw, request.stamp)
            with pytest.raises(StagedAccountingObservationError, match="not captured"):
                require_staged_accounting_observation(session, request.stamp.public_id)
            assert (
                await session.execute(select(func.count()).select_from(AccountingRawReceipt))
            ).scalar_one() > 0


async def test_private_reader_reports_missing_raw_origin_and_unowned_recovery_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail closed for actual missing SQL rows and absent caller ownership.

    Given: A genuine retained receive relation and another nonexistent raw or origin identity.
    When: Readers or recovered capture attempt to use those missing prerequisites.
    Then: Exact missing-source errors survive without creating a replacement sidecar.

    Args:
        tmp_path: Isolated physical source database directory.
        monkeypatch: Actual source and clock fixture lifetime.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        request = _capture_input(frame, 1001, "/fills/0")
        raw, original = await _capture(source, request)
        missing = native_authority_uuid(9999)
        async with source.repository.session() as session:
            with pytest.raises(RuntimeError, match="caller wallet-fenced transaction"):
                await capture_recovered_private_source_ownership(
                    session, raw.public_id, missing, _identity(1200)
                )
            with pytest.raises(PrivateSourceOwnershipBindingError, match="raw_missing"):
                await ownership._raw_source(session, missing, datetime.now(UTC))
            with pytest.raises(
                PrivateSourceOwnershipBindingError, match="original_sidecar_missing"
            ):
                await capture_recovered_private_source_ownership(
                    session, raw.public_id, missing, _identity(1200)
                )
            payload = original.payload.model_copy(
                update={
                    "binding_kind": "recovery_projection",
                    "origin_sidecar_public_id": missing,
                    "origin_sidecar_content_digest": "0" * 64,
                }
            )
            with pytest.raises(
                PrivateSourceOwnershipBindingError, match="original_sidecar_missing"
            ):
                await ownership._verify_origin(session, payload, request.raw, datetime.now(UTC))


async def test_private_reader_refuses_corrupted_loaded_payload_and_raw_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse contradictory loaded mirrors while preserving actual immutable storage.

    Given: Genuine raw and ownership rows were committed through the original SDK receive.
    When: Their loaded raw digest or sidecar JSON is corrupted inside a no-autoflush caller view.
    Then: Independent binders reject each contradiction and rollback leaves the actual rows intact.

    Args:
        tmp_path: Isolated physical source database directory.
        monkeypatch: Actual source and clock fixture lifetime.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        raw, original = await _capture(source, _capture_input(frame, 1001, "/fills/0"))
        async with source.repository.session() as session:
            row = await session.scalar(
                select(AccountingRawReceipt).where(AccountingRawReceipt.public_id == raw.public_id)
            )
            sidecar = await session.scalar(
                select(PrivateSourceOwnershipReceipt).where(
                    PrivateSourceOwnershipReceipt.public_id == original.public_id
                )
            )
            assert row is not None and sidecar is not None
            row.content_digest = "0" * 64
            sidecar.canonical_payload = "{invalid-json"
            with session.no_autoflush:
                with pytest.raises(PrivateSourceOwnershipBindingError, match="actual_raw_unproven"):
                    await ownership._raw_source(session, raw.public_id, datetime.now(UTC))
                with pytest.raises(PrivateSourceOwnershipBindingError, match="payload_invalid"):
                    await ownership._verify_local_row(session, sidecar, datetime.now(UTC))
            await session.rollback()
        async with source.repository.session() as session:
            assert (
                await ownership.get_bound_private_source_ownership(
                    session, raw.public_id, datetime.now(UTC)
                )
                == original
            )


async def test_private_source_rejects_unparseable_wire_and_missing_original_key_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Translate original-source failures without promoting a public frame context.

    Given: An actual issued frame with independently committed selected-key and owner-root evidence.
    When: Original wire parsing or either independent key binding cannot be established.
    Then: Each refusal names its missing original prerequisite without inferring account ownership.

    Args:
        tmp_path: Isolated physical source database directory.
        monkeypatch: Scoped independent root-read failure and actual source lifetime.
    """
    original_reader = ownership.get_bound_native_account_identity
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        context = frame.ticket.context
        broken = frame.observation.model_copy(
            update={"material": base64.b64encode(b"\xff").decode()}
        )
        with pytest.raises(PrivateSourceOwnershipBindingError, match="original_frame_unproven"):
            ownership._verify_frame(broken, context)
        async with source.repository.session() as session:
            missing = context.model_copy(
                update={"key_proof_public_id": native_authority_uuid(9999)}
            )
            with pytest.raises(
                PrivateSourceOwnershipBindingError, match="original_key_proof_missing"
            ):
                await ownership._bound_owner(session, missing)
            calls: list[str] = []

            async def fail_root_read(
                actual: AsyncSession, public_id: str
            ) -> BoundNativeAccountIdentityProof:
                """Exercise the real first proof before faulting its separate root read.

                Args:
                    actual: Actual caller session reading immutable identity evidence.
                    public_id: Actual requested proof or independently required owner root.

                Returns:
                    The genuine first bound proof, never a fabricated SQL row.
                """
                calls.append(public_id)
                if len(calls) == 2:
                    raise NativeAccountIdentityBindingError("isolated root binding failure")
                return await original_reader(actual, public_id)

            with monkeypatch.context() as scoped:
                scoped.setattr(ownership, "get_bound_native_account_identity", fail_root_read)
                with pytest.raises(
                    PrivateSourceOwnershipBindingError, match="original_key_root_missing"
                ):
                    await ownership._bound_owner(session, context)
            assert len(calls) == 2


@pytest.mark.parametrize("recovered", (False, True))
async def test_private_source_refuses_raw_observation_earlier_than_its_actual_frame(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recovered: bool
) -> None:
    """Preserve raw evidence while refusing impossible original receive chronology.

    Given: A real issued frame and a raw observation claiming an earlier receive clock.
    When: Either original receive or recovered-child ownership is composed in its actual transaction.
    Then: The sidecar refuses impossible chronology without changing the original committed relation.

    Args:
        tmp_path: Isolated physical source database directory.
        monkeypatch: Actual source and clock fixture lifetime.
        recovered: Whether the writer names a direct original sidecar.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        _, original = await _capture(source, _capture_input(frame, 1001, "/fills/0"))
        request = _capture_input(frame, 1002, "/fills/1")
        contradictory = request.raw.model_copy(
            update={
                "observed_at": frame.ticket.context.frame_observed_at - timedelta(microseconds=1)
            }
        )
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, source.selection.wallet_public_id
            )
            raw = await capture_bound_accounting_observation(session, contradictory, request.stamp)
            reason = (
                "recovery_chronology_unproven"
                if recovered
                else "actual_receive_chronology_unproven"
            )
            with pytest.raises(PrivateSourceOwnershipBindingError, match=reason):
                if recovered:
                    await capture_recovered_private_source_ownership(
                        session, raw.public_id, original.public_id, request.identity
                    )
                else:
                    await capture_private_source_ownership(
                        session, raw.public_id, request.ticket, request.identity
                    )
            assert (
                await session.scalar(
                    select(func.count()).select_from(PrivateSourceOwnershipReceipt)
                )
                == 1
            )
