"""Real current-route/source admission, exact retry and changed-catalog refusal."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import tzinfo
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select

import snapper.application.trade.unit_snapshot_submission as producer
from snapper.core.command_material import COMMAND_MATERIAL_CONFLICT_EVENT
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import canonical_snapshot_payload
from snapper.core.execution_unit_snapshot import snapshot_content_digest
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.execution_unit_admission_store import UnitAdmissionConflictError
from snapper.data.execution_unit_admission_store import get_bound_submission_admission
from snapper.data.execution_unit_admission_store import insert_submission_unit_admission
from snapper.data.execution_unit_catalog import UnitCatalogLookup
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import SubmissionUnitAdmission
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.models import WalletCredential
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE
from snapper.infrastructure.exchanges.tokenized_spec_provenance import TOKENIZED_SPEC_SOURCE
from tests.helpers.unit_admission import AdmissionScenario
from tests.helpers.unit_admission import AdmissionSource
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_identity
from tests.helpers.unit_admission import admission_stamp
from tests.helpers.unit_admission import refresh_admission_account
from tests.helpers.unit_admission import seed_admission_scenario

_NOW = datetime(2026, 10, 23, 6, tzinfo=UTC)


class _Clock:
    """Keep actual producer time deterministic without modifying any global datetime module."""

    moment = _NOW

    @classmethod
    def now(cls, timezone: tzinfo | None = None) -> datetime:
        """Return the explicit scenario clock in the requested timezone."""
        return cls.moment if timezone is None else cls.moment.astimezone(timezone)


@pytest.fixture
async def scenario(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> AsyncIterator[AdmissionScenario]:
    """Provide genuine isolated source, intent and account observations."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'admission.db'}")
    monkeypatch.setattr(producer, "datetime", _Clock)
    monkeypatch.setattr(_Clock, "moment", _NOW)
    try:
        await repository.create_all()
        yield await seed_admission_scenario(repository, _NOW)
    finally:
        await repository.engine.dispose()


@pytest.fixture
async def contract_scenario(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> AsyncIterator[AdmissionScenario]:
    """Provide a real file-backed contract account without recreating the full schema per case."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'contract.db'}")
    monkeypatch.setattr(producer, "datetime", _Clock)
    monkeypatch.setattr(_Clock, "moment", _NOW)
    try:
        await repository.create_all()
        yield await seed_admission_scenario(repository, _NOW, source="kraken_futures")
    finally:
        await repository.engine.dispose()


async def _counts(scenario: AdmissionScenario) -> tuple[int, int]:
    """Read both actual physical evidence counts after commit or rollback."""
    async with scenario.repository.session() as session:
        snapshots = (
            await session.execute(select(func.count()).select_from(ExecutionUnitSnapshot))
        ).scalar_one()
        admissions = (
            await session.execute(select(func.count()).select_from(SubmissionUnitAdmission))
        ).scalar_one()
        return snapshots, admissions


async def _refresh_source_spec(
    scenario: AdmissionScenario, changes: dict[str, object] | None = None
) -> None:
    """Write a real closed-and-successor specification after the original command."""
    at = scenario.now - timedelta(seconds=20)
    async with scenario.repository.session() as session:
        previous = (
            await session.execute(
                select(InstrumentSpec).where(
                    InstrumentSpec.instrument_public_id == admission_id(2),
                    InstrumentSpec.known_to == KNOWN_TO_MAX,
                )
            )
        ).scalar_one()
        fields: dict[str, object] = {
            column.key: getattr(previous, column.key)
            for column in InstrumentSpec.__table__.columns
            if column.key != "id"
        }
        previous.known_to = at
        fields.update(
            timestamp=at,
            known_to=KNOWN_TO_MAX,
            sequence_id=104,
            spec_version="fixture-etl:refresh",
            spec_observed_at=at,
        )
        fields.update(changes or {})
        session.add(InstrumentSpec(**fields))
        await session.commit()


async def test_actual_route_capture_commits_original_and_current_proof_graph(
    scenario: AdmissionScenario,
) -> None:
    """Real submitted operands and source identities survive independent storage reads.

    Given: A durable cash intent and successful current account-route observation,
    When: Its actual outgoing frame is captured before any venue I/O,
    Then: One original v1 record and one current v2 witness commit together.
    """
    result = await producer.capture_submission_unit_admission(
        scenario.repository, scenario.order, scenario.route, admission_identity()
    )
    assert result.created is True
    assert await _counts(scenario) == (1, 1)
    async with scenario.repository.session() as session:
        payload = await get_bound_submission_admission(session, result.public_id)
        row = (await session.execute(select(ExecutionUnitSnapshot))).scalar_one()
        baseline = ExecutionUnitSnapshotPayload.model_validate_json(row.canonical_payload)
    assert payload.schema_version == 2
    assert baseline.schema_version == 1
    assert payload.request_material.quantity == baseline.dimensions.signed_quantity
    assert payload.current_source.instrument_public_id == admission_id(2)
    assert payload.command_snapshot_public_id == row.public_id
    assert payload.route.authority_public_id == scenario.route.authority_public_id


async def test_exact_attempt_retry_preserves_one_identity_after_fresh_checks(
    scenario: AdmissionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """New insertion provenance cannot duplicate an already witnessed actual attempt.

    Given: A retained attempt whose current context is unchanged,
    When: It is recaptured with a later clock and another insertion UUID,
    Then: The exact original identity/content are returned without another record.
    """
    identity = admission_identity()
    first = await producer.capture_submission_unit_admission(
        scenario.repository, scenario.order, scenario.route, identity
    )
    monkeypatch.setattr(_Clock, "moment", _NOW + timedelta(seconds=1))
    retry = identity.model_copy(
        update={
            "admission": ExecutionEvidenceIdentity(
                public_id=admission_id(1196), session_id=admission_id(99), sequence_id=1196
            )
        }
    )
    second = await producer.capture_submission_unit_admission(
        scenario.repository, scenario.order, scenario.route, retry
    )
    assert (first.id, first.public_id, first.content_digest) == (
        second.id,
        second.public_id,
        second.content_digest,
    )
    assert second.created is False
    assert await _counts(scenario) == (1, 1)


async def test_benign_actual_catalog_refresh_is_recorded_without_rewriting_original(
    scenario: AdmissionScenario,
) -> None:
    """A metadata refresh with identical economics does not make old intents permanently stale.

    Given: A real specification successor authored after the original intent,
    When: Its economic dimensions are unchanged and admission is captured,
    Then: The original snapshot pins the old revision and v2 pins the actual current one.
    """
    await _refresh_source_spec(scenario)
    result = await producer.capture_submission_unit_admission(
        scenario.repository, scenario.order, scenario.route, admission_identity()
    )
    async with scenario.repository.session() as session:
        payload = await get_bound_submission_admission(session, result.public_id)
        row = (await session.execute(select(ExecutionUnitSnapshot))).scalar_one()
        baseline = ExecutionUnitSnapshotPayload.model_validate_json(row.canonical_payload)
    assert baseline.catalog.version == "fixture-etl:original"
    assert payload.current_catalog.version == "fixture-etl:refresh"
    assert payload.command_source.catalog.version == "fixture-etl:original"
    assert payload.current_source.catalog.version == "fixture-etl:refresh"


@pytest.mark.parametrize(
    "changes",
    (
        {"lot_size": 0.001},
        {"tick_size": 1.0},
        {"native_order_quantity_unit": "contract_count"},
        {"quantity_unit": "contract_count"},
    ),
)
async def test_changed_units_or_quantum_refuse_before_any_evidence_commit(
    scenario: AdmissionScenario, changes: dict[str, object]
) -> None:
    """The same displayed market cannot silently reinterpret an already authored intent.

    Given: A current specification whose economic fingerprint differs,
    When: A queued old request reaches actual admission,
    Then: CREATE is held and neither a fabricated baseline nor witness commits.
    """
    await _refresh_source_spec(scenario, changes)
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, scenario.route, admission_identity()
        )
    assert "admission_economic_context_changed" in str(raised.value.__cause__)
    assert await _counts(scenario) == (0, 0)


@pytest.mark.parametrize(
    ("changes", "reason"),
    (
        ({"status": "inactive"}, "not_active"),
        ({"min_order_size": 3.0}, "minimum_quantity"),
        ({"max_order_size": 1.0}, "maximum_quantity"),
    ),
)
async def test_refreshed_admission_limits_apply_even_when_unit_fingerprint_matches(
    scenario: AdmissionScenario, changes: dict[str, object], reason: str
) -> None:
    """Unit compatibility does not bypass an actual current restriction.

    Given: Unchanged units but an inactive instrument or new size limit,
    When: An old frame reaches the current admission boundary,
    Then: The actual restriction is honored independently of the economic fingerprint.
    """
    await _refresh_source_spec(scenario, changes)
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, scenario.route, admission_identity()
        )
    assert reason in str(raised.value.__cause__)
    assert await _counts(scenario) == (0, 0)


async def test_retry_cannot_reuse_old_current_catalog_after_benign_refresh(
    scenario: AdmissionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An actual new interpretation needs a fresh attempt witness even with equal economics.

    Given: An already witnessed actual attempt and a later catalog refresh,
    When: The old attempt is reused and then a separately identified attempt is captured,
    Then: Old-evidence reuse is held while the new witness preserves the original baseline.
    """
    await producer.capture_submission_unit_admission(
        scenario.repository, scenario.order, scenario.route, admission_identity()
    )
    await _refresh_source_spec(scenario)
    monkeypatch.setattr(_Clock, "moment", _NOW + timedelta(seconds=1))
    with pytest.raises(
        producer.SubmissionUnitSnapshotUnavailableError,
        match="submission_unit_snapshot_unavailable",
    ):
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, scenario.route, admission_identity()
        )
    new = await producer.capture_submission_unit_admission(
        scenario.repository, scenario.order, scenario.route, admission_identity(97)
    )
    assert new.created is True
    assert await _counts(scenario) == (1, 2)


@pytest.mark.parametrize(
    "status",
    ("accepted", "partially_filled", "filled", "cancelled", "rejected", "expired", "failed"),
)
async def test_retained_attempt_never_bypasses_current_command_disposition(
    scenario: AdmissionScenario, status: str
) -> None:
    """A stored witness is evidence rather than a reusable order-creation permission.

    Given: A previously witnessed command later accepted or terminalized,
    When: Its old frame and attempt are recaptured,
    Then: The changed lifecycle holds CREATE and preserves existing evidence unchanged.
    """
    await producer.capture_submission_unit_admission(
        scenario.repository, scenario.order, scenario.route, admission_identity()
    )
    async with scenario.repository.session() as session:
        command = (await session.execute(select(TradeCommand))).scalar_one()
        command.status = status
        await session.commit()
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, scenario.route, admission_identity()
        )
    assert "not_placeable" in str(raised.value.__cause__)
    assert await _counts(scenario) == (1, 1)


async def test_expired_route_is_readable_history_and_cannot_admit_a_new_create(
    scenario: AdmissionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Account-response retention does not extend live financial authority.

    Given: A valid route whose bounded account observation has expired,
    When: Admission is attempted and the historical authority is read separately,
    Then: CREATE is held while the original immutable evidence remains available.
    """
    monkeypatch.setattr(_Clock, "moment", _NOW + timedelta(minutes=3))
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, scenario.route, admission_identity()
        )
    assert "not_current" in str(raised.value.__cause__)
    async with scenario.repository.session() as session:
        retained = await get_bound_route_authority(session, scenario.route.authority_public_id)
    assert retained == scenario.authority
    assert await _counts(scenario) == (0, 0)


async def test_credential_rotation_does_not_reuse_the_prior_actual_client_authority(
    scenario: AdmissionScenario,
) -> None:
    """Credential changes require a new actual-route observation, not a fresh cash namespace.

    Given: The selected credential has a real SCD2 successor,
    When: The old executor authority is reused for a new admission,
    Then: Current-route proof fails before any snapshot or attempt is committed.
    """
    at = _NOW - timedelta(seconds=1)
    async with scenario.repository.session() as session:
        previous = (await session.execute(select(WalletCredential))).scalar_one()
        previous.known_to = at
        session.add(
            WalletCredential(
                **admission_stamp(190, at).model_dump(mode="python"),
                wallet_public_id=admission_id(3),
                exchange="kraken",
                credential_type="api_key_secret",
                encrypted_payload="isolated-successor-never-decrypted",
            )
        )
        await session.commit()
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, scenario.route, admission_identity()
        )
    assert "credential_revision_changed" in str(raised.value.__cause__)
    assert await _counts(scenario) == (0, 0)


async def test_retained_material_conflict_marker_is_independent_of_watermarks(
    scenario: AdmissionScenario,
) -> None:
    """A cursor advance cannot hide the exact command's durable refusal.

    Given: Consistent-looking current fields and an older durable conflict marker,
    When: The actual submission transaction rechecks the proof graph,
    Then: It honors the marker without treating it as a venue rejection.
    """
    async with scenario.repository.session() as session:
        session.add(
            VenueEvent(
                **admission_stamp(300, _NOW - timedelta(seconds=2)).model_dump(mode="python"),
                event_type=COMMAND_MATERIAL_CONFLICT_EVENT,
                command_public_id=admission_id(5),
                shard_key="kraken.BTC-USD.live",
                wallet_public_id=admission_id(3),
                exchange="kraken",
                instrument="BTC-USD",
                mode="live",
                received_at=_NOW - timedelta(seconds=2),
            )
        )
        await session.commit()
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, scenario.route, admission_identity()
        )
    assert "material_barrier" in str(raised.value.__cause__)
    assert await _counts(scenario) == (0, 0)


async def test_caller_rollback_removes_both_original_and_current_observations(
    scenario: AdmissionScenario,
) -> None:
    """A broader financial transaction can roll back witness capture with its reservations.

    Given: An actual caller-owned wallet-fenced transaction,
    When: Unit capture succeeds but the broader transaction rolls back,
    Then: Neither v1 baseline nor v2 witness survives independently.
    """
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(session, admission_id(3))
        result = await producer.capture_bound_submission_unit_admission(
            session, scenario.order, scenario.route, admission_identity()
        )
        assert result.created is True
        payload = await get_bound_submission_admission(session, result.public_id)
        await session.rollback()
    assert await _counts(scenario) == (0, 0)
    with pytest.raises(SnapshotBindingError, match="original_snapshot_missing"):
        await insert_submission_unit_admission(
            scenario.repository, payload, admission_stamp(400, _NOW)
        )


@pytest.mark.parametrize("source", ("kraken", "kraken_futures", "polygon"))
async def test_real_paper_target_keeps_underlying_source_uuid_separate(
    monkeypatch: pytest.MonkeyPatch,
    source: AdmissionSource,
    tmp_path: Path,
) -> None:
    """A shared Paper symbol cannot erase the original actual market identity.

    Given: A Paper target with its own UUID and a real source instrument,
    When: Its actual simulated account route and source economics are admitted,
    Then: Both source and target identities are retained explicitly in v2.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'admission.db'}")
    monkeypatch.setattr(producer, "datetime", _Clock)
    monkeypatch.setattr(_Clock, "moment", _NOW)
    try:
        await repository.create_all()
        scenario = await seed_admission_scenario(repository, _NOW, source=source, paper=True)
        result = await producer.capture_submission_unit_admission(
            repository, scenario.order, scenario.route, admission_identity()
        )
        async with repository.session() as session:
            payload = await get_bound_submission_admission(session, result.public_id)
        assert payload.scope.instrument_public_id == admission_id(140)
        assert payload.current_source.instrument_public_id == admission_id(2)
        assert payload.current_source.exchange == source
        assert payload.route.scope.environment == "paper"
    finally:
        await repository.engine.dispose()


async def test_missing_current_command_cannot_be_reconstructed_from_a_frame(
    scenario: AdmissionScenario,
) -> None:
    """A queued frame is not durable creation authority after its current intent disappears.

    Given: Only a retained closed command version remains,
    When: The old outgoing frame reaches admission,
    Then: Current command absence is explicit and no evidence is fabricated.
    """
    async with scenario.repository.session() as session:
        command = (await session.execute(select(TradeCommand))).scalar_one()
        command.known_to = _NOW - timedelta(seconds=1)
        await session.commit()
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, scenario.route, admission_identity()
        )
    assert "current_command_missing" in str(raised.value.__cause__)
    assert await _counts(scenario) == (0, 0)


async def test_public_store_retry_is_fresh_and_attempt_content_is_immutable(
    scenario: AdmissionScenario,
) -> None:
    """A new producer cannot mutate a prior attempt or revive a disappeared current command.

    Given: One bound actual attempt with its original complete proof graph,
    When: It is inserted exactly, altered, or retried after current intent closure,
    Then: Only exact current evidence is idempotent and the other operations fail.
    """
    first = await producer.capture_submission_unit_admission(
        scenario.repository, scenario.order, scenario.route, admission_identity()
    )
    async with scenario.repository.session() as session:
        payload = await get_bound_submission_admission(session, first.public_id)
        with pytest.raises(SnapshotBindingError, match="admission_missing"):
            await get_bound_submission_admission(session, admission_id(77))
    exact = await insert_submission_unit_admission(
        scenario.repository, payload, admission_stamp(400, _NOW)
    )
    assert exact.created is False
    altered = payload.model_copy(update={"interpreted_at": _NOW + timedelta(seconds=1)})
    with pytest.raises(UnitAdmissionConflictError, match="attempt_evidence_changed"):
        await insert_submission_unit_admission(
            scenario.repository, altered, admission_stamp(401, _NOW + timedelta(seconds=1))
        )
    async with scenario.repository.session() as session:
        command = (await session.execute(select(TradeCommand))).scalar_one()
        command.known_to = _NOW - timedelta(seconds=1)
        await session.commit()
    with pytest.raises(SnapshotBindingError, match="current_command_missing"):
        await insert_submission_unit_admission(
            scenario.repository, payload, admission_stamp(402, _NOW)
        )
    assert await _counts(scenario) == (1, 1)


async def test_wrong_retained_v1_phase_is_refused_before_current_witness(
    scenario: AdmissionScenario,
) -> None:
    """Indexed target columns cannot disguise a differently typed canonical snapshot.

    Given: A malformed retained record whose indexed phase disagrees with its payload,
    When: Original intent evidence is loaded for a current admission,
    Then: The contradiction is refused without using the record as authorization.
    """
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(session, admission_id(3))
        await producer.capture_bound_submission_unit_admission(
            session, scenario.order, scenario.route, admission_identity()
        )
        row = (await session.execute(select(ExecutionUnitSnapshot))).scalar_one()
        original = ExecutionUnitSnapshotPayload.model_validate_json(row.canonical_payload)
        await session.rollback()
    data = original.model_dump(mode="python")
    data["target"] = FillSnapshotTarget(
        execution_public_id=admission_id(7),
        execution_digest="a" * 64,
        venue_event_public_id=admission_id(8),
        venue_event_digest="b" * 64,
    )
    malformed = ExecutionUnitSnapshotPayload.model_validate(data)
    async with scenario.repository.session() as session:
        session.add(
            ExecutionUnitSnapshot(
                **admission_stamp(333, _NOW).model_dump(mode="python"),
                phase="submission",
                wallet_public_id=admission_id(3),
                exchange="kraken",
                mode="live",
                instrument_public_id=admission_id(2),
                command_public_id=admission_id(5),
                execution_public_id=None,
                spec_public_id=original.catalog.spec_public_id,
                spec_timestamp=original.catalog.spec_timestamp,
                spec_source=original.catalog.source,
                spec_version=original.catalog.version,
                content_digest=snapshot_content_digest(malformed),
                canonical_payload=canonical_snapshot_payload(malformed),
            )
        )
        await session.commit()
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, scenario.route, admission_identity()
        )
    assert "original_snapshot_not_submission" in str(raised.value.__cause__)
    assert await _counts(scenario) == (1, 0)


@pytest.mark.parametrize("mapping", ("unknown", "missing_real_source"))
async def test_paper_never_guesses_absent_original_or_current_source_identity(
    monkeypatch: pytest.MonkeyPatch, mapping: str, tmp_path: Path
) -> None:
    """An unchanged Paper symbol cannot replace a missing actual source.

    Given: A legacy missing mapping or a closed current real source instrument,
    When: A simulated order reaches actual unit admission,
    Then: The source UUID is never guessed from a matching display name.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'admission.db'}")
    monkeypatch.setattr(producer, "datetime", _Clock)
    monkeypatch.setattr(_Clock, "moment", _NOW)
    try:
        await repository.create_all()
        scenario = await seed_admission_scenario(repository, _NOW, paper=True)
        async with repository.session() as session:
            identity = admission_id(140) if mapping == "unknown" else admission_id(2)
            instrument = (
                await session.execute(select(Instrument).where(Instrument.public_id == identity))
            ).scalar_one()
            if mapping == "unknown":
                instrument.source_exchange = None
            else:
                instrument.known_to = _NOW - timedelta(seconds=20)
            await session.commit()
        with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
            await producer.capture_submission_unit_admission(
                repository, scenario.order, scenario.route, admission_identity()
            )
        expected = (
            "source_instrument_identity_unavailable"
            if mapping == "unknown"
            else "unit_catalog_lineage_missing"
        )
        assert expected in str(raised.value.__cause__)
        assert await _counts(scenario) == (0, 0)
    finally:
        await repository.engine.dispose()


@pytest.mark.parametrize("source", (TOKENIZED_SPEC_SOURCE, FUTURES_SPEC_SOURCE))
async def test_unproven_rebase_or_derivative_payoff_never_falls_back_to_cash(
    scenario: AdmissionScenario, source: str
) -> None:
    """Complete-looking symbols cannot substitute a missing financial source profile.

    Given: Tokenized rebase semantics or derivative payoff metadata is unproven,
    When: A cash-shaped old intent reaches current admission,
    Then: It is refused rather than silently selecting spot cash accounting.
    """
    async with scenario.repository.session() as session:
        spec = (await session.execute(select(InstrumentSpec))).scalar_one()
        spec.spec_source = source
        await session.commit()
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, scenario.route, admission_identity()
        )
    assert "unproven" in str(raised.value.__cause__)
    assert await _counts(scenario) == (0, 0)


@pytest.mark.parametrize(
    "condition", ("revoked", "stale", "wrong_kind", "dated_valid", "expired", "missing_expiry")
)
async def test_derivative_profile_certification_freshness_and_expiry_are_independent(
    monkeypatch: pytest.MonkeyPatch, condition: str, tmp_path: Path
) -> None:
    """Known units do not override revoked certification, stale definitions or expiry.

    Given: A genuine isolated linear-contract catalog and actual account-route evidence,
    When: Current derivative admission is checked under each lifecycle condition,
    Then: Only the explicit still-valid dated contract is admitted.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'admission.db'}")
    monkeypatch.setattr(producer, "datetime", _Clock)
    monkeypatch.setattr(_Clock, "moment", _NOW)
    try:
        await repository.create_all()
        scenario = await seed_admission_scenario(repository, _NOW, source="kraken_futures")
        async with repository.session() as session:
            spec = (await session.execute(select(InstrumentSpec))).scalar_one()
            if condition == "revoked":
                spec.unit_certified = False
            elif condition == "stale":
                spec.spec_observed_at = _NOW - timedelta(hours=13)
            elif condition == "wrong_kind":
                spec.instrument_kind = "spot"
            else:
                spec.instrument_kind = "future"
                if condition == "dated_valid":
                    spec.expiry_at = _NOW + timedelta(days=1)
                elif condition == "expired":
                    spec.expiry_at = _NOW - timedelta(seconds=1)
            await session.commit()
        if condition == "dated_valid":
            result = await producer.capture_submission_unit_admission(
                repository, scenario.order, scenario.route, admission_identity()
            )
            assert result.created is True
            assert await _counts(scenario) == (1, 1)
        else:
            with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError):
                await producer.capture_submission_unit_admission(
                    repository, scenario.order, scenario.route, admission_identity()
                )
            assert await _counts(scenario) == (0, 0)
    finally:
        await repository.engine.dispose()


def test_catalog_lookup_requires_one_explicit_identity() -> None:
    """Ambiguous lookup shape cannot become a guessed source market.

    Given: Neither or both native-name and symbol-UUID lookup keys,
    When: A temporal source lookup is authored,
    Then: It refuses the ambiguous identity before a database query.
    """
    with pytest.raises(ValueError, match="exactly one"):
        UnitCatalogLookup(exchange="kraken")
    with pytest.raises(ValueError, match="exactly one"):
        UnitCatalogLookup(
            exchange="kraken", native_symbol="BTC-USD", symbol_public_id=admission_id(1)
        )


@pytest.mark.parametrize("change", ("margin", "environment", "command_alias"))
async def test_a_new_attempt_cannot_move_the_same_command_or_cid_to_another_account(
    contract_scenario: AdmissionScenario, change: str
) -> None:
    """Account scope remains pinned across distinct attempt and command-version identities.

    Given: One immutable original production admission for the durable command,
    When: A new attempt presents another margin account or demo environment,
    Then: The old command/CID remains held on its original account scope.
    """
    scenario = contract_scenario
    await producer.capture_submission_unit_admission(
        scenario.repository, scenario.order, scenario.route, admission_identity()
    )
    scope = scenario.authority.scope
    changes: dict[str, object]
    if change == "environment":
        changes = {
            "scope": scope.model_copy(update={"environment": "demo"}),
            "public_api_origin": "https://demo-futures.kraken.com",
        }
    else:
        changes = {"scope": scope.model_copy(update={"margin_account": "futures.cash"})}
    if change == "command_alias":
        async with scenario.repository.session() as session:
            command = (await session.execute(select(TradeCommand))).scalar_one()
            command.public_id = admission_id(76)
            await session.commit()
    _Clock.moment = _NOW + timedelta(seconds=2)
    route = await refresh_admission_account(scenario, changes, _Clock.moment, 400)
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, route, admission_identity(97)
        )
    assert "admission_command_account_changed" in str(raised.value.__cause__)
    assert await _counts(scenario) == (1, 1)


@pytest.mark.parametrize("identity", ("unknown", "same", "different"))
async def test_credential_rotation_requires_the_same_proven_external_account(
    contract_scenario: AdmissionScenario, identity: str
) -> None:
    """A fresh current credential is insufficient proof that the original external account survived.

    Given: A command admitted before an actual selected credential revision changes,
    When: A fresh observer response carries an unknown, same or different account id,
    Then: Only the same explicitly known account admits another witness.
    """
    scenario = contract_scenario
    account_id = None if identity == "unknown" else "venue-account-original"
    _Clock.moment = _NOW + timedelta(seconds=1)
    first_route: ExecutionRouteReference = await refresh_admission_account(
        scenario, {"venue_account_id": account_id}, _Clock.moment, 400
    )
    await producer.capture_submission_unit_admission(
        scenario.repository, scenario.order, first_route, admission_identity()
    )
    credential_at = _NOW + timedelta(seconds=2)
    async with scenario.repository.session() as session:
        old = (await session.execute(select(WalletCredential))).scalar_one()
        old.known_to = credential_at
        session.add(
            WalletCredential(
                **admission_stamp(94, credential_at).model_dump(mode="python"),
                wallet_public_id=scenario.authority.scope.wallet_public_id,
                exchange=scenario.authority.scope.exchange,
                credential_type="api_key_secret",
                encrypted_payload="isolated-new-fixture-value-never-decrypted",
            )
        )
        await session.commit()
    _Clock.moment = _NOW + timedelta(seconds=3)
    route = await refresh_admission_account(
        scenario,
        {
            "credential_public_id": admission_id(94),
            "credential_timestamp": credential_at,
            "venue_account_id": "venue-account-other" if identity == "different" else account_id,
        },
        _Clock.moment,
        401,
    )
    if identity == "same":
        result = await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, route, admission_identity(97)
        )
        assert result.created is True
        assert await _counts(scenario) == (1, 2)
    else:
        with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
            await producer.capture_submission_unit_admission(
                scenario.repository, scenario.order, route, admission_identity(97)
            )
        expected = (
            "venue_account_changed" if identity == "different" else "rotation_account_unproven"
        )
        assert expected in str(raised.value.__cause__)
        assert await _counts(scenario) == (1, 1)


async def test_one_client_cannot_change_a_known_venue_account_identity(
    contract_scenario: AdmissionScenario,
) -> None:
    """An unchanged credential cannot override contradictory authenticated account identities.

    Given: A command pinned to an explicit known venue account,
    When: The same selected client later reports a different account identifier,
    Then: The contradiction holds the command before another admission commits.
    """
    scenario = contract_scenario
    _Clock.moment = _NOW + timedelta(seconds=1)
    first = await refresh_admission_account(
        scenario, {"venue_account_id": "venue-account-original"}, _Clock.moment, 400
    )
    await producer.capture_submission_unit_admission(
        scenario.repository, scenario.order, first, admission_identity()
    )
    _Clock.moment = _NOW + timedelta(seconds=2)
    second = await refresh_admission_account(
        scenario, {"venue_account_id": "venue-account-other"}, _Clock.moment, 401
    )
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            scenario.repository, scenario.order, second, admission_identity(97)
        )
    assert "admission_venue_account_changed" in str(raised.value.__cause__)
    assert await _counts(scenario) == (1, 1)


@pytest.mark.parametrize(
    "version", ["s2a-v2:old-active", "s2a-v3:old-active", "fixture-etl:unproven"]
)
async def test_current_futures_admission_refuses_old_market_status_interpretation(
    contract_scenario: AdmissionScenario, version: str
) -> None:
    """An old active label cannot substitute for the corrected source interpretation.

    Given: A current certified contract row whose parser did not prove market status.
    When: Its durable command reaches the actual submission admission boundary.
    Then: Admission refuses before either a baseline or submission proof commits.
    """
    await _refresh_source_spec(contract_scenario, {"spec_version": version})
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_submission_unit_admission(
            contract_scenario.repository,
            contract_scenario.order,
            contract_scenario.route,
            admission_identity(),
        )
    assert "admission_current_derivative_status_policy_unproven" in str(raised.value.__cause__)
    assert await _counts(contract_scenario) == (0, 0)
