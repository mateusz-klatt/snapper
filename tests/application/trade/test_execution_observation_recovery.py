"""Interrupted full-frame capture, exact child recovery and durable restart idempotence."""

import base64
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

import snapper.application.trade.execution_observation_recovery as recovery
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.data.accounting_observation_binding import AccountingObservationBindingError
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.contract_accounting_store import RawAccountingWriteResult
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.models import AccountingHoldResolution
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold
from snapper.data.models import Execution
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 10, 23, 11, tzinfo=UTC)
_WIRE = (
    b'{"feed":"fills","fills":[{"fill_id":"first","price":0.123456789012345678901},'
    b'{"fill_id":"second"},null]}'
)


def _id(number: int) -> str:
    """Keep local fixture observation, wallet and credential identities independent."""
    return f"00000000-0000-7000-8000-{number:012d}"


def _raw(wire: bytes = _WIRE, **updates: object) -> RawAccountingObservation:
    """Retain only the first original selector despite a multi-child source envelope."""
    raw = RawAccountingObservation(
        scope=AccountingObservationScope(
            wallet_public_id=_id(101),
            exchange="kraken_futures",
            mode="live",
            environment="production",
        ),
        route_revision="actual-client-selection",
        credential_public_id=_id(102),
        credential_timestamp=_NOW - timedelta(days=1),
        kind="execution",
        channel="kraken_futures.ws.fills",
        representation="wire_bytes_base64",
        material=base64.b64encode(wire).decode("ascii"),
        target_json_pointer="/fills/0",
        observed_at=_NOW,
        source_identity="first",
        source_identity_kind="unproven",
    )
    return RawAccountingObservation.model_validate({**raw.model_dump(), **updates})


def _stamp(number: int, seconds: int = 1) -> AccountingWriteProvenance:
    """Supply a separate actual local recovery knowledge time and trace identity."""
    return AccountingWriteProvenance(
        public_id=_id(number),
        session_id=_id(999),
        sequence_id=number,
        timestamp=_NOW + timedelta(seconds=seconds),
    )


def test_all_children_preserve_wire_clock_and_only_their_own_identity_hints() -> None:
    """Recovery retains every child including a malformed null value.

    Given: one saved selector,
    When: the whole-frame recovery selector examines it,
    Then: recovery retains every child including a malformed null value.
    """
    original = _raw(
        target_json_pointer="/fills/1",
        order_public_id=_id(201),
        source_instrument_public_id=_id(202),
        native_symbol="first-child-only",
    )
    children = recovery.recoverable_frame_children(original)
    assert [child.target_json_pointer for child in children] == [
        "/fills/0",
        "/fills/1",
        "/fills/2",
    ]
    assert [child.source_identity for child in children] == ["first", "second", None]
    assert [child.source_identity_kind for child in children] == ["unproven", "unproven", None]
    for child in children:
        assert base64.b64decode(child.material) == _WIRE
        assert child.observed_at == _NOW
        assert child.scope == original.scope
        assert child.credential_public_id == original.credential_public_id
        assert child.route_revision == original.route_revision
        assert child.order_public_id is None
        assert child.source_instrument_public_id is None
        assert child.native_symbol is None


@pytest.mark.parametrize(
    ("feed", "body", "pointers", "identities"),
    [
        (
            "fills_snapshot",
            '"fills":[{"fill_id":""},{"fill_id":7}]',
            ["/fills/0", "/fills/1"],
            [None, None],
        ),
        (
            "account_log_snapshot",
            '"logs":[{"booking_uid":"same"},{}]',
            ["/logs/0", "/logs/1"],
            ["same", None],
        ),
        (
            "account_log",
            '"new_entry":{"booking_uid":"case-Sensitive"}',
            ["/new_entry"],
            ["case-Sensitive"],
        ),
        ("account_log", '"new_entry":null', ["/new_entry"], [None]),
        ("fills", '"fills":[]', [], []),
    ],
)
def test_known_futures_shapes(
    feed: str, body: str, pointers: list[str], identities: list[str | None]
) -> None:
    """Recovery selects their exact native shape.

    Given: documented list or singleton envelopes,
    When: the whole-frame recovery selector examines it,
    Then: recovery selects their exact native shape.
    """
    wire = ('{"feed":"' + feed + '",' + body + "}").encode()
    children = recovery.recoverable_frame_children(_raw(wire, channel=f"kraken_futures.ws.{feed}"))
    assert [child.target_json_pointer for child in children] == pointers
    assert [child.source_identity for child in children] == identities


def test_spot_execution_frame_uses_the_actual_data_container() -> None:
    """Futures field names never substitute for the documented data list.

    Given: a Spot envelope,
    When: the whole-frame recovery selector examines it,
    Then: Futures field names never substitute for the documented data list.
    """
    original = _raw()
    spot_scope = AccountingObservationScope.model_validate(
        {**original.scope.model_dump(), "exchange": "kraken"}
    )
    children = recovery.recoverable_frame_children(
        _raw(
            b'{"channel":"executions","data":[{"exec_id":"spot-1"},false]}',
            scope=spot_scope,
            channel="kraken.ws.executions",
        )
    )
    assert [child.target_json_pointer for child in children] == ["/data/0", "/data/1"]
    assert [child.source_identity for child in children] == ["spot-1", None]


@pytest.mark.parametrize("limit", [True, False, 0, -1, 1001, 1.5, "1"])
def test_invalid_child_bound_refuses_coercion(limit: object) -> None:
    """Recovery never silently expands or coerces its requested budget.

    Given: an invalid bound,
    When: the whole-frame recovery selector examines it,
    Then: recovery never silently expands or coerces its requested budget.
    """
    with pytest.raises(ValueError, match="child bound"):
        recovery.recoverable_frame_children(_raw(), cast(int, limit))


def test_excess_children_are_refused_without_truncating_the_frame() -> None:
    """A partial result must not masquerade as full recovery.

    Given: more children than the budget,
    When: the whole-frame recovery selector examines it,
    Then: a partial result must not masquerade as full recovery.
    """
    with pytest.raises(recovery.AccountingFrameRecoveryUnavailableError, match="bound_exceeded"):
        recovery.recoverable_frame_children(_raw(), maximum_children=2)


@pytest.mark.parametrize(
    "wire",
    [b"[1]", b"{", b"\xff", b'{"feed":"fills","feed":"fills"}', b'{"feed":"fills","fills":[NaN]}'],
)
def test_invalid_original_json_remains_uninterpreted(wire: bytes) -> None:
    """Recovery leaves the existing source held.

    Given: ambiguous or malformed original JSON,
    When: the whole-frame recovery selector examines it,
    Then: recovery leaves the existing source held.
    """
    with pytest.raises(recovery.AccountingFrameRecoveryUnavailableError, match="wire_invalid"):
        recovery.recoverable_frame_children(_raw(wire))


@pytest.mark.parametrize(
    ("wire", "reason"),
    [
        (b'{"feed":"fills","api_key":"synthetic-test-control"}', "control_envelope"),
        (b'{"feed":"other","fills":[]}', "envelope_unproven"),
        (b'{"feed":[],"fills":[]}', "envelope_unproven"),
        (b'{"feed":"fills"}', "container_missing"),
        (b'{"feed":"fills","fills":{}}', "container_invalid"),
    ],
)
def test_unproven_envelopes_cannot_become_child_receipts(wire: bytes, reason: str) -> None:
    """No record shape is invented.

    Given: an unsupported/control envelope or absent container,
    When: the whole-frame recovery selector examines it,
    Then: no record shape is invented.
    """
    with pytest.raises(recovery.AccountingFrameRecoveryUnavailableError, match=reason):
        recovery.recoverable_frame_children(_raw(wire))


def test_reconstructed_material_and_mismatched_capture_channel_are_refused() -> None:
    """Child keys alone prove nothing.

    Given: weaker material or a contradictory source channel,
    When: the whole-frame recovery selector examines it,
    Then: child keys alone prove nothing.
    """
    with pytest.raises(recovery.AccountingFrameRecoveryUnavailableError, match="original_wire"):
        recovery.recoverable_frame_children(_raw(representation="parsed_fields_json"))
    with pytest.raises(recovery.AccountingFrameRecoveryUnavailableError, match="envelope_unproven"):
        recovery.recoverable_frame_children(_raw(channel="kraken_futures.ws.other"))


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Use a separate real SQLite store with the immutable raw evidence and hold guards."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'recovery.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def test_one_committed_child_recovers_the_whole_frame_without_posting_financial_rows(
    repository: SQLAlchemyRepository,
) -> None:
    """All siblings and holds survive recovery.

    Given: a process that retained only its first child,
    When: frame recovery persists the remaining children,
    Then: all siblings and holds survive recovery.
    """
    source = await capture_accounting_observation(repository, _raw(), _stamp(1))
    results = await recovery.recover_accounting_frame(repository, source.public_id, _stamp(2, 2))
    assert len(results) == 3
    assert all(result.created and not result.conflicted for result in results)
    async with repository.session() as session:
        raw_count = await session.scalar(select(func.count()).select_from(AccountingRawReceipt))
        hold_count = await session.scalar(select(func.count()).select_from(AccountingScopeHold))
        assert raw_count == hold_count == 4
        assert await session.scalar(select(func.count()).select_from(Execution)) == 0
        assert await session.scalar(select(func.count()).select_from(AccountingHoldResolution)) == 0
        children = [
            await get_bound_accounting_observation(session, item.public_id) for item in results
        ]
    assert [child.target_json_pointer for child in children] == ["/fills/0", "/fills/1", "/fills/2"]
    assert all(
        child.observed_at == _NOW and base64.b64decode(child.material) == _WIRE
        for child in children
    )


@pytest.mark.parametrize("lost_commit_ack", [False, True])
async def test_interrupted_recovery_reuses_committed_children_after_restart(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    lost_commit_ack: bool,
) -> None:
    """Restart never duplicates it.

    Given: a failed second child or its lost commit acknowledgement,
    When: frame recovery persists the remaining children,
    Then: restart never duplicates it.
    """
    source = await capture_accounting_observation(repository, _raw(), _stamp(1))
    attempts = 0

    async def interrupted_capture(
        repo: AccountingRepository, raw: RawAccountingObservation, stamp: AccountingWriteProvenance
    ) -> RawAccountingWriteResult:
        """Fail at a real persistence boundary while preserving the already committed first child."""
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            if lost_commit_ack:
                await capture_accounting_observation(repo, raw, stamp)
            raise OperationalError("commit", {}, RuntimeError("interrupted recovery"))
        return await capture_accounting_observation(repo, raw, stamp)

    monkeypatch.setattr(recovery, "capture_accounting_observation", interrupted_capture)
    with pytest.raises(OperationalError, match="interrupted recovery"):
        await recovery.recover_accounting_frame(repository, source.public_id, _stamp(2, 2))
    monkeypatch.setattr(recovery, "capture_accounting_observation", capture_accounting_observation)
    results = await recovery.recover_accounting_frame(repository, source.public_id, _stamp(3, 3))
    assert [result.created for result in results] == [False, not lost_commit_ack, True]
    again = await recovery.recover_accounting_frame(repository, results[-1].public_id, _stamp(4, 4))
    assert [result.public_id for result in again] == [result.public_id for result in results]
    assert not any(result.created for result in again)
    async with repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(AccountingRawReceipt)) == 4
        assert await session.scalar(select(func.count()).select_from(AccountingScopeHold)) == 4


async def test_sibling_original_receipt_converges_but_another_observation_clock_does_not(
    repository: SQLAlchemyRepository,
) -> None:
    """Recovery converges without merging a later observation.

    Given: sibling receipts of the same frame,
    When: frame recovery persists the remaining children,
    Then: recovery converges without merging a later observation.
    """
    first = await capture_accounting_observation(repository, _raw(), _stamp(1))
    sibling = await capture_accounting_observation(
        repository, _raw(target_json_pointer="/fills/1", source_identity="second"), _stamp(2)
    )
    later = await capture_accounting_observation(
        repository, _raw(observed_at=_NOW + timedelta(seconds=1)), _stamp(3)
    )
    original = await recovery.recover_accounting_frame(repository, first.public_id, _stamp(4, 2))
    same = await recovery.recover_accounting_frame(repository, sibling.public_id, _stamp(5, 2))
    fresh = await recovery.recover_accounting_frame(repository, later.public_id, _stamp(6, 2))
    assert [result.public_id for result in same] == [result.public_id for result in original]
    assert not any(result.created for result in same)
    assert not {result.public_id for result in original} & {result.public_id for result in fresh}


async def test_another_credential_revision_does_not_relabel_original_recovery(
    repository: SQLAlchemyRepository,
) -> None:
    """Each retained frame keeps its original client identity.

    Given: a newer selected credential,
    When: frame recovery persists the remaining children,
    Then: each retained frame keeps its original client identity.
    """
    first = await capture_accounting_observation(repository, _raw(), _stamp(1))
    changed = _raw(
        credential_public_id=_id(103),
        credential_timestamp=_NOW,
        route_revision="a-different-selected-client",
    )
    second = await capture_accounting_observation(repository, changed, _stamp(2))
    original = await recovery.recover_accounting_frame(repository, first.public_id, _stamp(3, 2))
    rotated = await recovery.recover_accounting_frame(repository, second.public_id, _stamp(4, 2))
    repeated = await recovery.recover_accounting_frame(repository, first.public_id, _stamp(5, 3))
    assert [item.public_id for item in repeated] == [item.public_id for item in original]
    assert not {item.public_id for item in original} & {item.public_id for item in rotated}
    async with repository.session() as session:
        old_child = await get_bound_accounting_observation(session, original[0].public_id)
        new_child = await get_bound_accounting_observation(session, rotated[0].public_id)
    assert old_child.credential_public_id == _id(102)
    assert new_child.credential_public_id == _id(103)
    assert old_child.route_revision == "actual-client-selection"
    assert new_child.route_revision == "a-different-selected-client"


@pytest.mark.parametrize(("observed_seconds", "recorded_seconds"), [(0, 3), (3, 1)])
async def test_recovery_cannot_predate_source_capture_or_local_source_knowledge(
    repository: SQLAlchemyRepository, observed_seconds: int, recorded_seconds: int
) -> None:
    """Recovery refuses a backdated child stamp.

    Given: later capture or recording knowledge,
    When: frame recovery persists the remaining children,
    Then: recovery refuses a backdated child stamp.
    """
    source = await capture_accounting_observation(
        repository,
        _raw(observed_at=_NOW + timedelta(seconds=observed_seconds)),
        _stamp(1, recorded_seconds),
    )
    with pytest.raises(
        recovery.AccountingFrameRecoveryUnavailableError, match="knowledge_backdated"
    ):
        await recovery.recover_accounting_frame(repository, source.public_id, _stamp(2, 2))
    async with repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(AccountingRawReceipt)) == 1


async def test_missing_source_receipt_cannot_be_replaced_with_caller_material(
    repository: SQLAlchemyRepository,
) -> None:
    """Recovery creates no observation or financial evidence.

    Given: no actual immutable source row,
    When: frame recovery persists the remaining children,
    Then: recovery creates no observation or financial evidence.
    """
    with pytest.raises(AccountingObservationBindingError, match="receipt_missing"):
        await recovery.recover_accounting_frame(repository, _id(404), _stamp(1))
