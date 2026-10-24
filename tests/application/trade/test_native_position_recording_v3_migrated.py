"""Record genuine SDK sources against unchanged migrations through 0065 on both backends.

PostgreSQL requires SNAPPER_NEXT38_PG_CONNECTION_FILE naming an owner-only JSON
receipt with run_directory, socket_directory, role and port. The fixture accepts
only an owned Unix-socket cluster, verifies its actual server identity, and owns
one newly created database per case. SQLite always uses a blank migrated file.
Neither backend initializes its source catalog through ORM create_all.
Each case owns full physical migration and SDK setup under a bounded integration
timeout, including when the complete suite measures coverage concurrently.
"""

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal
from uuid import UUID
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from pydantic import TypeAdapter
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.pool import NullPool

from snapper.application.portfolio.accounting_native_source_v2 import (
    AccountingNativeSourceClaimPayloadV2,
)
from snapper.application.portfolio.accounting_native_source_v2 import (
    canonical_native_source_claim_v2,
)
from snapper.application.portfolio.native_financial_payloads_v2 import (
    canonical_native_financial_payload_v2,
)
from snapper.application.portfolio.native_position_admission_v3 import AdmittedNativePositionEventV3
from snapper.application.portfolio.native_position_admission_v3 import (
    admitted_native_position_digest_v3,
)
from snapper.application.portfolio.native_position_admission_v3 import (
    canonical_admitted_native_position_v3,
)
from snapper.application.trade.native_position_recording_v3 import RecordedNativePositionV3
from snapper.application.trade.native_position_recording_v3 import record_native_position_v3
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.json_types import JsonObject
from snapper.core.native_financial_publication_v2 import NativeContractPositionPublicationV2
from snapper.data.accounting_native_source_store_v2 import native_claim_columns_v2
from snapper.data.contract_account_projection_store import load_bound_contract_account_snapshot
from snapper.data.contract_accounting_integrity import run_contract_accounting_restore_audit
from snapper.data.financial_account_owner_policy_writer import FinancialOwnerPolicyChange
from snapper.data.financial_account_owner_policy_writer import write_financial_owner_policy
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import FinancialPublicationIntent
from snapper.data.models import PrivateSourceOwnershipReceipt
from snapper.data.models import Symbol
from snapper.data.models import VenueEvent
from snapper.data.models import is_open_ended_version
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_financial_event_family import AdmittedNativeEventMaterialV3
from snapper.data.native_financial_event_family import get_native_financial_event_family
from snapper.data.native_financial_event_family import native_position_event_columns_v3
from snapper.data.native_financial_publication_store_v2 import (
    get_bound_native_position_publication_v2,
)
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.native_account_authority import NativeAuthorityDatabase
from tests.helpers.native_position_recording_v3 import NativePositionRecordingScenarioV3
from tests.helpers.native_position_recording_v3 import native_position_recording_scenario_v3
from tests.helpers.native_position_recording_v3 import record_native_position_scenario_v3

pytestmark = pytest.mark.timeout(120)


@dataclass
class _CommittedObserver:
    """Independently observe the same migrated database during the SDK's actual HTTP send."""

    engine: Engine
    seen: list[tuple[int, int]] = field(default_factory=list)

    def __call__(self) -> tuple[int, int]:
        """Read committed attempts and proofs using a separate nonpooled synchronous connection.

        Returns:
            Actual committed counts visible before the local signed-HTTP response is supplied.
        """
        with self.engine.connect() as connection:
            attempts = connection.execute(
                text("SELECT COUNT(*) FROM native_account_read_attempts")
            ).scalar_one()
            proofs = connection.execute(
                text("SELECT COUNT(*) FROM native_account_identity_proofs")
            ).scalar_one()
        assert isinstance(attempts, int) and isinstance(proofs, int)
        result = attempts, proofs
        self.seen.append(result)
        return result

    def identity_proofs(self) -> tuple[tuple[str, str], ...]:
        """Independently read exact committed proof identities and canonical bytes at physical send.

        Returns:
            Original same-database proof rows without weakening UUID or private-material checks.
        """
        values: list[tuple[str, str]] = []
        with self.engine.connect() as connection:
            for public_id, payload in connection.execute(
                text("SELECT public_id, canonical_payload FROM native_account_identity_proofs")
            ):
                normalized_id = str(public_id) if isinstance(public_id, UUID) else public_id
                assert isinstance(normalized_id, str) and isinstance(payload, str)
                values.append((normalized_id, payload))
        return tuple(values)


def _sync_url(url: URL) -> URL:
    """Select the already declared synchronous driver for an independent test observer.

    Args:
        url: Actual isolated application database using its normal asynchronous driver.

    Returns:
        The identical database and Unix socket using the existing synchronous test dependency.
    """
    if url.get_backend_name() == "sqlite":
        return url.set(drivername="sqlite")
    query = dict(url.query)
    query.pop("ssl")
    query["sslmode"] = "disable"
    return url.set(drivername="postgresql+psycopg2", query=query)


def _require_blank(connection: Connection) -> None:
    """Refuse to apply migration evidence to an existing application catalog.

    Args:
        connection: Newly owned database before any real migration has executed.
    """
    assert inspect(connection).get_table_names() == []


def _upgrade_empty(url: URL) -> None:
    """Apply the real blank-to0065 chain without model DDL or a substitute migration.

    Args:
        url: Newly owned empty SQLite file or PostgreSQL database.
    """
    root = Path(__file__).resolve().parents[3]
    probe = create_engine(_sync_url(url), poolclass=NullPool)
    try:
        with probe.connect() as connection:
            _require_blank(connection)
    finally:
        probe.dispose()
    config = Config()
    config.set_main_option("path_separator", "newline")
    config.set_main_option("script_location", str(root / "src/snapper/data/migrations"))
    config.set_main_option("version_locations", str(root / "src/snapper/data/migrations/versions"))
    config.set_main_option(
        "sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%")
    )
    command.upgrade(config, "0065")


@asynccontextmanager
async def _migrated_source(url: URL) -> AsyncIterator[NativeAuthorityDatabase]:
    """Own actual migrated storage and independent observer resources for one complete SDK case.

    Args:
        url: Fresh isolated target whose actual migration graph ends at0065.

    Yields:
        Explicit initialized source injection that bypasses every ORM create_all fixture.
    """
    await asyncio.to_thread(_upgrade_empty, url)
    repository = SQLAlchemyRepository(url.render_as_string(hide_password=False))
    observer = _CommittedObserver(create_engine(_sync_url(url), poolclass=NullPool))
    try:
        async with repository.session() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0065"
            assert await session.scalar(select(func.count()).select_from(Execution)) == 0
            symbol = (
                await session.scalars(
                    select(Symbol).where(
                        Symbol.native_symbol == "BTC-USD",
                        Symbol.timestamp <= datetime.now(UTC),
                        Symbol.known_to > datetime.now(UTC),
                    )
                )
            ).one()
            assert is_open_ended_version(symbol.known_to)
            assert (symbol.base, symbol.quote, symbol.asset_type) == ("BTC", "USD", "crypto")
        yield NativeAuthorityDatabase(repository, observer, observer.identity_proofs, symbol)
    finally:
        await repository.engine.dispose()
        observer.engine.dispose()


@dataclass(frozen=True, slots=True)
class _PostgresEndpoint:
    """An explicitly owned local cluster receipt, never a deployment database URL."""

    run_directory: Path
    socket_directory: Path
    role: str
    port: int


def _owned_directory(value: object) -> Path:
    """Validate receipt directories before any PostgreSQL connection is possible.

    Args:
        value: Untrusted path supplied by the explicit local-cluster receipt.

    Returns:
        Existing owner-only directory belonging to the current user.
    """
    assert isinstance(value, str)
    result = Path(value).resolve(strict=True)
    assert result.is_dir() and result.stat().st_uid == os.geteuid()
    assert result.stat().st_mode & 0o777 == 0o700
    return result


def _postgres_endpoint() -> _PostgresEndpoint:
    """Require an explicit owned Unix-socket receipt instead of reading DB_URL or deployment settings.

    Returns:
        Validated local cluster metadata for newly created disposable databases only.
    """
    receipt = os.environ.get("SNAPPER_NEXT38_PG_CONNECTION_FILE")
    if receipt is None:
        pytest.skip(
            "Actual migrated PostgreSQL SDK proof requires an explicit owned-cluster receipt"
        )
    path = Path(receipt).resolve(strict=True)
    assert path.stat().st_uid == os.geteuid() and path.stat().st_mode & 0o777 == 0o600
    info = TypeAdapter(JsonObject).validate_json(path.read_text())
    run = _owned_directory(info["run_directory"])
    socket = _owned_directory(info["socket_directory"])
    _owned_directory(str(run / "d"))
    role, port = info["role"], info["port"]
    assert socket.is_relative_to(run)
    assert isinstance(role, str) and role and isinstance(port, int) and not isinstance(port, bool)
    assert 20000 <= port < 40000
    return _PostgresEndpoint(run, socket, role, port)


def _require_owned_server(connection: Connection, endpoint: _PostgresEndpoint) -> None:
    """Bind actual server responses to the owned receipt before creating or dropping any database.

    Args:
        connection: Independent Unix-socket administrative connection.
        endpoint: Already validated owner-only local cluster metadata.
    """
    assert connection.scalar(text("SHOW data_directory")) == str(endpoint.run_directory / "d")
    assert connection.scalar(text("SELECT inet_server_addr() IS NULL")) is True
    assert connection.scalar(text("SHOW listen_addresses")) == ""
    assert connection.scalar(text("SELECT current_user")) == endpoint.role
    assert connection.scalar(text("SHOW fsync")) == "on"
    assert connection.scalar(text("SHOW synchronous_commit")) == "on"


@asynccontextmanager
async def _postgres_database(directory: Path) -> AsyncIterator[URL]:
    """Create and remove one blank database on the explicitly owned local PostgreSQL server.

    Args:
        directory: Private per-case namespace for an empty passfile, never a deployment secret.

    Yields:
        Genuine isolated asyncpg database URL shared by migration, SDK writer and observer.
    """
    endpoint = _postgres_endpoint()
    passfile = directory / "next38-empty-passfile"
    passfile.touch(mode=0o600, exist_ok=False)
    url = URL.create(
        "postgresql+asyncpg",
        username=endpoint.role,
        port=endpoint.port,
        database="postgres",
        query={"host": str(endpoint.socket_directory), "passfile": str(passfile), "ssl": "disable"},
    )
    admin = create_engine(_sync_url(url), poolclass=NullPool, isolation_level="AUTOCOMMIT")
    name = "next38_" + uuid4().hex
    created = False
    try:
        with admin.connect() as connection:
            _require_owned_server(connection, endpoint)
            connection.exec_driver_sql(f'CREATE DATABASE "{name}"')
            created = True
        yield url.set(database=name)
    finally:
        try:
            if created:
                with admin.connect() as connection:
                    _require_owned_server(connection, endpoint)
                    connection.exec_driver_sql(f'DROP DATABASE "{name}"')
        finally:
            admin.dispose()
            passfile.unlink()


@pytest.fixture(params=("sqlite", "postgresql"))
async def migrated_database(
    request: pytest.FixtureRequest, tmp_path: Path
) -> AsyncIterator[NativeAuthorityDatabase]:
    """Supply actual migrated backends independently of the autouse ORM schema template.

    Args:
        request: Actual parametrized backend selection.
        tmp_path: Per-case private namespace for the real empty database or cluster passfile.

    Yields:
        Same-database source and observer injection after the real complete0065 upgrade.
    """
    assert request.param in {"sqlite", "postgresql"}
    if request.param == "sqlite":
        url = URL.create("sqlite+aiosqlite", database=str(tmp_path / "actual-migrated.db"))
        async with _migrated_source(url) as database:
            yield database
        return
    async with _postgres_database(tmp_path) as url, _migrated_source(url) as database:
        yield database


async def _counts(repository: SQLAlchemyRepository) -> tuple[int, ...]:
    """Read independently committed targets and original source facts from a fresh session.

    Args:
        repository: Actual migrated database used by the real SDK source graph.

    Returns:
        Execution, venue, fill-unit, anchor, event, claim, intent, raw and ownership counts.
    """
    models = (
        Execution,
        VenueEvent,
        ContractAccountingAnchor,
        ContractAccountingEvent,
        AccountingNativeSourceClaim,
        FinancialPublicationIntent,
        AccountingRawReceipt,
        PrivateSourceOwnershipReceipt,
    )
    async with repository.session() as session:
        values = [
            int((await session.execute(select(func.count()).select_from(model))).scalar_one())
            for model in models
        ]
        units = int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(ExecutionUnitSnapshot)
                    .where(ExecutionUnitSnapshot.phase == "fill")
                )
            ).scalar_one()
        )
    return values[0], values[1], units, *values[2:]


def _assert_one_recording(before: tuple[int, ...], after: tuple[int, ...]) -> None:
    """Require every financial target once while all original raw and ownership rows stay exact.

    Args:
        before: Independently observed committed counts before the actual SDK financial writer.
        after: Independently observed committed counts after its one real commit.
    """
    assert len(before) == len(after) == 9
    assert after == tuple(value + 1 for value in before[:7]) + before[7:]


async def _assert_retained_graph(
    scenario: NativePositionRecordingScenarioV3, recorded: RecordedNativePositionV3
) -> None:
    """Verify actual migrated mirrors, canonical families and references above successful insertion.

    Args:
        scenario: Genuine original authenticated SDK fill, log, authority and admission graph.
        recorded: Coordinates returned only after the real financial commit completed.
    """
    async with scenario.source.repository.session() as session:
        material = await get_native_financial_event_family(
            session, recorded.position.public_id, datetime.now(UTC)
        )
        assert isinstance(material, AdmittedNativeEventMaterialV3)
        envelope = AdmittedNativePositionEventV3.model_validate_json(material.row.canonical_payload)
        assert material.row.canonical_payload == canonical_admitted_native_position_v3(envelope)
        assert material.row.content_digest == admitted_native_position_digest_v3(envelope)
        assert canonical_native_financial_payload_v2(
            envelope.position
        ) == canonical_native_financial_payload_v2(material.payload)
        assert all(
            getattr(material.row, key) == value
            for key, value in native_position_event_columns_v3(envelope).items()
        )
        assert (
            envelope.fill_unit_admission.submission_admission
            == scenario.request.submission_admission
        )
        claim = (
            await session.scalars(
                select(AccountingNativeSourceClaim).where(
                    AccountingNativeSourceClaim.accounting_event_public_id == material.row.public_id
                )
            )
        ).one()
        payload = AccountingNativeSourceClaimPayloadV2.model_validate_json(claim.canonical_payload)
        assert claim.canonical_payload == canonical_native_source_claim_v2(payload)
        assert payload.accounting_event_content_digest == material.row.content_digest
        assert all(
            getattr(claim, key) == value for key, value in native_claim_columns_v2(payload).items()
        )
        publication = await get_bound_native_position_publication_v2(
            session, recorded.publication.public_id
        )
        assert isinstance(publication.payload, NativeContractPositionPublicationV2)
        assert publication.payload.position_event_digest == material.row.content_digest
        assert publication.payload.execution_public_id == recorded.execution_public_id
        assert publication.payload.unit_snapshot_public_id == recorded.unit_snapshot_public_id


@pytest.mark.parametrize("completion", ("partial", "full"))
async def test_actual_migrated_sdk_recording_commits_original_graph_and_thirty_guard_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    migrated_database: NativeAuthorityDatabase,
    completion: Literal["partial", "full"],
) -> None:
    """Prove one real SDK financial transaction on the actual unchanged migration chain.

    Given: A genuinely blank-to0065 migrated database and original signed HTTP and SDK sources.
    When: The atomic caller records one actual partial or full original fill and later position log.
    Then: Complete outer V3, inner V2, shared claim, publication, held projection and thirty guards bind.
    """
    async with native_position_recording_scenario_v3(
        tmp_path, monkeypatch, completion=completion, database=migrated_database
    ) as scenario:
        before = await _counts(scenario.source.repository)
        recorded = await record_native_position_scenario_v3(scenario)
        assert recorded.created
        _assert_one_recording(before, await _counts(scenario.source.repository))
        await _assert_retained_graph(scenario, recorded)
        async with scenario.source.repository.session() as session:
            await scenario.source.repository._begin_execution_insert_transaction(session)
            await scenario.source.repository._acquire_execution_fence_lock(
                session, scenario.request.wallet_public_id
            )
            snapshot = await load_bound_contract_account_snapshot(
                session, scenario.authority.scope, datetime.now(UTC)
            )
            await session.commit()
        assert snapshot.positions[0].quantity == Decimal(1 if completion == "partial" else 2)
        assert snapshot.native_cash == () and snapshot.readiness == "held"
        report = await run_contract_accounting_restore_audit(scenario.source.repository)
        assert (report.event_count, report.claim_count, report.publication_count) == (1, 1, 1)
        assert len(report.physical_guards) == 30
        observer = migrated_database.observe_committed
        assert isinstance(observer, _CommittedObserver) and len(observer.seen) >= 2
        assert all(attempts >= 1 for attempts, _proofs in observer.seen)
        assert scenario.source.adapter.proofs_at_accounts_send
        assert min(scenario.source.adapter.proofs_at_accounts_send) >= 1


async def _insert_failure(session: AsyncSession, table: str, install: bool) -> None:
    """Toggle only an isolated late INSERT refusal while retaining all original immutable guards.

    Args:
        session: Actual caller session against the migrated disposable database.
        table: One explicitly parametrized late financial target.
        install: Install the deliberate refusal or remove only that fixture-owned trigger/function.
    """
    assert table in {
        "contract_accounting_events",
        "accounting_native_source_claims",
        "financial_publication_intents",
    }
    if session.get_bind().dialect.name == "sqlite":
        sql = (
            f"CREATE TRIGGER next38_late_insert_failure BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT, 'next38_actual_late_insert_refusal'); END"
            if install
            else "DROP TRIGGER next38_late_insert_failure"
        )
        await session.execute(text(sql))
    elif install:
        await session.execute(
            text(
                "CREATE FUNCTION next38_late_insert_refusal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'next38_actual_late_insert_refusal'; END; $$"
            )
        )
        await session.execute(
            text(
                f"CREATE TRIGGER next38_late_insert_failure BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION next38_late_insert_refusal()"
            )
        )
    else:
        await session.execute(text(f"DROP TRIGGER next38_late_insert_failure ON {table}"))
        await session.execute(text("DROP FUNCTION next38_late_insert_refusal()"))
    await session.commit()


@pytest.mark.parametrize(
    "table",
    (
        "contract_accounting_events",
        "accounting_native_source_claims",
        "financial_publication_intents",
    ),
)
async def test_actual_migrated_late_insert_refusal_rolls_back_every_new_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    migrated_database: NativeAuthorityDatabase,
    table: str,
) -> None:
    """Preserve original migrated sources while refusing a real late financial SQL insert.

    Given: Real authenticated sources and unchanged migration constraints plus one deliberate INSERT fault.
    When: The complete atomic SDK financial writer reaches that actual late SQL boundary.
    Then: All newly created targets roll back and the original request succeeds after fault removal.
    """
    async with native_position_recording_scenario_v3(
        tmp_path, monkeypatch, database=migrated_database
    ) as scenario:
        before = await _counts(scenario.source.repository)
        async with scenario.source.repository.session() as session:
            await _insert_failure(session, table, True)
        with pytest.raises(DBAPIError, match="next38_actual_late_insert_refusal"):
            await record_native_position_scenario_v3(scenario)
        assert await _counts(scenario.source.repository) == before
        async with scenario.source.repository.session() as session:
            await _insert_failure(session, table, False)
        recorded = await record_native_position_scenario_v3(scenario)
        _assert_one_recording(before, await _counts(scenario.source.repository))
        await _assert_retained_graph(scenario, recorded)


class _CommitAcknowledgementLostError(RuntimeError):
    """The observer loses acknowledgement only after the real migrated financial commit succeeds."""


async def test_actual_migrated_commit_uncertainty_and_revoked_retry_keep_original_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, migrated_database: NativeAuthorityDatabase
) -> None:
    """Recover original coordinates from a new repository after actual commit acknowledgement loss.

    Given: Genuine source evidence on the actual migration chain and a real commit followed by lost ACK.
    When: A new repository recovers the request and retries after owner revocation and authority expiry.
    Then: One graph, original canonical identities and clocks survive without a renewed source permission.
    """
    async with native_position_recording_scenario_v3(
        tmp_path, monkeypatch, database=migrated_database
    ) as scenario:
        before = await _counts(scenario.source.repository)
        original_commit = AsyncSession.commit

        async def commit_then_lose_ack(session: AsyncSession) -> None:
            """Complete the actual database commit before deliberately hiding its acknowledgement.

            Args:
                session: Actual financial writer transaction being observed at its sole commit.
            """
            await original_commit(session)
            raise _CommitAcknowledgementLostError("next38_actual_commit_ack_lost")

        with monkeypatch.context() as scoped:
            scoped.setattr(AsyncSession, "commit", commit_then_lose_ack)
            with pytest.raises(
                _CommitAcknowledgementLostError, match="next38_actual_commit_ack_lost"
            ):
                await record_native_position_scenario_v3(scenario)
        committed = await _counts(scenario.source.repository)
        _assert_one_recording(before, committed)
        reopened = SQLAlchemyRepository(scenario.source.repository.db_url)
        try:
            recovered = await record_native_position_v3(
                reopened, scenario.request, scenario.identity, scenario.source.policy
            )
            assert not recovered.created
            await _assert_retained_graph(scenario, recovered)
            await write_financial_owner_policy(
                reopened,
                FinancialOwnerPolicyChange(
                    policy=FinancialAccountOwnerPolicy(assignments=()),
                    updated_by="next38-migrated-revocation",
                ),
                scenario.source.tracker,
            )
            replay = await record_native_position_v3(
                reopened,
                scenario.request,
                scenario.identity,
                NativeAuthorityFreshnessPolicy(
                    timedelta(microseconds=1), timedelta(microseconds=1)
                ),
            )
            assert not replay.created and replay == recovered
            assert await _counts(reopened) == committed
        finally:
            await reopened.engine.dispose()
