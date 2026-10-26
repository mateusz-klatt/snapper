"""Genuine migrated inventory storage and signed SDK HTTP under an isolated source adapter.

PostgreSQL shares the existing explicit owner-only Unix-socket receipt and
disposable-database guard. Neither database path invokes ORM create_all. The
independent observer runs on another real connection at the original Requests
transport boundary, before the local response is supplied.
"""

import asyncio
import json
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Literal
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from requests import ConnectionError as RequestsConnectionError
from requests import PreparedRequest
from requests import Response
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.pool import NullPool
from urllib3.response import HTTPResponse

import snapper.application.trade.current_futures_inventory as current_reader
import snapper.data.futures_inventory_read_store as inventory_store
import snapper.infrastructure.exchanges.futures_inventory_wire as inventory_wire
from snapper.core.futures_inventory_read import FuturesInventoryReadAttempt
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.data.models import FuturesInventoryReadAttempt as InventoryAttemptRow
from snapper.data.models import FuturesInventoryReadOutcome as InventoryOutcomeRow
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from tests.application.trade.test_native_position_recording_v3_migrated import _CommittedObserver
from tests.application.trade.test_native_position_recording_v3_migrated import _sync_url
from tests.helpers.native_account_authority import NativeAuthorityDatabase
from tests.helpers.native_account_authority import NativeAuthorityHttpAdapter
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_scenario

INVENTORY_PATHS: dict[InventoryReadPurpose, str] = {
    "kraken_futures.open_orders": "/derivatives/api/v3/openorders",
    "kraken_futures.open_positions": "/derivatives/api/v3/openpositions",
}
type InventorySourceFault = Literal["key", "signature", "nonce", "duplicate", "malformed"]


def _uuid_text(value: object) -> str:
    """Normalize only a real driver's equivalent UUID representation for public test identity."""
    if isinstance(value, UUID):
        return str(value)
    assert isinstance(value, str)
    return value


@dataclass(frozen=True, slots=True)
class InventorySendObservation:
    """Committed request, immutable key proof and absence of an outcome at actual inventory send."""

    attempt_public_id: str
    attempt: FuturesInventoryReadAttempt
    proof_public_id: str
    proof: NativeAccountIdentityProofPayload


@dataclass
class InventoryCommittedObserver:
    """Observe actual committed inventory rows independently of the producer transaction."""

    engine: Engine
    seen: list[InventorySendObservation] = field(default_factory=list)

    def inventory_send(self, purpose: InventoryReadPurpose) -> InventorySendObservation:
        """Require the original request and exact safe proof to be durable before its GET response.

        Args:
            purpose: Actual fixed endpoint selected by the original signed PreparedRequest.

        Returns:
            Actual independently visible request/proof binding with no already committed outcome.
        """
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT a.public_id, a.canonical_payload, p.public_id, p.canonical_payload, "
                    "(SELECT count(*) FROM futures_inventory_read_outcomes AS o "
                    "WHERE o.attempt_public_id=a.public_id) "
                    "FROM futures_inventory_read_attempts AS a "
                    "JOIN native_account_identity_proofs AS p "
                    "ON a.key_proof_public_id=p.public_id "
                    "AND a.key_proof_content_digest=p.content_digest "
                    "WHERE a.purpose=:purpose ORDER BY a.family_sequence DESC LIMIT 1"
                ),
                {"purpose": purpose},
            ).one()
        assert isinstance(row[1], str) and isinstance(row[3], str) and row[4] == 0
        observed = InventorySendObservation(
            _uuid_text(row[0]),
            FuturesInventoryReadAttempt.model_validate_json(row[1]),
            _uuid_text(row[2]),
            NativeAccountIdentityProofPayload.model_validate_json(row[3]),
        )
        assert observed.attempt.purpose == purpose
        assert observed.attempt.key_proof.public_id == observed.proof_public_id
        assert observed.attempt.key_proof.account_uid == observed.proof.observation.account_uid
        assert observed.proof.matches_owner
        self.seen.append(observed)
        return observed


@dataclass(frozen=True, slots=True)
class MigratedInventoryDatabase:
    """Actual blank-to0066 catalog, source repository and independent synchronous observer."""

    native: NativeAuthorityDatabase
    observer: InventoryCommittedObserver


def _upgrade_empty_inventory(url: URL) -> None:
    """Apply the actual complete migration graph to one newly owned blank database."""
    root = Path(__file__).resolve().parents[2]
    observer = create_engine(_sync_url(url), poolclass=NullPool)
    try:
        with observer.connect() as connection:
            assert inspect(connection).get_table_names() == []
    finally:
        observer.dispose()
    config = Config()
    config.set_main_option("path_separator", "newline")
    config.set_main_option("script_location", str(root / "src/snapper/data/migrations"))
    config.set_main_option("version_locations", str(root / "src/snapper/data/migrations/versions"))
    config.set_main_option(
        "sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%")
    )
    command.upgrade(config, "0066")


@asynccontextmanager
async def migrated_inventory_database(url: URL) -> AsyncIterator[MigratedInventoryDatabase]:
    """Own genuinely migrated storage without using the autouse ORM schema template.

    Args:
        url: Explicitly owned blank SQLite file or disposable local PostgreSQL database.

    Yields:
        Real isolated repository and independently connected original-source observers.
    """
    await asyncio.to_thread(_upgrade_empty_inventory, url)
    repository = SQLAlchemyRepository(url.render_as_string(hide_password=False))
    observer = InventoryCommittedObserver(create_engine(_sync_url(url), poolclass=NullPool))
    native_observer = _CommittedObserver(observer.engine)
    try:
        async with repository.session() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0066"
            now = datetime.now(UTC)
            symbol = (
                await session.scalars(
                    select(Symbol).where(
                        Symbol.native_symbol == "BTC-USD",
                        Symbol.timestamp <= now,
                        Symbol.known_to > now,
                    )
                )
            ).one()
        native = NativeAuthorityDatabase(
            repository, native_observer, native_observer.identity_proofs, symbol
        )
        yield MigratedInventoryDatabase(native, observer)
    finally:
        await repository.engine.dispose()
        observer.engine.dispose()


class MigratedInventoryHttpAdapter(NativeAuthorityHttpAdapter):
    """Feed original signed SDK reads while retaining actual independent SQL visibility evidence."""

    def __init__(
        self, native: NativeAuthorityScenario, database: MigratedInventoryDatabase
    ) -> None:
        """Extend only the two fixed inventory endpoint replies in an otherwise genuine SDK flow."""
        super().__init__(
            native.adapter.path,
            native.clock,
            database.native.observe_committed,
            database.native.observe_identity_proofs,
        )
        self.observer = database.observer
        self.inventory_calls: list[PreparedRequest] = []
        self.original_bodies: list[bytes] = []
        self.fail_purpose: InventoryReadPurpose | None = None
        self.block_purpose: InventoryReadPurpose | None = None
        self.source_fault: InventorySourceFault | None = None
        self.empty = False
        self.inventory_entered = threading.Event()
        self.inventory_release = threading.Event()

    def _original_body(self, purpose: InventoryReadPurpose) -> bytes:
        """Retain exact original decimal lexemes and mature contracts without parser substitutes."""
        clock = json.dumps(self.clock.current.isoformat().replace("+00:00", "Z")).encode()
        prefix = b'{"result":"success","serverTime":' + clock
        if self.empty:
            array = b'"openOrders"' if purpose.endswith("open_orders") else b'"openPositions"'
            return prefix + b"," + array + b":[]}"
        if purpose == "kraken_futures.open_orders":
            return (
                prefix + b',"openOrders":[{"order_id":"00000000-0000-7000-8000-000000000083",'
                b'"cliOrdId":"original-local-command","symbol":"PF_XBTUSD","side":"buy",'
                b'"orderType":"lmt","status":"untouched","filledSize":0.125000000000000000001,'
                b'"unfilledSize":0.375,"limitPrice":12345.000000000000000000001,'
                b'"reduceOnly":false,"receivedTime":"2026-10-24T23:59:58Z",'
                b'"lastUpdateTime":"2026-10-24T23:59:59Z"}]}'
            )
        return (
            prefix + b',"openPositions":[{"symbol":"FF_XBTUSD_260925","side":"short",'
            b'"size":1.250000000000000000001,"price":12345.000000000000000000001,'
            b'"unrealizedPnl":-2.5,"unrealizedFunding":null,"pnlCurrency":"USD"}]}'
        )

    def _response_body(self, request: PreparedRequest, purpose: InventoryReadPurpose) -> bytes:
        """Supply bounded adversarial original entities before the actual privacy/parser checks."""
        body = self._original_body(purpose)
        if self.source_fault == "malformed":
            return body.replace(b'"filledSize":0.125000000000000000001,', b"")
        if self.source_fault == "duplicate":
            return body.replace(b'"result":"success",', b'"result":"success","result":"success",')
        if self.source_fault is None:
            return body
        header = {"key": "APIKey", "signature": "Authent", "nonce": "Nonce"}[self.source_fault]
        echoed = json.dumps({"innocent": request.headers[header]}).encode()
        return body[:-1] + b',"extra":' + echoed + b"}"

    def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
        """Observe real durable attempts at final signed transport, then return controlled bytes.

        Args:
            request: Original final SDK request with synthetic credentials and original provider URL.
            args: Requests adapter positional arguments.
            kwargs: Actual routed timeout, proxy and streaming configuration.

        Returns:
            A genuine streaming Response read by the unpatched issued-receipt transport.
        """
        purpose = next(
            (name for name, path in INVENTORY_PATHS.items() if (request.url or "").endswith(path)),
            None,
        )
        if purpose is None:
            return super().send(request, *args, **kwargs)
        assert request.method == "GET" and request.body is None
        assert request.headers["APIKey"] and request.headers["Authent"]
        self.calls.append(request)
        self.inventory_calls.append(request)
        self.observer.inventory_send(purpose)
        if self.block_purpose == purpose:
            self.inventory_entered.set()
            if not self.inventory_release.wait(5):
                raise TimeoutError("Local inventory response release was not delivered")
        if self.fail_purpose == purpose:
            raise RequestsConnectionError("synthetic-inventory-transport-failure")
        body = self._response_body(request, purpose)
        self.original_bodies.append(body)
        response = Response()
        response.status_code = 200
        response.url = request.url or ""
        response.request = request
        response.raw = HTTPResponse(body=BytesIO(body), status=200, preload_content=False)
        self.responses.append(response)
        return response


@dataclass(frozen=True, slots=True)
class MigratedInventoryScenario:
    """Actual source client, deterministic trusted clock and independent migrated-database checks."""

    native: NativeAuthorityScenario
    adapter: MigratedInventoryHttpAdapter
    database: MigratedInventoryDatabase


@dataclass
class InventoryCommitAcknowledgement:
    """Track one injected reply loss after the original SQL commit has actually completed."""

    target: Literal["attempt", "outcome"]
    lost: bool = False


@asynccontextmanager
async def lose_inventory_commit_acknowledgement(
    monkeypatch: pytest.MonkeyPatch, target: Literal["attempt", "outcome"]
) -> AsyncIterator[InventoryCommitAcknowledgement]:
    """Lose exactly one post-commit reply while preserving genuine transactions and durable rows.

    Args:
        monkeypatch: Scoped commit-reply injection, restored before normal fixture cleanup.
        target: Actual inventory attempt or captured-outcome commit whose acknowledgement is lost.

    Yields:
        A receipt proving the selected original database commit completed before the injected error.
    """
    original_commit = AsyncSession.commit
    acknowledgement = InventoryCommitAcknowledgement(target)

    async def commit_then_lose_reply(session: AsyncSession) -> None:
        """Run the real commit before injecting only the requested inventory reply failure."""
        lose_reply = False
        if not acknowledgement.lost:
            model = InventoryAttemptRow if target == "attempt" else InventoryOutcomeRow
            query = select(func.count()).select_from(model)
            if target == "outcome":
                query = query.where(InventoryOutcomeRow.result_kind == "captured")
            lose_reply = bool(await session.scalar(query))
        await original_commit(session)
        if lose_reply:
            acknowledgement.lost = True
            raise ConnectionError("Synthetic inventory acknowledgement lost after actual commit")

    with monkeypatch.context() as scoped:
        scoped.setattr(AsyncSession, "commit", commit_then_lose_reply)
        yield acknowledgement


@asynccontextmanager
async def migrated_inventory_scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, database: MigratedInventoryDatabase
) -> AsyncIterator[MigratedInventoryScenario]:
    """Own installed SDK clients and attach only actual local HTTP/clock boundaries.

    Args:
        tmp_path: Caller-owned test namespace, never a deployment path or credential source.
        monkeypatch: Fixture-owned trusted clock and routing patches, without parser/store stubs.
        database: Genuinely migrated source catalog and its independent observer connection.

    Yields:
        Real source preparation, inventory transport/store and current-reader dependencies.
    """
    async with native_authority_scenario(tmp_path, monkeypatch, database.native) as native:
        adapter = MigratedInventoryHttpAdapter(native, database)
        native.adapter = adapter
        for session in native.sessions:
            session.mount("https://", adapter)
        for module in (inventory_store, inventory_wire, current_reader):
            monkeypatch.setattr(module, "datetime", native.clock)
        try:
            yield MigratedInventoryScenario(native, adapter, database)
        finally:
            adapter.inventory_release.set()
            adapter.accounts_release.set()
            adapter.close()
