"""Real isolated account, command and catalog observations for admission witnesses."""

import json
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Literal

from sqlalchemy import select

from snapper.application.portfolio.spot_precision_certification import KRAKEN_INSTRUMENT_SPEC_SOURCE
from snapper.application.updaters.symbols.polygon_cash_profile import POLYGON_CASH_PROFILE_SOURCE
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import ExecutionRouteSelection
from snapper.core.execution_route_authority import route_authority_content_digest
from snapper.core.execution_route_authority import route_selection_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_admission import SubmissionEvidenceIdentity
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.data.execution_route_authority_store import insert_execution_route_authority
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from snapper.data.models import TradeCommand
from snapper.data.models import VenueAccountObservation
from snapper.data.models import VenueAccountState
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import VenueAccountAttemptRow
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_ETL_VERSION
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE
from snapper.messaging.schemas.data import OrderRequestData

type AdmissionSource = Literal["kraken", "kraken_futures", "polygon"]


def admission_id(number: int) -> str:
    """Return explicit canonical fixture identities without an ORM default dependency.

    Args:
        number: Isolated semantic identity number.

    Returns:
        A canonical UUID used exclusively in the isolated fixture.
    """
    return f"00000000-0000-7000-8000-{number:012d}"


def admission_stamp(number: int, at: datetime) -> SnapshotWriteProvenance:
    """Build an actual insertion envelope for the isolated witness.

    Args:
        number: Independent fixture record identity.
        at: Explicit producer knowledge clock.

    Returns:
        A strict immutable-writer envelope.
    """
    return SnapshotWriteProvenance(
        public_id=admission_id(number),
        session_id=admission_id(99),
        sequence_id=number,
        timestamp=at,
    )


def admission_identity(number: int = 96) -> SubmissionEvidenceIdentity:
    """Keep a submit attempt, original observation and admission record independently identifiable.

    Args:
        number: Actual fixture attempt identity.

    Returns:
        Explicit distinct immutable producer identities.
    """
    return SubmissionEvidenceIdentity(
        attempt_public_id=admission_id(number),
        admission=ExecutionEvidenceIdentity(
            public_id=admission_id(number + 1000),
            session_id=admission_id(99),
            sequence_id=number + 1000,
        ),
        command_snapshot=ExecutionEvidenceIdentity(
            public_id=admission_id(number + 2000),
            session_id=admission_id(99),
            sequence_id=number + 2000,
        ),
    )


@dataclass(frozen=True)
class AdmissionScenario:
    """One real isolated execution frame and its independently observed account route."""

    repository: SQLAlchemyRepository
    now: datetime
    order: OrderRequestData
    authority: ExecutionRouteAuthorityPayload
    route: ExecutionRouteReference
    source_exchange: AdmissionSource


def _source_spec(source: AdmissionSource, at: datetime, instrument_id: str) -> InstrumentSpec:
    """Provide actual metadata with explicit native/internal units and payoff dimensions."""
    futures = source == "kraken_futures"
    source_name = {
        "kraken": KRAKEN_INSTRUMENT_SPEC_SOURCE,
        "kraken_futures": FUTURES_SPEC_SOURCE,
        "polygon": POLYGON_CASH_PROFILE_SOURCE,
    }[source]
    return InstrumentSpec(
        public_id=admission_id(4 if instrument_id == admission_id(2) else 141),
        session_id=admission_id(99),
        sequence_id=4,
        timestamp=at,
        instrument_public_id=instrument_id,
        tick_size=None if source == "polygon" else 0.5,
        lot_size=None if source == "polygon" else 0.0001,
        min_order_size=None if source == "polygon" else 0.0001,
        qty_decimals=None if source == "polygon" else 4,
        status="active",
        instrument_kind="perpetual" if futures else "spot",
        contract_size=Decimal(1) if futures else None,
        quantity_unit="contract_count" if futures else "base_asset",
        native_order_quantity_unit="base_asset",
        contract_payoff_model="linear" if futures else None,
        contract_value_currency="BTC" if futures else None,
        settlement_currency="USD" if futures else None,
        spec_source=source_name,
        spec_version=f"{FUTURES_SPEC_ETL_VERSION}:original" if futures else "fixture-etl:original",
        spec_observed_at=at,
        unit_certified=futures,
    )


async def _seed_catalog(
    repository: SQLAlchemyRepository,
    now: datetime,
    source: AdmissionSource,
    paper: bool,
) -> OrderRequestData:
    """Persist actual source/Paper identities plus the original durable CREATE."""
    exchange: OrderExchange
    if paper:
        exchange = ExchangeEnum.PAPER
    elif source == "kraken":
        exchange = ExchangeEnum.KRAKEN
    elif source == "kraken_futures":
        exchange = ExchangeEnum.KRAKEN_FUTURES
    else:
        raise ValueError("Polygon scenarios require a Paper execution target")
    mode: Literal["live", "paper"] = "paper" if paper else "live"
    spec_at = now - timedelta(hours=1)
    intent_at = now - timedelta(seconds=30)
    native = "AAPL" if source == "polygon" else "BTC-USD"
    shard = f"{exchange}.{native}.{mode}" + (".strategy-test" if paper else "")
    rows = [
        Symbol(
            **admission_stamp(1, spec_at).model_dump(mode="python"),
            native_symbol=native,
            base="AAPL" if source == "polygon" else "BTC",
            quote="USD",
            asset_type="equity" if source == "polygon" else "crypto",
            created_at=spec_at,
        ),
        Instrument(
            **admission_stamp(2, spec_at).model_dump(mode="python"),
            symbol_public_id=admission_id(1),
            exchange=source,
        ),
        _source_spec(source, spec_at, admission_id(2)),
        Wallet(
            **admission_stamp(3, now - timedelta(hours=2)).model_dump(mode="python"),
            label="isolated-route-wallet",
            is_paper=paper,
        ),
        WalletCredential(
            **admission_stamp(90, now - timedelta(hours=2)).model_dump(mode="python"),
            wallet_public_id=admission_id(3),
            exchange=exchange,
            credential_type="paper" if paper else "api_key_secret",
            encrypted_payload="isolated-fixture-value-never-decrypted",
        ),
        TradeCommand(
            **admission_stamp(5, intent_at).model_dump(mode="python"),
            command_type="create",
            shard_key=shard,
            wallet_public_id=admission_id(3),
            exchange=exchange,
            instrument=native,
            mode=mode,
            strategy_id="manual",
            client_order_id="client-1",
            venue_client_id="client-1",
            side="buy",
            order_type="limit",
            quantity=2.0,
            price=100.0,
            post_only=True,
            reduce_only=False,
            status="created",
            created_at=intent_at,
            correlation_id=admission_id(15),
        ),
    ]
    if paper:
        rows.extend(
            [
                Instrument(
                    **admission_stamp(140, spec_at).model_dump(mode="python"),
                    symbol_public_id=admission_id(1),
                    exchange="paper",
                    source_exchange=source,
                ),
                _source_spec(source, spec_at, admission_id(140)),
            ]
        )
    async with repository.session() as session:
        session.add_all(rows)
        await session.commit()
    return OrderRequestData(
        public_id="client-1",
        timestamp=intent_at,
        session_id=admission_id(99),
        sequence_id=1,
        strategy_id="manual",
        exchange=exchange,
        instrument=native,
        mode=mode,
        side="buy",
        order_type="limit",
        quantity=2.0,
        price=100.0,
        post_only=True,
        client_order_id="client-1",
        wallet_public_id=admission_id(3),
        strategy_tag="strategy-test" if paper else None,
    )


async def _seed_account(
    repository: SQLAlchemyRepository, order: OrderRequestData, now: datetime
) -> ExecutionRouteAuthorityPayload:
    """Record an actual repository account snapshot before constructing its route witness."""
    paper = order.mode == "paper"
    at = now - timedelta(seconds=10)
    balances = json.dumps(
        [
            {
                "currency": "USD",
                "total": 1000.0,
                "free": 1000.0,
                "used": 0.0,
                "total_decimal": "1000.00",
                "free_decimal": "1000.00",
                "used_decimal": "0.00",
                "numeric_provenance": "venue_raw",
            }
        ]
    )
    positions = order.exchange == "kraken_futures"
    attempt: VenueAccountAttemptRow = {
        "wallet_public_id": admission_id(3),
        "exchange": order.exchange,
        "mode": order.mode,
        "balance_status": "simulated" if paper else "observed",
        "position_status": "observed" if positions else "not_applicable",
        "valuation_status": "native_only",
        "balances_json": balances,
        "open_positions_json": "[]" if positions else None,
        "balance_observed_at": at,
        "position_observed_at": at if positions else None,
        "authoritative_until": now + timedelta(minutes=2),
        "error": None,
        "session_id": admission_id(99),
        "sequence_id": 91,
        "bus_time": at,
    }
    state_id = await repository.record_venue_account_snapshot(attempt)
    async with repository.session() as session:
        state = (
            await session.execute(select(VenueAccountState).where(VenueAccountState.id == state_id))
        ).scalar_one()
        observation = (
            await session.execute(
                select(VenueAccountObservation).where(
                    VenueAccountObservation.id == state.current_attempt_observation_id
                )
            )
        ).scalar_one()
    scope = ExecutionAccountScope(
        wallet_public_id=admission_id(3),
        exchange=order.exchange,
        mode=order.mode,
        environment="paper" if paper else "production",
        margin_account=("paper.cash" if paper else "futures.flex" if positions else "spot.cash"),
    )
    origin = (
        None
        if paper
        else (
            "https://futures.kraken.com"
            if order.exchange == "kraken_futures"
            else "https://api.kraken.com"
        )
    )
    credential_at = now - timedelta(hours=2)
    return ExecutionRouteAuthorityPayload(
        scope=scope,
        credential_public_id=admission_id(90),
        credential_timestamp=credential_at,
        public_api_origin=origin,
        account_observation_public_id=observation.public_id,
        account_observation_timestamp=observation.timestamp,
        balance_observed_at=at,
        balance_receipt_json=balances,
        observed_at=now,
        valid_until=now + timedelta(minutes=2),
        route_revision=route_selection_digest(scope, admission_id(90), credential_at, origin),
    )


async def seed_admission_scenario(
    repository: SQLAlchemyRepository,
    now: datetime,
    source: AdmissionSource = "kraken",
    paper: bool = False,
) -> AdmissionScenario:
    """Persist real isolated source, intent, account and immutable route authority.

    Args:
        repository: Actual isolated SQLite repository with its real create-all schema.
        now: Deterministic actual producer clock used by the scenario.
        source: Explicit real source venue, independent of a Paper target.
        paper: Select simulated execution and its own distinct instrument UUID.

    Returns:
        A genuine durable scenario without an exchange client, server or production access.
    """
    order = await _seed_catalog(repository, now, source, paper)
    authority = await _seed_account(repository, order, now)
    result = await insert_execution_route_authority(repository, authority, admission_stamp(93, now))
    route = ExecutionRouteReference(
        authority_public_id=result.public_id,
        authority_content_digest=route_authority_content_digest(authority),
        scope=authority.scope,
        route_revision=authority.route_revision,
    )
    return AdmissionScenario(repository, now, order, authority, route, source)


async def refresh_admission_account(
    scenario: AdmissionScenario,
    changes: dict[str, object],
    at: datetime,
    number: int,
) -> ExecutionRouteReference:
    """Persist a real later account cycle and selected public-route witness.

    Args:
        scenario: Isolated original command and actual account observations.
        changes: Explicit changed public selection or observed venue account identity.
        at: Actual later response and interpretation clock.
        number: Independent durable observation and authority identity.

    Returns:
        A fresh bound route without issuing an SDK request or granting resend permission.
    """
    authority = scenario.authority
    attempt: VenueAccountAttemptRow = {
        "wallet_public_id": authority.scope.wallet_public_id,
        "exchange": authority.scope.exchange,
        "mode": authority.scope.mode,
        "balance_status": "observed",
        "position_status": "observed",
        "valuation_status": "native_only",
        "balances_json": authority.balance_receipt_json,
        "open_positions_json": "[]",
        "balance_observed_at": at,
        "position_observed_at": at,
        "authoritative_until": at + timedelta(minutes=2),
        "error": None,
        "session_id": admission_id(99),
        "sequence_id": number,
        "bus_time": at,
    }
    state_id = await scenario.repository.record_venue_account_snapshot(attempt)
    async with scenario.repository.session() as session:
        observation = (
            await session.execute(
                select(VenueAccountObservation)
                .join(
                    VenueAccountState,
                    VenueAccountState.current_attempt_observation_id == VenueAccountObservation.id,
                )
                .where(VenueAccountState.id == state_id)
            )
        ).scalar_one()
    material = {**authority.model_dump(mode="python"), **changes}
    selected = ExecutionRouteSelection.model_validate(
        {key: material[key] for key in ExecutionRouteSelection.model_fields}
    )
    material.update(
        account_observation_public_id=observation.public_id,
        account_observation_timestamp=observation.timestamp,
        balance_observed_at=at,
        observed_at=at,
        valid_until=at + timedelta(minutes=2),
        route_revision=route_selection_digest(
            selected.scope,
            selected.credential_public_id,
            selected.credential_timestamp,
            selected.public_api_origin,
        ),
    )
    payload = ExecutionRouteAuthorityPayload.model_validate(material)
    result = await insert_execution_route_authority(
        scenario.repository, payload, admission_stamp(number, at)
    )
    return ExecutionRouteReference(
        authority_public_id=result.public_id,
        authority_content_digest=result.content_digest,
        scope=selected.scope,
        route_revision=payload.route_revision,
    )
