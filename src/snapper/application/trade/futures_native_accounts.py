"""Interpret native account quantities from one retained original Futures REST response.

Exact REST account keys remain separate profiles. This source reader establishes
neither a WebSocket alias, available collateral, current account-read authority
nor permission to submit an order. The response must already be durably retained.
"""

import base64
import re
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import InvalidOperation
from typing import Literal
from typing import cast

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.models import AccountingRawReceipt
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import is_execution_control_message

FUTURES_NATIVE_ACCOUNTS_CHANNEL = "kraken_futures.rest.accounts.v1"
FUTURES_PREPARED_ACCOUNTS_CHANNEL = "kraken_futures.rest.accounts.prepared-key.v2"
type NativeAccountType = Literal["cashAccount", "marginAccount", "multiCollateralMarginAccount"]


class NativeAccountsSourceError(ValueError):
    """The retained source does not prove one supported native account inventory."""


class _NativeAccountsEvidence(BaseModel):
    """Keep exact source interpretation immutable and reject unchecked instance mutation."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class NativeAccountQuantity(_NativeAccountsEvidence):
    """One verbatim currency's native quantity, with no free/used or valuation inference."""

    currency: EvidenceText
    quantity: EvidenceDecimal


class NativeAccountComponent(_NativeAccountsEvidence):
    """An exact non-settlement balance entry whose cash or contract unit is not inferred."""

    native_key: EvidenceText
    quantity: EvidenceDecimal


class FuturesNativeAccountProfile(_NativeAccountsEvidence):
    """A literal REST account key and separately retained native currency inventory."""

    native_account_key: EvidenceText
    account_type: NativeAccountType
    settlement_currency: EvidenceText | None = None
    quantities: tuple[NativeAccountQuantity, ...] = Field(max_length=4096)
    unclassified_components: tuple[NativeAccountComponent, ...] = Field(default=(), max_length=4096)


class FuturesNativeAccountsSnapshot(_NativeAccountsEvidence):
    """One complete supported source inventory, independent of financial/account-read authority."""

    source_policy: Literal[
        "kraken_futures_accounts_v1", "kraken_futures_accounts_prepared_key_v2"
    ] = "kraken_futures_accounts_v1"
    server_time_text: EvidenceText
    server_time: EvidenceInstant
    profiles: tuple[FuturesNativeAccountProfile, ...] = Field(max_length=256)


class FuturesNativeAccountsRequest(_NativeAccountsEvidence):
    """Actual retained receipt and selected public client identity, with no caller-supplied amounts."""

    raw_receipt_public_id: EvidenceUUID
    client_selection: ExecutionClientSelection


class BoundFuturesNativeAccounts(_NativeAccountsEvidence):
    """Verified source inventory and its distinct capture and local recording clocks."""

    request: FuturesNativeAccountsRequest
    source_observed_at: EvidenceInstant
    source_recorded_at: EvidenceInstant
    snapshot: FuturesNativeAccountsSnapshot


def _object(value: object) -> Mapping[str, object]:
    """Require a source object without normalizing currencies or discarding unknown structures."""
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise NativeAccountsSourceError("native_accounts_object_required")
    return value


def _text(value: object) -> str:
    """Retain exact nonblank spelling rather than trim an account or currency identity."""
    if not isinstance(value, str) or not value.strip():
        raise NativeAccountsSourceError("native_accounts_identity_required")
    return value


def _quantity(value: object, *, allow_text: bool = False) -> Decimal:
    """Accept original JSON numeric operands without float, boolean or valuation promotion."""
    if allow_text and isinstance(value, str):
        if re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?", value) is None:
            raise NativeAccountsSourceError("native_accounts_decimal_text_invalid")
        try:
            return Decimal(value)
        except InvalidOperation as error:
            raise NativeAccountsSourceError("native_accounts_decimal_text_invalid") from error
    if isinstance(value, bool) or not isinstance(value, int | Decimal):
        raise NativeAccountsSourceError("native_accounts_exact_quantity_required")
    return Decimal(value)


def _server_time(value: object) -> tuple[str, datetime]:
    """Preserve the provider's original timestamp text and require its documented UTC clock."""
    source = _text(value)
    try:
        parsed = datetime.fromisoformat(source)
    except ValueError as error:
        raise NativeAccountsSourceError("native_accounts_server_time_invalid") from error
    if parsed.utcoffset() != timedelta(0):
        raise NativeAccountsSourceError("native_accounts_server_time_not_utc")
    return source, parsed.astimezone(UTC)


def _balance_components(source: Mapping[str, object]) -> tuple[NativeAccountComponent, ...]:
    """Retain mixed legacy balance entries without calling a contract symbol a currency."""
    return tuple(
        NativeAccountComponent(native_key=_text(key), quantity=_quantity(value, allow_text=True))
        for key, value in _object(source.get("balances")).items()
    )


def _legacy_profile(key: str, source: Mapping[str, object]) -> FuturesNativeAccountProfile:
    """Separate explicit settlement currency from unclassified legacy contract/asset entries."""
    margin = source["type"] == "marginAccount"
    settlement = _text(source.get("currency")) if margin else None
    components = _balance_components(source)
    quantities = tuple(
        NativeAccountQuantity(currency=entry.native_key, quantity=entry.quantity)
        for entry in components
        if settlement is None or entry.native_key == settlement
    )
    if margin and not quantities:
        raise NativeAccountsSourceError("native_accounts_settlement_quantity_missing")
    return FuturesNativeAccountProfile(
        native_account_key=_text(key),
        account_type="marginAccount" if margin else "cashAccount",
        settlement_currency=settlement,
        quantities=quantities,
        unclassified_components=tuple(
            entry for entry in components if margin and entry.native_key != settlement
        ),
    )


def _profile(key: str, value: object) -> FuturesNativeAccountProfile:
    """Select native balances by explicit account type, never by a familiar-looking account key."""
    source = _object(value)
    account_type = source.get("type")
    if account_type not in ("cashAccount", "marginAccount", "multiCollateralMarginAccount"):
        raise NativeAccountsSourceError("native_accounts_type_unsupported")
    if account_type != "multiCollateralMarginAccount":
        return _legacy_profile(key, source)
    balances = _object(source.get("currencies"))
    quantities = tuple(
        NativeAccountQuantity(
            currency=_text(currency),
            quantity=_quantity(_object(amount).get("quantity")),
        )
        for currency, amount in balances.items()
    )
    return FuturesNativeAccountProfile(
        native_account_key=_text(key),
        account_type=account_type,
        quantities=quantities,
    )


def parse_futures_native_accounts(
    observation: RawAccountingObservation,
) -> FuturesNativeAccountsSnapshot:
    """Read all supported profiles from exact source bytes, refusing incomplete source envelopes.

    Args:
        observation: Original complete REST response retained before numeric interpretation.

    Returns:
        Separate literal account profiles and native quantities, without account-log alias inference.
    """
    raw = RawAccountingObservation.model_validate(observation)
    if (
        raw.scope.exchange != "kraken_futures"
        or raw.kind != "account_cash"
        or raw.channel not in {FUTURES_NATIVE_ACCOUNTS_CHANNEL, FUTURES_PREPARED_ACCOUNTS_CHANNEL}
        or raw.target_json_pointer is not None
        or raw.representation != "wire_bytes_base64"
    ):
        raise NativeAccountsSourceError("native_accounts_original_source_required")
    try:
        document = decode_private_execution_receipt(base64.b64decode(raw.material, validate=True))
    except (ValueError, UnicodeError) as error:
        raise NativeAccountsSourceError("native_accounts_original_json_invalid") from error
    if is_execution_control_message(document) or document.get("result") != "success":
        raise NativeAccountsSourceError("native_accounts_success_unproven")
    time_text, server_time = _server_time(document.get("serverTime"))
    accounts = _object(document.get("accounts"))
    return FuturesNativeAccountsSnapshot(
        source_policy=(
            "kraken_futures_accounts_prepared_key_v2"
            if raw.channel == FUTURES_PREPARED_ACCOUNTS_CHANNEL
            else "kraken_futures_accounts_v1"
        ),
        server_time_text=time_text,
        server_time=server_time,
        profiles=tuple(_profile(key, value) for key, value in accounts.items()),
    )


def _verify_selection(raw: RawAccountingObservation, selection: ExecutionClientSelection) -> None:
    """Bind only the actual retained credential/client revision and supported public environment."""
    origins = {
        "production": "https://futures.kraken.com",
        "demo": "https://demo-futures.kraken.com",
    }
    actual = (
        raw.scope.wallet_public_id,
        raw.scope.exchange,
        raw.scope.mode,
        raw.scope.environment,
        raw.credential_public_id,
        raw.credential_timestamp,
        raw.route_revision,
    )
    expected = (
        selection.wallet_public_id,
        selection.exchange,
        selection.mode,
        selection.environment,
        selection.credential_public_id,
        selection.credential_timestamp,
        client_selection_digest(selection),
    )
    if (
        actual != expected
        or selection.exchange != "kraken_futures"
        or selection.public_api_origin != origins.get(selection.environment)
    ):
        raise NativeAccountsSourceError("native_accounts_client_selection_mismatch")
    if cast(datetime, selection.credential_timestamp) > raw.observed_at:
        raise NativeAccountsSourceError("native_accounts_capture_precedes_credential_revision")


async def get_bound_futures_native_accounts(
    session: AsyncSession, request: FuturesNativeAccountsRequest, known_at: datetime
) -> BoundFuturesNativeAccounts:
    """Reparse actual immutable account source without granting current financial authority.

    Args:
        session: Caller-owned transaction; no account state or financial row is written.
        request: Exact source receipt and selected public client revision.
        known_at: Actual local knowledge boundary, separate from the provider's clock.

    Returns:
        Verified source inventory and original clocks; current-read freshness remains separate.
    """
    evidence = FuturesNativeAccountsRequest.model_validate(request)
    if known_at.utcoffset() is None:
        raise NativeAccountsSourceError("native_accounts_knowledge_clock_not_aware")
    raw = await get_bound_accounting_observation(session, evidence.raw_receipt_public_id)
    _verify_selection(raw, evidence.client_selection)
    recorded_at = (
        await session.execute(
            select(AccountingRawReceipt.timestamp).where(
                AccountingRawReceipt.public_id == evidence.raw_receipt_public_id
            )
        )
    ).scalar_one()
    if max(recorded_at, raw.observed_at) > known_at:
        raise NativeAccountsSourceError("native_accounts_source_not_yet_known")
    return BoundFuturesNativeAccounts(
        request=evidence,
        source_observed_at=raw.observed_at,
        source_recorded_at=recorded_at,
        snapshot=parse_futures_native_accounts(raw),
    )
