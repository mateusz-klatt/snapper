"""Read exact immutable raw evidence before promoting it into a financial fact.

Capture remains permissive. This financial reader verifies the envelope and
indexed projections and refuses changed-identity conflict receipts. It grants
neither current account authority nor a successful economic interpretation.
"""

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import accounting_hold_scope_digest
from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.core.contract_accounting_observation import canonical_accounting_observation
from snapper.data.models import AccountingRawReceipt


class AccountingObservationBindingError(ValueError):
    """An exact source receipt is absent, conflicted or inconsistent with its immutable payload."""


async def get_bound_accounting_observation(
    session: AsyncSession, public_id: str
) -> RawAccountingObservation:
    """Validate retained raw material and every indexed provenance copy.

    Args:
        session: Caller-owned financial or replay transaction; no new fence is acquired.
        public_id: Exact immutable local observation UUID retained by Stage A.

    Returns:
        Original verified envelope, still requiring actual route and source interpretation.

    Raises:
        AccountingObservationBindingError: If retained identity or material is not trustworthy.
    """
    row = (
        await session.execute(
            select(AccountingRawReceipt).where(AccountingRawReceipt.public_id == public_id)
        )
    ).scalar_one_or_none()
    if row is None:
        raise AccountingObservationBindingError("accounting_raw_receipt_missing")
    try:
        raw = RawAccountingObservation.model_validate_json(row.canonical_payload)
    except ValidationError as error:
        raise AccountingObservationBindingError("accounting_raw_receipt_payload_invalid") from error
    scope = raw.scope
    actual = (
        row.public_id,
        row.scope_digest,
        row.wallet_public_id,
        row.exchange,
        row.mode,
        row.environment,
        row.margin_account,
        row.route_authority_public_id,
        row.kind,
        row.observed_at,
        row.content_digest,
        row.canonical_payload,
    )
    expected = (
        row.claimed_public_id,
        accounting_hold_scope_digest(scope),
        scope.wallet_public_id,
        scope.exchange,
        scope.mode,
        scope.environment,
        scope.margin_account,
        raw.route_authority_public_id,
        raw.kind,
        raw.observed_at,
        accounting_observation_digest(raw),
        canonical_accounting_observation(raw),
    )
    if actual != expected:
        raise AccountingObservationBindingError("accounting_raw_receipt_projection_mismatch")
    return raw
