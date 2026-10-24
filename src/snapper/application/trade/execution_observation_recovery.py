"""Recover every selected child from a durably retained original private frame.

A callback can stop after committing its first child. Its full original frame
still contains the remaining children. Recovery creates stable raw receipts and
holds for all of them; it neither interprets money nor certifies completeness
outside that one retained frame. Original observation holds remain unresolved.
"""

import base64
from dataclasses import dataclass
from typing import Literal
from uuid import NAMESPACE_URL
from uuid import uuid5

from sqlalchemy import select

from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.contract_accounting_observation import accounting_observation_digest
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.contract_accounting_store import RawAccountingWriteResult
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.models import AccountingRawReceipt
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import is_execution_control_message


class AccountingFrameRecoveryUnavailableError(ValueError):
    """A retained envelope cannot prove a supported bounded set of original children."""


@dataclass(frozen=True)
class _FrameShape:
    """Known source container and identity hint, without financial interpretation."""

    field: str
    identity_field: str
    kind: Literal["execution", "unknown"]
    singular: bool = False


_FUTURES_SHAPES = {
    "fills": _FrameShape("fills", "fill_id", "execution"),
    "fills_snapshot": _FrameShape("fills", "fill_id", "execution"),
    "account_log": _FrameShape("new_entry", "booking_uid", "unknown", singular=True),
    "account_log_snapshot": _FrameShape("logs", "booking_uid", "unknown"),
}


def _shape(raw: RawAccountingObservation, document: dict[str, object]) -> _FrameShape:
    """Require matching actual envelope and captured route instead of guessing from child keys."""
    if is_execution_control_message(document):
        raise AccountingFrameRecoveryUnavailableError("accounting_recovery_control_envelope")
    if (
        raw.scope.exchange == "kraken"
        and raw.channel == "kraken.ws.executions"
        and document.get("channel") == "executions"
    ):
        return _FrameShape("data", "exec_id", "execution")
    feed = document.get("feed")
    if (
        raw.scope.exchange == "kraken_futures"
        and isinstance(feed, str)
        and raw.channel == f"kraken_futures.ws.{feed}"
        and feed in _FUTURES_SHAPES
    ):
        return _FUTURES_SHAPES[feed]
    raise AccountingFrameRecoveryUnavailableError("accounting_recovery_envelope_unproven")


def _frame_basis(raw: RawAccountingObservation) -> RawAccountingObservation:
    """Remove child-specific hints while retaining exact frame, clock and actual client scope."""
    return RawAccountingObservation.model_validate(
        {
            **raw.model_dump(),
            "kind": "unknown",
            "target_json_pointer": None,
            "source_identity": None,
            "source_identity_kind": None,
            "order_public_id": None,
            "source_instrument_public_id": None,
            "native_symbol": None,
        }
    )


def _selected_child(
    basis: RawAccountingObservation, shape: _FrameShape, child: object, pointer: str
) -> RawAccountingObservation:
    """Retain malformed children too and label any provider identity only as an unproven hint."""
    identity = child.get(shape.identity_field) if isinstance(child, dict) else None
    hint = identity if isinstance(identity, str) and identity.strip() else None
    return RawAccountingObservation.model_validate(
        {
            **basis.model_dump(),
            "kind": shape.kind,
            "target_json_pointer": pointer,
            "source_identity": hint,
            "source_identity_kind": "unproven" if hint is not None else None,
        }
    )


def recoverable_frame_children(
    observation: RawAccountingObservation, maximum_children: int = 1000
) -> tuple[RawAccountingObservation, ...]:
    """Enumerate every original child, independently of the first committed selector.

    Args:
        observation: Verified original source envelope, including its actual capture clock.
        maximum_children: Bounded full-frame size; a larger frame is refused without truncation.

    Returns:
        Canonical selectors over unchanged wire bytes, including malformed child values.
    """
    raw = RawAccountingObservation.model_validate(observation)
    if type(maximum_children) is not int or not 1 <= maximum_children <= 1000:
        raise ValueError("Accounting recovery child bound must be an integer from 1 to 1000")
    if raw.representation != "wire_bytes_base64":
        raise AccountingFrameRecoveryUnavailableError("accounting_recovery_original_wire_required")
    try:
        document = decode_private_execution_receipt(base64.b64decode(raw.material, validate=True))
    except (ValueError, UnicodeError) as error:
        raise AccountingFrameRecoveryUnavailableError("accounting_recovery_wire_invalid") from error
    shape = _shape(raw, document)
    if shape.field not in document:
        raise AccountingFrameRecoveryUnavailableError("accounting_recovery_container_missing")
    children = [document[shape.field]] if shape.singular else document[shape.field]
    if not isinstance(children, list):
        raise AccountingFrameRecoveryUnavailableError("accounting_recovery_container_invalid")
    if len(children) > maximum_children:
        raise AccountingFrameRecoveryUnavailableError("accounting_recovery_child_bound_exceeded")
    basis = _frame_basis(raw)
    return tuple(
        _selected_child(
            basis, shape, child, f"/{shape.field}" if shape.singular else f"/{shape.field}/{index}"
        )
        for index, child in enumerate(children)
    )


def _child_identity(child: RawAccountingObservation) -> str:
    """Make restarts and sibling-parent recovery converge on one local child observation."""
    frame_digest = accounting_observation_digest(_frame_basis(child))
    return str(
        uuid5(
            NAMESPACE_URL,
            f"snapper:accounting-frame-child:v1:{frame_digest}:{child.target_json_pointer}",
        )
    )


async def recover_accounting_frame(
    repository: AccountingRepository,
    raw_receipt_public_id: str,
    provenance: AccountingWriteProvenance,
) -> tuple[RawAccountingWriteResult, ...]:
    """Durably materialize all source children with interruption-safe independent commits.

    Args:
        repository: Actual immutable evidence store and wallet write-fence provider.
        raw_receipt_public_id: An existing full-frame receipt, including a previously recovered child.
        provenance: Actual recovery knowledge clock and local sequence/session identity.

    Returns:
        Stable child receipt and hold identities, never a financial success or hold resolution.
    """
    stamp = AccountingWriteProvenance.model_validate(provenance)
    async with repository.session() as session:
        raw = await get_bound_accounting_observation(session, raw_receipt_public_id)
        recorded_at = (
            await session.execute(
                select(AccountingRawReceipt.timestamp).where(
                    AccountingRawReceipt.public_id == raw_receipt_public_id
                )
            )
        ).scalar_one()
        if stamp.timestamp < max(recorded_at, raw.observed_at):
            raise AccountingFrameRecoveryUnavailableError("accounting_recovery_knowledge_backdated")
    children = recoverable_frame_children(raw)
    results: list[RawAccountingWriteResult] = []
    for child in children:
        child_stamp = AccountingWriteProvenance.model_validate(
            {**stamp.model_dump(), "public_id": _child_identity(child)}
        )
        results.append(await capture_accounting_observation(repository, child, child_stamp))
    return tuple(results)
