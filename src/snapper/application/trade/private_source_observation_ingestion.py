"""Compose original private financial raw, coarse hold and optional actual receive ownership atomically.

A frame binding freezes its original selected client and one compatible nominal
legacy route decision before any child storage await. Ownership is optional;
unknown or refused proof leaves unchanged raw-and-hold evidence. Neither this
adapter nor a receive ticket interprets an execution or releases a hold.
"""

import asyncio
import base64
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from uuid import uuid7

from loguru import logger
from sqlalchemy.exc import OperationalError

from snapper.application.trade.execution_observation_ingestion import build_accounting_observation
from snapper.application.trade.unit_snapshot_route import CapturedExecutionRoute
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.private_frame_ticket import require_authenticated_private_frame
from snapper.data.contract_accounting_capture_session import capture_bound_accounting_observation
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.private_source_ownership_store import PrivateSourceOwnershipBindingError
from snapper.data.private_source_ownership_store import capture_private_source_ownership
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.messaging.infrastructure.publisher import SequenceTracker


@dataclass(frozen=True, slots=True)
class _FrameBinding:
    """Frozen original source choice and only an actually validated original receive ticket."""

    selection: ExecutionClientSelection
    authority: CapturedExecutionRoute | None
    ticket: object | None


@dataclass(frozen=True, slots=True)
class _CaptureEvidence:
    """Original immutable child evidence and stable identities reused across operational retries."""

    observation: RawAccountingObservation
    provenance: AccountingWriteProvenance
    ownership_identity: ExecutionEvidenceIdentity
    ticket: object | None


def _freeze_original_authority(
    authority: CapturedExecutionRoute | None,
) -> CapturedExecutionRoute | None:
    """Copy one public historical provider view without letting later callers mutate the frozen frame."""
    if authority is None:
        return None
    return CapturedExecutionRoute(
        deepcopy(authority.result),
        ExecutionRouteAuthorityPayload.model_validate(authority.payload.model_dump(mode="python")),
        ExecutionRouteReference.model_validate(authority.reference.model_dump(mode="python")),
    )


def _matching_ticket(ticket: object | None, source: PrivateExecutionObservation) -> object | None:
    """Accept only actual issuance against these exact original source bytes, without losing unowned raw."""
    if ticket is None or source.representation != "wire_bytes_base64":
        return None
    try:
        require_authenticated_private_frame(
            ticket, base64.b64decode(source.material, validate=True)
        )
    except ValueError:
        return None
    return ticket


async def _capture_atomically(repository: AccountingRepository, data: _CaptureEvidence) -> str:
    """Keep raw/hold and any ownership inside one real wallet-fenced caller transaction."""
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, data.observation.scope.wallet_public_id
        )
        raw = await capture_bound_accounting_observation(session, data.observation, data.provenance)
        if not raw.conflicted and data.ticket is not None:
            try:
                await capture_private_source_ownership(
                    session, raw.public_id, data.ticket, data.ownership_identity
                )
            except PrivateSourceOwnershipBindingError:
                logger.warning(
                    "Private observation {} retained with its financial hold and unproven ownership",
                    raw.public_id,
                )
        await session.commit()
        return raw.public_id


class FrozenPrivateFrameObservationSink:
    """Capture every child using one original frame selection, ticket and nominal route decision."""

    def __init__(
        self,
        repository: AccountingRepository,
        tracker: SequenceTracker,
        binding: _FrameBinding,
    ) -> None:
        """Keep no dynamic client/authority providers that later child awaits could relabel."""
        self._repository = repository
        self._tracker = tracker
        self._binding = binding

    async def __call__(self, source: PrivateExecutionObservation) -> str:
        """Commit one original child plus optional actually issued ownership before parser continuation.

        Args:
            source: Original financial source captured before parsing or queueing.

        Returns:
            Actual committed raw UUID, including a separately retained conflict UUID when required.
        """
        selected = self._binding.selection
        sequence = f"accounting.raw.{selected.exchange}.{selected.wallet_public_id}"
        data = _CaptureEvidence(
            observation=build_accounting_observation(source, selected, self._binding.authority),
            provenance=AccountingWriteProvenance(
                public_id=str(uuid7()),
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(sequence),
                timestamp=datetime.now(UTC),
            ),
            ownership_identity=ExecutionEvidenceIdentity(
                public_id=str(uuid7()),
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(f"{sequence}.ownership"),
            ),
            ticket=_matching_ticket(self._binding.ticket, source),
        )
        while True:
            try:
                return await _capture_atomically(self._repository, data)
            except OperationalError:
                logger.warning(
                    "Private observation storage unavailable; retaining callback {} for retry",
                    data.provenance.public_id,
                )
                await asyncio.sleep(1)


class AuthenticatedPrivateObservationSink:
    """Bind one original frame before its first child await while retaining unchanged fallback holds."""

    def __init__(
        self,
        repository: AccountingRepository,
        selection: Callable[[], ExecutionClientSelection | None],
        authority: Callable[[], CapturedExecutionRoute | None],
        tracker: SequenceTracker,
    ) -> None:
        """Retain ordinary runtime providers for one synchronous frame binding, without venue calls."""
        self._repository = repository
        self._selection = selection
        self._authority = authority
        self._tracker = tracker

    def bind_frame(
        self, ticket: object | None, original_financial_wire: bytes | None
    ) -> FrozenPrivateFrameObservationSink:
        """Freeze the actual original selected identity before an N-child callback can await storage.

        Args:
            ticket: Actual opaque receive capability when the owned SDK bridge issued one.
            original_financial_wire: Exact original financial bytes, never reconstructed JSON/control.

        Returns:
            One child sink using the original selection/route decision for its entire frame.

        Raises:
            RuntimeError: If neither an actual ticket nor the current callback proves any client scope.
        """
        selected: ExecutionClientSelection | None = None
        actual_ticket: object | None = None
        if ticket is not None and original_financial_wire is not None:
            try:
                frame = require_authenticated_private_frame(ticket, original_financial_wire)
                selected = frame.client_selection
                actual_ticket = ticket
            except ValueError:
                pass
        if selected is None:
            selected = self._selection()
        if selected is None:
            raise RuntimeError("private_observation_client_identity_unproven")
        return FrozenPrivateFrameObservationSink(
            self._repository,
            self._tracker,
            _FrameBinding(
                ExecutionClientSelection.model_validate(selected.model_dump(mode="python")),
                _freeze_original_authority(self._authority()),
                actual_ticket,
            ),
        )
