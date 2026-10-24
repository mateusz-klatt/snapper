"""Persist actual private source observations before clients parse or suppress them."""

import asyncio
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from uuid import uuid7

from loguru import logger
from sqlalchemy.exc import OperationalError

from snapper.application.trade.unit_snapshot_route import CapturedExecutionRoute
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.messaging.infrastructure.publisher import SequenceTracker


def _matching_authority(
    selection: ExecutionClientSelection, authority: CapturedExecutionRoute | None
) -> str | None:
    """Attach only a historical authority from this exact selected client revision."""
    if authority is None:
        return None
    if client_selection_from_authority(authority.payload) != selection:
        return None
    return authority.result.public_id


def build_accounting_observation(
    source: PrivateExecutionObservation,
    selection: ExecutionClientSelection,
    authority: CapturedExecutionRoute | None,
) -> RawAccountingObservation:
    """Retain source bytes with actual client identity before financial interpretation.

    Args:
        source: Actual callback source, including an exact frame selector when available.
        selection: Public provenance from the same selected SDK and credential revision.
        authority: Optional historical response from that client, never a prerequisite.

    Returns:
        An uninterpreted receipt with a coarse account hold until its margin is proven.
    """
    context = source.context
    return RawAccountingObservation(
        scope=AccountingObservationScope(
            wallet_public_id=selection.wallet_public_id,
            exchange=selection.exchange,
            mode=selection.mode,
            environment=selection.environment,
        ),
        route_revision=client_selection_digest(selection),
        route_authority_public_id=_matching_authority(selection, authority),
        credential_public_id=selection.credential_public_id,
        credential_timestamp=selection.credential_timestamp,
        kind=context.kind,
        channel=context.channel,
        representation=source.representation,
        material=source.material,
        target_json_pointer=context.target_json_pointer,
        observed_at=context.observed_at,
        source_identity=context.source_identity,
        source_identity_kind="unproven" if context.source_identity is not None else None,
    )


class DurableExecutionObservationSink:
    """Await an atomic raw receipt and hold commit before acknowledging its callback.

    Operational storage failures retain the exact local UUID and source in the
    blocked callback. They never return success or let parsing continue. Process
    death before a commit still requires venue history recovery; this writer does
    not claim a durable transport spool for bytes not yet committed.
    """

    def __init__(
        self,
        repository: AccountingRepository,
        selection: Callable[[], ExecutionClientSelection | None],
        authority: Callable[[], CapturedExecutionRoute | None],
        tracker: SequenceTracker,
    ) -> None:
        """Retain actual runtime providers without obtaining credentials or querying venues."""
        self._repository = repository
        self._selection = selection
        self._authority = authority
        self._tracker = tracker

    async def __call__(self, source: PrivateExecutionObservation) -> str:
        """Commit one callback observation, reusing its exact identity on storage retry.

        Args:
            source: Source material captured before parsing or queueing.

        Returns:
            Committed raw receipt UUID, including a retained conflict receipt if necessary.

        Raises:
            RuntimeError: If the actual client's route and credential identity are unproven.
        """
        selection = self._selection()
        if selection is None:
            raise RuntimeError("private_observation_client_identity_unproven")
        observation = build_accounting_observation(source, selection, self._authority())
        provenance = AccountingWriteProvenance(
            public_id=str(uuid7()),
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(
                f"accounting.raw.{selection.exchange}.{selection.wallet_public_id}"
            ),
            timestamp=datetime.now(UTC),
        )
        while True:
            try:
                result = await capture_accounting_observation(
                    self._repository, observation, provenance
                )
                return result.public_id
            except OperationalError:
                logger.warning(
                    "Private observation storage unavailable; retaining callback {} for retry",
                    provenance.public_id,
                )
                await asyncio.sleep(1)
