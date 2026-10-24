"""Prepare actual native owner evidence before a Futures executor starts private receive or recovery.

The existing generic source sink remains installed. Futures additionally binds
the original frame before child persistence and transfers only its actually
issued proof-bound runtime key token into the owned client lifecycle. This
installation creates no financial source interpretation or order permission.
"""

from collections.abc import Callable

from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.application.trade.private_source_observation_ingestion import (
    AuthenticatedPrivateObservationSink,
)
from snapper.application.trade.unit_snapshot_route import CapturedExecutionRoute
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.messaging.infrastructure.publisher import SequenceTracker


async def prepare_native_execution_observation_capture(
    repository: Repository | None,
    client: ExchangeClientBase,
    authority: Callable[[], CapturedExecutionRoute | None],
    tracker: SequenceTracker,
) -> None:
    """Bind actual source ownership inside the connected client context before any private subscription.

    Args:
        repository: Actual executor store; native ownership requires the real SQL transaction provider.
        client: Original identity-bound exchange client whose private lifecycle has not started.
        authority: Current nominal legacy route provider, independently frozen once per original frame.
        tracker: Actual producer session and monotonic evidence sequence allocator.

    Raises:
        ValueError: If a Futures executor lacks actual persistent source ownership prerequisites.
        BaseException: Preparation, installation and cancellation failures propagate to client cleanup.
    """
    if not isinstance(client, KrakenFuturesExchangeClient):
        return
    if not isinstance(repository, SQLAlchemyRepository):
        raise ValueError("native_private_execution_repository_required")
    sink = AuthenticatedPrivateObservationSink(
        repository, client.execution_client_selection, authority, tracker
    )
    prepared = await prepare_native_private_owner(repository, client, tracker)
    try:
        token = prepared.require_token()
        client.set_private_frame_observation_sink(sink.bind_frame)
        client.bind_prepared_private_owner(token)
    except BaseException:
        prepared.close()
        raise
