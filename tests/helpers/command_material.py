"""Build exact historical intent versions in an isolated repository for regression tests."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from uuid import uuid7

from snapper.application.engine.service import compute_shard_key
from snapper.core.types import ExecutionMode
from snapper.core.types import OrderExchange
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import TradeCommand
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeCommandRow

MATERIAL_WALLET = "00000000-0000-7000-8000-000000000211"


async def seed_command_history(
    repository: SQLAlchemyRepository,
    client_order_id: str,
    post_only_versions: tuple[bool, ...],
    *,
    command_type: str = "create",
    exchange: OrderExchange = "kraken",
) -> str:
    """Persist a logical command's exact versions without repairing historical modifier loss.

    Args:
        repository: Private test repository.
        client_order_id: Venue client identity distinct from the generated command UUID.
        post_only_versions: Historical modifier values in physical insertion order.
        command_type: Create, submit or a separately identified cancel intent.
        exchange: Actual execution venue; Paper uses its corresponding isolated mode.

    Returns:
        The actual UUID shared by this logical command's versions.
    """
    command_id = str(uuid7())
    first_at = datetime.now(UTC) - timedelta(hours=1)
    mode: ExecutionMode = "paper" if exchange == "paper" else "live"
    shard_key = compute_shard_key(
        instrument="BTC-USD",
        exchange=exchange,
        mode=mode,
        wallet_public_id=MATERIAL_WALLET,
        strategy_tag=None,
    )
    async with repository.session() as session:
        for index, post_only in enumerate(post_only_versions):
            session.add(
                TradeCommand(
                    public_id=command_id,
                    command_type=command_type,
                    shard_key=shard_key,
                    exchange=exchange,
                    instrument="BTC-USD",
                    mode=mode,
                    strategy_id="material-regression",
                    client_order_id=client_order_id,
                    venue_client_id=client_order_id,
                    side="buy",
                    order_type="limit",
                    quantity=1.0,
                    price=100.0,
                    stop_price=None,
                    leverage=None,
                    reduce_only=False,
                    post_only=post_only,
                    status="created",
                    attempt_count=index,
                    created_at=first_at,
                    correlation_id=command_id,
                    session_id="material-test",
                    sequence_id=index + 1,
                    timestamp=first_at + timedelta(seconds=index),
                    known_to=(
                        KNOWN_TO_MAX
                        if index == len(post_only_versions) - 1
                        else first_at + timedelta(seconds=index + 1)
                    ),
                    wallet_public_id=MATERIAL_WALLET,
                )
            )
        await session.commit()
    return command_id


async def active_command(
    repository: SQLAlchemyRepository, client_order_id: str, exchange: str = "kraken"
) -> TradeCommandRow:
    """Read the current command while preserving the getter's explicit absence contract.

    Args:
        repository: Actual isolated history repository.
        client_order_id: Native client identity of the target CREATE.
        exchange: Actual execution venue.

    Returns:
        The actual current create/submit row.
    """
    command = await repository.get_active_create_command_by_client_order_id(
        client_order_id, exchange
    )
    assert command is not None
    return command
