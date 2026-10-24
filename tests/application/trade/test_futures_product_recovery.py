"""Actual product source cancellation and uncertain capture acknowledgement preserve durable ordering."""

import asyncio
from pathlib import Path

import pytest
from kraken.futures import User
from sqlalchemy import func
from sqlalchemy import select

import snapper.application.trade.futures_product_read as producer
from snapper.core.futures_product_read import FuturesProductReadFailure
from snapper.core.futures_product_read import FuturesProductReadSource
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.futures_product_capture import FuturesProductWriteResult
from snapper.data.futures_product_read_store import BoundFuturesProductOutcome
from snapper.data.futures_product_read_store import get_latest_futures_product_read
from snapper.data.models import FuturesProductReadAttempt
from snapper.data.models import FuturesProductReadOutcome
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    GuardedFuturesProductReceipt,
)
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from tests.helpers.futures_product_read import futures_product_scenario


async def test_cancellation_after_actual_http_before_capture_records_only_fixed_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retain conservative cancellation when actual HTTP finishes before capture begins.

    Given: Genuine signed product HTTP returns while its read operation still awaits completion.
    When: The caller cancels before the original captured-outcome transaction starts.
    Then: The original request keeps one fixed cancellation result and no successful source body.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        arrived = asyncio.Event()
        actual_read = producer._read_product_result

        async def pause_return(
            client: KrakenFuturesExchangeClient,
            user: User,
            token: PreparedKeyToken,
            operation: producer._ProductOperation,
        ) -> GuardedFuturesProductReceipt:
            """Finish actual signed HTTP before pausing the operation's return boundary."""
            received = await actual_read(client, user, token, operation)
            arrived.set()
            await asyncio.Event().wait()
            return received

        monkeypatch.setattr(producer, "_read_product_result", pause_return)
        task = asyncio.create_task(
            producer.read_native_futures_products(native.repository, native.client, native.tracker)
        )
        try:
            await asyncio.wait_for(arrived.wait(), timeout=5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            async with native.repository.session() as session:
                latest = await get_latest_futures_product_read(
                    session, native.selection, native.clock.current
                )
                assert latest is not None and latest.outcome is not None
                result = latest.outcome.payload.result
                assert isinstance(result, FuturesProductReadFailure)
                assert result.reason == "cancelled"
                assert "body" not in result.model_dump()
                assert (
                    await session.scalar(
                        select(func.count()).select_from(FuturesProductReadOutcome)
                    )
                    == 1
                )
            assert len(scenario.adapter.product_calls) == 1
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("committed", [False, True])
async def test_cancellation_during_capture_preserves_original_pending_or_committed_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, committed: bool
) -> None:
    """Preserve actual database truth when cancellation interrupts the capture acknowledgement boundary.

    Given: A real product response reaches capture before or after its original transaction commits.
    When: The caller cancels while waiting at that capture boundary.
    Then: The request remains honestly pending or keeps its one original success without replacement.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        arrived = asyncio.Event()
        original_record = producer._record_product_result
        recorded: list[BoundFuturesProductOutcome] = []

        async def pause_capture(
            repository: AccountingRepository,
            operation: producer._ProductOperation,
            result: FuturesProductWriteResult,
        ) -> BoundFuturesProductOutcome:
            """Pause on the selected side of the unchanged actual capture transaction."""
            if committed:
                recorded.append(await original_record(repository, operation, result))
            arrived.set()
            await asyncio.Event().wait()
            return await original_record(repository, operation, result)

        monkeypatch.setattr(producer, "_record_product_result", pause_capture)
        task = asyncio.create_task(
            producer.read_native_futures_products(native.repository, native.client, native.tracker)
        )
        try:
            await asyncio.wait_for(arrived.wait(), timeout=5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            async with native.repository.session() as session:
                latest = await get_latest_futures_product_read(
                    session, native.selection, native.clock.current
                )
                assert latest is not None
                assert (
                    await session.scalar(
                        select(func.count()).select_from(FuturesProductReadAttempt)
                    )
                    == 1
                )
                count = await session.scalar(
                    select(func.count()).select_from(FuturesProductReadOutcome)
                )
                assert count == int(committed)
                if committed:
                    assert latest.outcome == recorded[0]
                    assert isinstance(recorded[0].payload.result, FuturesProductReadSource)
                else:
                    assert latest.outcome is None
            assert len(scenario.adapter.product_calls) == 1
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_lost_capture_acknowledgement_does_not_replace_or_repeat_original_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the actual committed outcome after its acknowledgement fails outside the database.

    Given: Genuine product HTTP and its original captured-outcome transaction have completed.
    When: The caller receives an exception after the real commit instead of its acknowledgement.
    Then: An independent session rebinds the unchanged original result and no second HTTP call occurs.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        original_record = producer._record_product_result
        recorded: list[BoundFuturesProductOutcome] = []

        async def lose_acknowledgement(
            repository: AccountingRepository,
            operation: producer._ProductOperation,
            result: FuturesProductWriteResult,
        ) -> BoundFuturesProductOutcome:
            """Commit through the actual store before injecting a lost return acknowledgement."""
            recorded.append(await original_record(repository, operation, result))
            raise RuntimeError("synthetic_product_commit_acknowledgement_lost")

        monkeypatch.setattr(producer, "_record_product_result", lose_acknowledgement)
        with pytest.raises(RuntimeError, match="acknowledgement_lost"):
            await producer.read_native_futures_products(
                native.repository, native.client, native.tracker
            )
        async with native.repository.session() as session:
            latest = await get_latest_futures_product_read(
                session, native.selection, native.clock.current
            )
            assert latest is not None and latest.outcome == recorded[0]
            assert isinstance(recorded[0].payload.result, FuturesProductReadSource)
            assert (
                await session.scalar(select(func.count()).select_from(FuturesProductReadOutcome))
                == 1
            )
        assert len(scenario.adapter.product_calls) == 1
