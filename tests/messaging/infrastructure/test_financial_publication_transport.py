"""Use genuine immutable intents and real local ZMQ frames through the registered parser."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import UUID
from uuid import uuid4

import pytest
import zmq
import zmq.asyncio

from snapper.application.trade.financial_publication_transport import FinancialPublicationTransport
from snapper.application.trade.futures_fill_recording import FuturesPositionRecordingRequest
from snapper.application.trade.futures_fill_recording import record_futures_position
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.financial_publication import ContractPositionPublication
from snapper.core.financial_publication import canonical_financial_publication
from snapper.core.native_financial_publication_v2 import NativeContractPositionPublicationV2
from snapper.core.versioned_financial_publication_payload import (
    versioned_financial_publication_topic,
)
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.financial_publication import FinancialPublicationData
from snapper.messaging.schemas.messages import MESSAGE_TYPE_MAP
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message
from snapper.messaging.topics.validation import TopicValidationError
from tests.application.trade.test_unit_snapshot_fill import _KNOWN
from tests.helpers.contract_account_projection import (
    projection_scenario as shared_projection_scenario,
)
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.native_position_recording_v3 import native_position_recording_scenario_v3
from tests.helpers.native_position_recording_v3 import record_native_position_scenario_v3
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_stamp

projection_scenario = shared_projection_scenario
_STRUCTURAL_CLOCK = datetime(2026, 10, 23, 10, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class _LocalFinancialBus:
    """Own actual local transport wrappers without a mock publisher or parser."""

    publisher: MessagePublisher
    validated_publisher: ValidatedPublisher
    subscriber: ValidatedSubscriber


@asynccontextmanager
async def _local_financial_bus() -> AsyncIterator[_LocalFinancialBus]:
    """Wait for the real XPUB subscription acknowledgement before publishing any frame.

    Yields:
        Actual validated publisher and subscriber wrappers on one isolated inproc transport.
    """
    context = zmq.asyncio.Context()
    raw_publisher = context.socket(zmq.XPUB)
    raw_subscriber = context.socket(zmq.SUB)
    endpoint = f"inproc://financial-publication-{uuid4()}"
    validated_publisher = ValidatedPublisher(raw_publisher)
    subscriber = ValidatedSubscriber(raw_subscriber)
    try:
        raw_publisher.setsockopt(zmq.XPUB_VERBOSE, 1)
        raw_publisher.bind(endpoint)
        raw_subscriber.connect(endpoint)
        subscriber.subscribe("accounting.")
        assert await raw_publisher.recv() == b"\x01accounting."
        publisher = MessagePublisher(validated_publisher, SequenceTracker())
        yield _LocalFinancialBus(publisher, validated_publisher, subscriber)
    finally:
        subscriber.close()
        validated_publisher.close()
        context.destroy(linger=0)


def _structural_payload() -> ContractPositionPublication:
    """Build only schema material, without asserting that its reference rows exist.

    Returns:
        An explicit structural legacy payload for malformed transport boundary cases.
    """
    return ContractPositionPublication(
        scope=ExecutionAccountScope(
            wallet_public_id=admission_id(1),
            exchange="kraken_futures",
            mode="live",
            environment="production",
            margin_account="structural-example-only",
        ),
        source_execution_id="structural-fill-only",
        execution_public_id=admission_id(2),
        venue_event_public_id=admission_id(3),
        unit_snapshot_public_id=admission_id(4),
        position_event_public_id=admission_id(5),
        instrument_public_id=admission_id(6),
        raw_receipt_public_id=admission_id(7),
        route_authority_public_id=admission_id(8),
        ledger_sequence=1,
        executed_at=_STRUCTURAL_CLOCK,
        execution_known_at=_STRUCTURAL_CLOCK,
        recorded_at=_STRUCTURAL_CLOCK,
    )


def _structural_envelope(tracker: SequenceTracker) -> FinancialPublicationData:
    """Wrap structural material without fabricating retained publication ownership.

    Args:
        tracker: Actual test publisher session and sequence allocator.

    Returns:
        A complete typed envelope whose payload is explicitly a structural example.
    """
    return FinancialPublicationData(
        public_id=admission_id(9),
        wallet_public_id=admission_id(1),
        session_id=tracker.session_id,
        sequence_id=1,
        timestamp=_STRUCTURAL_CLOCK + timedelta(seconds=1),
        payload=_structural_payload(),
    )


async def _receive_financial(bus: _LocalFinancialBus) -> FinancialPublicationData:
    """Supply the separately received actual topic to the real registered parser.

    Args:
        bus: Actual ready local transport whose subscriber owns the original two wire frames.

    Returns:
        The original registered financial message after exact actual-topic binding.
    """
    topic, wire = await bus.subscriber.recv_multipart()
    parsed = parse_message(wire.decode("utf-8"), topic=topic)
    assert isinstance(parsed, FinancialPublicationData)
    assert parsed.topic == topic == versioned_financial_publication_topic(parsed.payload)
    return parsed


@pytest.mark.asyncio
async def test_actual_uuid5_legacy_intent_redelivers_over_local_zmq_without_identity_replacement(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Carry the actual atomic legacy writer result through the complete local transport path.

    Given: Genuine source receipts and accounting targets create one original UUID5 publication intent.
    When: The actual transport publishes that same intent twice through validated XPUB/SUB and parsing.
    Then: Original canonical payload and publication identity remain fixed while bus sequence advances.
    """
    scenario = projection_scenario
    recorded = await record_futures_position(
        scenario.fill.admission.repository,
        FuturesPositionRecordingRequest(
            **scenario.fill.source_request.model_dump(mode="python"),
            position_receipt_public_id=scenario.position_receipt_public_id,
        ),
        admission_stamp(600, _KNOWN + timedelta(seconds=2)),
    )
    intent = recorded.publication
    assert UUID(intent.public_id).version == 5
    canonical = canonical_financial_publication(intent.payload)
    async with _local_financial_bus() as bus:
        transport = FinancialPublicationTransport(bus.publisher)
        await transport(intent.topic, intent.public_id, intent.payload)
        first = await _receive_financial(bus)
        await transport(intent.topic, intent.public_id, intent.payload)
        second = await _receive_financial(bus)
        assert first.public_id == second.public_id == intent.public_id
        assert (
            first.wallet_public_id
            == second.wallet_public_id
            == intent.payload.scope.wallet_public_id
        )
        assert first.session_id == second.session_id == bus.publisher.session_id
        assert (first.sequence_id, second.sequence_id) == (1, 2)
        assert first.payload == second.payload == intent.payload
        assert canonical_financial_publication(intent.payload) == canonical


@pytest.mark.asyncio
async def test_actual_native_v3_intent_selects_its_registered_v2_payload_on_local_zmq(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Use actual signed HTTP, authenticated source frames and the native atomic writer.

    Given: The genuine native V3 fixture commits original sources before creating its financial targets.
    When: Its real retained publication is sent through the actual validated local bus and parser.
    Then: The native payload family and every original authority and ownership reference survive intact.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        intent = (await record_native_position_scenario_v3(scenario)).publication
        async with _local_financial_bus() as bus:
            await FinancialPublicationTransport(bus.publisher)(
                intent.topic, intent.public_id, intent.payload
            )
            received = await _receive_financial(bus)
            assert isinstance(received.payload, NativeContractPositionPublicationV2)
            assert received.public_id == intent.public_id
            assert received.payload == intent.payload
            assert received.topic == intent.topic
            assert received.wallet_public_id == scenario.source.selection.wallet_public_id


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["wallet", "instrument", "exchange", "environment", "family"])
async def test_actual_frame_topic_is_not_replaced_by_the_json_topic_copy(mismatch: str) -> None:
    """Reject a real transport frame that contradicts an otherwise valid stamped envelope.

    Given: An explicitly structural envelope names one exact legacy payload destination.
    When: Actual validated ZMQ transport carries its bytes under another valid destination.
    Then: The registered parser rejects the actual frame without substituting the envelope's topic.
    """
    async with _local_financial_bus() as bus:
        message = _structural_envelope(bus.publisher.tracker)
        original = versioned_financial_publication_topic(message.payload)
        replacements = {
            "wallet": original.replace(admission_id(1), admission_id(21)),
            "instrument": original.replace(admission_id(6), admission_id(26)),
            "exchange": original.replace("kraken_futures", "original.provider"),
            "environment": original.replace(".production.", ".demo."),
            "family": original.replace(".contract_position.", ".native_position.v2."),
        }
        actual = replacements[mismatch]
        await bus.validated_publisher.send_multipart(actual, message.publish_to(original))
        topic, wire = await bus.subscriber.recv_multipart()
        assert topic == actual and actual != original
        with pytest.raises(MessageParseError, match="actual transport topic"):
            parse_message(wire.decode(), topic=topic)


@pytest.mark.parametrize("missing", ["actual_topic", "stamped_topic"])
def test_new_financial_parser_requires_both_actual_and_stamped_topics(missing: str) -> None:
    """Refuse shape-only JSON that has no independently matched transport destination.

    Given: An explicit structural financial envelope and its original derived destination.
    When: Parsing omits either the actual frame topic or the stamped JSON topic.
    Then: The registered new message cannot be accepted through the legacy topic-free interface.
    """
    message = _structural_envelope(SequenceTracker())
    topic = versioned_financial_publication_topic(message.payload)
    assert MESSAGE_TYPE_MAP["financial_publication"] is FinancialPublicationData
    with pytest.raises(MessageParseError, match="topic"):
        if missing == "actual_topic":
            parse_message(message.publish_to(topic).decode())
        else:
            parse_message(message.model_dump_json(), topic=topic)


@pytest.mark.asyncio
async def test_actual_publisher_and_subscriber_refuse_malformed_accounting_routes() -> None:
    """Exercise the original wrappers rather than substituting mocked topic validators.

    Given: A ready real local accounting transport and a structural financial envelope.
    When: Publishing and subscribing request an unsupported accounting version before a valid send.
    Then: Both wrappers refuse and the subsequent original valid message is the actual received frame.
    """
    async with _local_financial_bus() as bus:
        message = _structural_envelope(bus.publisher.tracker)
        topic = versioned_financial_publication_topic(message.payload)
        with pytest.raises(TopicValidationError):
            await bus.validated_publisher.send_multipart(
                "accounting.native_position.v3.invalid", b"invalid"
            )
        with pytest.raises(TopicValidationError):
            bus.subscriber.subscribe("accounting.native_position.v3.")
        await bus.publisher.send(topic, message)
        assert (await _receive_financial(bus)).public_id == message.public_id


def test_new_financial_registration_preserves_old_topic_free_message_parsing() -> None:
    """Keep existing message callers compatible while adding the stricter financial boundary.

    Given: A complete original tick envelope using the existing registered class.
    When: An old caller parses its JSON without a separately supplied transport topic.
    Then: The original concrete tick type and its fields remain unchanged.
    """
    tracker = SequenceTracker()
    tick = TickData(
        public_id=admission_id(51),
        session_id=tracker.session_id,
        sequence_id=1,
        timestamp=_STRUCTURAL_CLOCK,
        exchange="kraken",
        instrument="BTC-USD",
        volume=1.0,
    )
    assert parse_message(tick.to_json()) == tick
    assert parse_message(tick.to_json(), topic="market.kraken.BTC-USD.ticks") == tick
