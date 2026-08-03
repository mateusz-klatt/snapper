"""Focused tests for shared market-data schema fields."""

from datetime import UTC
from datetime import datetime
from typing import Any

import pytest
from pydantic import ValidationError

from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import RelatedInstrumentsUnderlying
from snapper.messaging.schemas.data import SignalData

_FIXED_TIME = datetime(2026, 6, 8, 12, 0, 0, tzinfo=UTC)


def _signal_kwargs(**overrides: Any) -> dict[str, Any]:
    """Return base SignalData kwargs (live exchange) with optional overrides."""
    base: dict[str, Any] = {
        "type": "signal",
        "sequence_id": 1,
        "public_id": "sig-1",
        "timestamp": _FIXED_TIME,
        "session_id": "sess-1",
        "instrument": "BTC-USD",
        "exchange": "kraken",
        "side": "buy",
        "strength": 0.5,
        "reason": "test",
        "fired_at": _FIXED_TIME,
    }
    base.update(overrides)
    return base


def test_related_instruments_underlying_description_field_optional() -> None:
    """Related-instruments underlying accepts a null resolved description.

    Given: a related-instruments underlying payload without resolved copy,
    When: the schema is constructed,
    Then: the description field accepts ``None``.
    """
    model = RelatedInstrumentsUnderlying(
        public_id="ua-1",
        ticker="SPX",
        name="S&P 500",
        asset_class="index",
        sector="US Large Cap",
        description=None,
    )
    assert model.description is None


class TestSignalDataPairedGroupValidation:
    """All-or-none consistency checks for the paired-group descriptor."""

    def test_standalone_signal_leaves_all_group_fields_none(self) -> None:
        """A standalone signal carries no paired-group descriptor.

        Given: a SignalData with no paired-group fields supplied,
        When: the schema is constructed,
        Then: all four paired-group fields default to None.
        """
        signal = SignalData(**_signal_kwargs())
        assert signal.paired_group_id is None
        assert signal.paired_group_size is None
        assert signal.paired_group_index is None
        assert signal.paired_group_policy is None
        assert signal.paired_group_key is None

    def test_full_group_descriptor_is_accepted(self) -> None:
        """A complete, in-range group descriptor validates.

        Given: a SignalData with id, size, index, policy and key all set
            and ``0 <= index < size`` with ``size >= 2``,
        When: the schema is constructed,
        Then: every paired-group field round-trips verbatim.
        """
        signal = SignalData(
            **_signal_kwargs(
                paired_group_id="grp-1",
                paired_group_size=2,
                paired_group_index=1,
                paired_group_policy="simultaneous",
                paired_group_key="kraken:BTC-USD:live|kraken:ETH-USD:live",
            )
        )
        assert signal.paired_group_id == "grp-1"
        assert signal.paired_group_size == 2
        assert signal.paired_group_index == 1
        assert signal.paired_group_policy == "simultaneous"
        assert signal.paired_group_key == "kraken:BTC-USD:live|kraken:ETH-USD:live"

    def test_group_member_without_id_is_rejected(self) -> None:
        """Paired-group members require an explicit group id.

        Given: a SignalData with paired_group_size but no paired_group_id,
        When: the schema is constructed,
        Then: a ValidationError is raised.
        """
        member_without_id_kwargs = _signal_kwargs(paired_group_size=2)
        with pytest.raises(ValidationError, match="require paired_group_id"):
            SignalData(**member_without_id_kwargs)

    def test_group_id_without_size_is_rejected(self) -> None:
        """A group id requires size, index and policy alongside it.

        Given: a SignalData with paired_group_id but no size/index/policy,
        When: the schema is constructed,
        Then: a ValidationError is raised.
        """
        id_without_size_kwargs = _signal_kwargs(paired_group_id="grp-1")
        with pytest.raises(ValidationError, match="requires paired_group_size"):
            SignalData(**id_without_size_kwargs)

    def test_group_size_below_two_is_rejected(self) -> None:
        """A paired group needs at least two legs.

        Given: a complete descriptor with paired_group_size == 1,
        When: the schema is constructed,
        Then: a ValidationError is raised.
        """
        single_leg_group_kwargs = _signal_kwargs(
            paired_group_id="grp-1",
            paired_group_size=1,
            paired_group_index=0,
            paired_group_policy="simultaneous",
            paired_group_key="kraken:BTC-USD:live",
        )
        with pytest.raises(ValidationError, match="must be >= 2"):
            SignalData(**single_leg_group_kwargs)

    def test_group_index_out_of_range_is_rejected(self) -> None:
        """The leg index must satisfy ``0 <= index < size``.

        Given: a complete descriptor with index equal to size,
        When: the schema is constructed,
        Then: a ValidationError is raised.
        """
        index_out_of_range_kwargs = _signal_kwargs(
            paired_group_id="grp-1",
            paired_group_size=2,
            paired_group_index=2,
            paired_group_policy="sequential_handoff",
            paired_group_key="kraken:BTC-USD:live",
        )
        with pytest.raises(ValidationError, match="0 <= index"):
            SignalData(**index_out_of_range_kwargs)

    def test_blank_group_id_is_rejected(self) -> None:
        """A whitespace-only group id is a fail-open grouping key and is rejected.

        Given: a descriptor whose paired_group_id is whitespace-only while
            size, index and policy are otherwise valid,
        When: the schema is constructed,
        Then: a ValidationError is raised so an empty grouping key never
            silently splits a multi-leg group.
        """
        blank_group_id_kwargs = _signal_kwargs(
            paired_group_id="   ",
            paired_group_size=2,
            paired_group_index=0,
            paired_group_policy="simultaneous",
            paired_group_key="kraken:BTC-USD:live",
        )
        with pytest.raises(ValidationError, match="non-empty group identifier"):
            SignalData(**blank_group_id_kwargs)

    def test_group_without_key_is_rejected(self) -> None:
        """A group id requires a paired_group_key alongside it.

        Given: a SignalData with id, size, index and policy set but no
            paired_group_key,
        When: the schema is constructed,
        Then: a ValidationError is raised so a group can never be created
            without the canonical leg-set key the arming barrier validates.
        """
        group_without_key_kwargs = _signal_kwargs(
            paired_group_id="grp-1",
            paired_group_size=2,
            paired_group_index=0,
            paired_group_policy="simultaneous",
        )
        with pytest.raises(ValidationError, match="paired_group_key alongside"):
            SignalData(**group_without_key_kwargs)

    def test_blank_group_key_is_rejected(self) -> None:
        """A whitespace-only group key is rejected.

        Given: a descriptor whose paired_group_key is whitespace-only while
            id, size, index and policy are otherwise valid,
        When: the schema is constructed,
        Then: a ValidationError is raised so an empty leg-set key never
            silently fails every arming validation.
        """
        blank_group_key_kwargs = _signal_kwargs(
            paired_group_id="grp-1",
            paired_group_size=2,
            paired_group_index=0,
            paired_group_policy="simultaneous",
            paired_group_key="   ",
        )
        with pytest.raises(ValidationError, match="non-empty group key"):
            SignalData(**blank_group_key_kwargs)

    def test_group_key_without_id_is_rejected(self) -> None:
        """A paired_group_key requires an explicit group id.

        Given: a SignalData with paired_group_key but no paired_group_id,
        When: the schema is constructed,
        Then: a ValidationError is raised.
        """
        key_without_id_kwargs = _signal_kwargs(paired_group_key="kraken:BTC-USD:live")
        with pytest.raises(ValidationError, match="require paired_group_id"):
            SignalData(**key_without_id_kwargs)


def test_downstream_schemas_carry_paired_group_descriptor() -> None:
    """Order/execution schemas transport the paired-group descriptor verbatim.

    Given: order-request, order, execution and order-event payloads each
        constructed with a full paired-group descriptor,
    When: the schemas are built,
    Then: each carries the four paired-group fields unchanged (transport
        only, no re-validation downstream of the signal origin).
        ``paired_group_key`` is intentionally SignalData-only: it is
        origination metadata the coordinator consumes to build/validate the
        group, so it is not threaded onto the downstream schemas.
    """
    group = {
        "paired_group_id": "grp-1",
        "paired_group_size": 2,
        "paired_group_index": 0,
        "paired_group_policy": "simultaneous",
    }
    request = OrderRequestData(
        type="order_request",
        sequence_id=1,
        public_id="req-1",
        timestamp=_FIXED_TIME,
        session_id="sess-1",
        strategy_id="strat",
        exchange="kraken",
        instrument="BTC-USD",
        mode="live",
        side="buy",
        order_type="market",
        quantity=1.0,
        client_order_id="cid-1",
        **group,
    )
    order = OrderData(
        type="order",
        sequence_id=1,
        public_id="ord-1",
        timestamp=_FIXED_TIME,
        session_id="sess-1",
        client_order_id="cid-1",
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        status="submitted",
        order_type="market",
        size=1.0,
        filled_size=0.0,
        created_at=_FIXED_TIME,
        **group,
    )
    execution = ExecutionData(
        type="execution",
        sequence_id=1,
        public_id="exe-1",
        timestamp=_FIXED_TIME,
        session_id="sess-1",
        client_order_id="cid-1",
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        size=1.0,
        price=50000.0,
        last_size=1.0,
        last_price=50000.0,
        fee=0.1,
        fee_asset="USD",
        status="filled",
        executed_at=_FIXED_TIME,
        **group,
    )
    event = OrderEventData(
        type="order_event",
        sequence_id=1,
        public_id="evt-1",
        timestamp=_FIXED_TIME,
        session_id="sess-1",
        exchange_order_id="ex-1",
        client_order_id="cid-1",
        exchange="kraken",
        instrument="BTC-USD",
        event="accepted",
        **group,
    )
    for payload in (request, order, execution, event):
        assert payload.paired_group_id == "grp-1"
        assert payload.paired_group_size == 2
        assert payload.paired_group_index == 0
        assert payload.paired_group_policy == "simultaneous"
