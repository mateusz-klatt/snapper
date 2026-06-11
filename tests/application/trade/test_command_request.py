"""Tests for shared TradeCommand -> dispatch payload reconstruction."""

from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

from snapper.application.trade.command_request import order_request_from_command
from snapper.application.trade.command_request import parse_shard_key
from snapper.data.repository_types import TradeCommandRow


def _cmd(**overrides: Any) -> TradeCommandRow:
    """Build a full TradeCommandRow for reconstruction tests."""
    now = datetime.now(UTC)
    row: dict[str, Any] = {
        "public_id": "cmd-1",
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 9,
        "command_type": "create",
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "strat-1",
        "client_order_id": "cid-1",
        "venue_client_id": "cid-1",
        "idempotency_key": None,
        "side": "buy",
        "order_type": "limit",
        "quantity": 1.5,
        "price": 101.0,
        "leverage": 3,
        "reduce_only": True,
        "status": "dispatched",
        "attempt_count": 1,
        "last_error": None,
        "created_at": now,
        "dispatched_at": now,
        "acked_at": None,
        "terminal_at": None,
        "exchange_order_id": None,
        "supersedes_command_id": None,
        "correlation_id": "corr-1",
        "wallet_public_id": "wallet-1",
        "operator_public_id": "op-1",
        "user_public_id": None,
        "source_surface": "strategy",
        "plan_public_id": None,
    }
    row.update(overrides)
    return cast(TradeCommandRow, row)


class TestParseShardKey:
    """Shard-key parsing across legacy and wallet-aware formats."""

    def test_three_segment_legacy_key(self) -> None:
        """A 3-segment key parses with empty wallet and no tag.

        Given: a legacy exchange.instrument.mode key,
        When: parsed,
        Then: wallet_short is empty and strategy_tag is None.
        """
        assert parse_shard_key("kraken.BTC-USD.live") == ("kraken", "BTC-USD", "live", "", None)

    def test_wallet_segment_is_detected(self) -> None:
        """A w-prefixed 13-char hex segment is the wallet short.

        Given: a wallet-aware 4-segment key,
        When: parsed,
        Then: the wallet short is extracted without a tag.
        """
        assert parse_shard_key("kraken.BTC-USD.live.wabcdef012345") == (
            "kraken",
            "BTC-USD",
            "live",
            "abcdef012345",
            None,
        )

    def test_wallet_and_tag_segments(self) -> None:
        """Wallet segment plus trailing strategy tag both parse.

        Given: a 5-segment wallet-aware key with a paper-mode tag,
        When: parsed,
        Then: both wallet short and tag come back.
        """
        assert parse_shard_key("kraken.BTC-USD.paper.wabcdef012345.momo") == (
            "kraken",
            "BTC-USD",
            "paper",
            "abcdef012345",
            "momo",
        )

    def test_tag_without_wallet_segment(self) -> None:
        """A non-wallet 4th segment is the strategy tag (legacy format).

        Given: a 4-segment key whose 4th part is not w+12hex,
        When: parsed,
        Then: it lands in strategy_tag with an empty wallet.
        """
        assert parse_shard_key("kraken.BTC-USD.paper.momo") == (
            "kraken",
            "BTC-USD",
            "paper",
            "",
            "momo",
        )

    def test_too_short_key_is_none(self) -> None:
        """A key with fewer than 3 segments is unparseable.

        Given: a 2-segment string,
        When: parsed,
        Then: None is returned.
        """
        assert parse_shard_key("kraken.BTC-USD") is None


class TestOrderRequestFromCommand:
    """Field-for-field reconstruction of the dispatch payload."""

    def test_reconstructs_identity_and_sizing(self) -> None:
        """The request mirrors the command row's identity and sizing.

        Given: a full create command row,
        When: the request is reconstructed,
        Then: identity, sizing, routing and the TRUE age anchor match.
        """
        cmd = _cmd()
        order = order_request_from_command(cmd)
        assert order.public_id == "cid-1"
        assert order.client_order_id == "cid-1"
        assert order.strategy_id == "strat-1"
        assert order.instrument == "BTC-USD"
        assert order.exchange == "kraken"
        assert order.side == "buy"
        assert order.order_type == "limit"
        assert order.quantity == 1.5
        assert order.price == 101.0
        assert order.leverage == 3
        assert order.reduce_only is True
        assert order.wallet_public_id == "wallet-1"
        assert order.operator_public_id == "op-1"
        assert order.signaled_at == cmd["created_at"]

    def test_strategy_tag_parsed_from_shard_key(self) -> None:
        """The tag survives the round trip through the shard key.

        Given: a paper command whose shard key carries a tag,
        When: the request is reconstructed,
        Then: strategy_tag is the shard key's tag segment.
        """
        order = order_request_from_command(
            _cmd(shard_key="kraken.BTC-USD.paper.momo", mode="paper")
        )
        assert order.strategy_tag == "momo"

    def test_unparseable_shard_key_yields_no_tag(self) -> None:
        """A corrupt shard key degrades to a tagless request.

        Given: a command row with a 2-segment shard key,
        When: the request is reconstructed,
        Then: strategy_tag is None.
        """
        order = order_request_from_command(_cmd(shard_key="bad-key"))
        assert order.strategy_tag is None

    def test_missing_wallet_falls_back_to_empty(self) -> None:
        """A row without a wallet reconstructs with the legacy sentinel.

        Given: a command row whose wallet_public_id is None,
        When: the request is reconstructed,
        Then: wallet_public_id is the empty string.
        """
        order = order_request_from_command(_cmd(wallet_public_id=None))
        assert order.wallet_public_id == ""

    def test_stop_order_carries_type_and_trigger(self) -> None:
        """A stop command reconstructs with CORE type and its trigger (#156).

        Given: a stop_limit command row with a persisted stop_price,
        When: the request is reconstructed,
        Then: order_type stays the CORE value and stop_price survives —
            the outbox publish AND the adoption sweeps carry the trigger.
        """
        order = order_request_from_command(
            _cmd(order_type="stop_limit", price=99.0, stop_price=100.5)
        )
        assert order.order_type == "stop_limit"
        assert order.price == 99.0
        assert order.stop_price == 100.5

    def test_row_without_stop_price_key_reconstructs_none(self) -> None:
        """A pre-#156 row shape (no stop_price key) degrades to None.

        Given: a command row dict lacking the stop_price key entirely,
        When: the request is reconstructed,
        Then: stop_price is None rather than a KeyError.
        """
        legacy = dict(_cmd())
        legacy.pop("stop_price", None)
        order = order_request_from_command(cast(TradeCommandRow, legacy))
        assert order.stop_price is None
