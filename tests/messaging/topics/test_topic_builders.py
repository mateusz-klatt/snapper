"""Tests for topic string builders and parsers."""

import pytest

from snapper.messaging.topics.builders import ParsedOrderTopic
from snapper.messaging.topics.builders import ParsedSignalTopic
from snapper.messaging.topics.builders import admin_topic
from snapper.messaging.topics.builders import heartbeat_topic
from snapper.messaging.topics.builders import is_order_topic
from snapper.messaging.topics.builders import market_topic
from snapper.messaging.topics.builders import order_command_topic
from snapper.messaging.topics.builders import order_commands_prefix
from snapper.messaging.topics.builders import order_event_topic
from snapper.messaging.topics.builders import order_events_prefix
from snapper.messaging.topics.builders import parse_market_topic
from snapper.messaging.topics.builders import parse_order_command_topic
from snapper.messaging.topics.builders import parse_order_event_topic
from snapper.messaging.topics.builders import parse_signal_topic
from snapper.messaging.topics.builders import signal_topic
from snapper.messaging.topics.builders import system_topic


class TestMarketTopic:
    """Tests for market_topic builder function."""

    def test_tick_topic_with_string_exchange(self) -> None:
        """Verify tick topic builds correctly with string exchange.

        Given: String exchange and instrument,
        When: Building tick topic,
        Then: Returns correctly formatted topic.
        """
        result = market_topic("kraken", "BTC-USD", "ticks")
        assert result == "market.kraken.BTC-USD.ticks"

    def test_tick_topic_various_exchanges(self) -> None:
        """Verify tick topic builds correctly with various exchanges.

        Given: Different exchange names,
        When: Building tick topics,
        Then: Returns correctly formatted topics.
        """
        assert market_topic("kraken", "ETH-USD", "ticks") == "market.kraken.ETH-USD.ticks"
        assert (
            market_topic("paper", "BTC-USD", "ticks", source_exchange="kraken")
            == "market.paper.kraken.BTC-USD.ticks"
        )
        assert market_topic("zonda", "BTC-PLN", "ticks") == "market.zonda.BTC-PLN.ticks"

    def test_trades_topic(self) -> None:
        """Verify trades topic builds correctly.

        Given: Exchange and instrument,
        When: Building trades topic,
        Then: Returns correctly formatted topic.
        """
        result = market_topic("zonda", "BTC-PLN", "trades")
        assert result == "market.zonda.BTC-PLN.trades"

    def test_candles_topic_with_timeframe(self) -> None:
        """Verify candles topic includes timeframe.

        Given: Exchange, instrument and timeframe,
        When: Building candles topic,
        Then: Returns topic with timeframe suffix.
        """
        result = market_topic("kraken", "BTC-USD", "candles", "1m")
        assert result == "market.kraken.BTC-USD.candles.1m"

    def test_candles_topic_various_timeframes(self) -> None:
        """Verify candles topic works with various timeframes.

        Given: Different timeframe values,
        When: Building candles topics,
        Then: Returns correct topics for each timeframe.
        """
        assert (
            market_topic("kraken", "BTC-USD", "candles", "5m") == "market.kraken.BTC-USD.candles.5m"
        )
        assert (
            market_topic("kraken", "BTC-USD", "candles", "1h") == "market.kraken.BTC-USD.candles.1h"
        )
        assert (
            market_topic("kraken", "BTC-USD", "candles", "1d") == "market.kraken.BTC-USD.candles.1d"
        )

    def test_candles_topic_requires_timeframe(self) -> None:
        """Verify candles topic raises error without timeframe.

        Given: Candles data type without timeframe,
        When: Building topic,
        Then: Raises ValueError.
        """
        with pytest.raises(ValueError, match="timeframe is required"):
            market_topic("kraken", "BTC-USD", "candles")

    def test_paper_topic_with_source_exchange(self) -> None:
        """Verify paper market topic includes source exchange segment.

        Given: Paper exchange and source exchange,
        When: Building market topic,
        Then: Topic includes source exchange in third segment.
        """
        result = market_topic("paper", "BTC-USD", "ticks", source_exchange="kraken")
        assert result == "market.paper.kraken.BTC-USD.ticks"

    def test_paper_candles_topic_with_source_exchange(self) -> None:
        """Verify paper candles topic includes source exchange and timeframe."""
        result = market_topic(
            "paper",
            "BTC-USD",
            "candles",
            timeframe="1m",
            source_exchange="polygon",
        )
        assert result == "market.paper.polygon.BTC-USD.candles.1m"

    def test_paper_candles_topic_with_source_requires_timeframe(self) -> None:
        """Verify paper candles topic with source raises without timeframe.

        Given: Paper exchange candles with source exchange but no timeframe,
        When: Building market topic,
        Then: Raises ValueError.
        """
        with pytest.raises(ValueError, match="timeframe is required"):
            market_topic("paper", "BTC-USD", "candles", source_exchange="kraken")

    def test_paper_market_topic_requires_source_exchange(self) -> None:
        """Verify paper market topic requires source exchange.

        Given: Paper exchange topic without source exchange,
        When: Building market topic,
        Then: Raises ValueError.
        """
        with pytest.raises(ValueError, match="source_exchange is required"):
            market_topic("paper", "BTC-USD", "ticks")


class TestOrderCommandTopic:
    """Tests for order_command_topic builder function."""

    def test_submit_command_string_exchange(self) -> None:
        """Verify submit command topic with string exchange.

        Given: String exchange and instrument,
        When: Building submit command topic,
        Then: Returns correctly formatted topic.
        """
        result = order_command_topic("kraken", "BTC-USD", "submit")
        assert result == "orders.commands.kraken.BTC-USD.submit"

    def test_submit_command_paper_exchange(self) -> None:
        """Verify submit command topic with paper exchange.

        Given: Paper exchange name,
        When: Building submit command topic,
        Then: Returns correctly formatted topic.
        """
        result = order_command_topic("paper", "ETH-USD", "submit")
        assert result == "orders.commands.paper.ETH-USD.submit"

    def test_cancel_command(self) -> None:
        """Verify cancel command topic builds correctly.

        Given: Exchange and instrument,
        When: Building cancel command topic,
        Then: Returns correctly formatted topic.
        """
        result = order_command_topic("kraken", "BTC-USD", "cancel")
        assert result == "orders.commands.kraken.BTC-USD.cancel"

    def test_replace_command(self) -> None:
        """Verify replace command topic builds correctly.

        Given: Exchange and instrument,
        When: Building replace command topic,
        Then: Returns correctly formatted topic.
        """
        result = order_command_topic("kraken", "BTC-USD", "replace")
        assert result == "orders.commands.kraken.BTC-USD.replace"


class TestOrderEventTopic:
    """Tests for order_event_topic builder function."""

    def test_submitted_event(self) -> None:
        """Verify submitted event topic builds correctly.

        Given: Exchange and instrument,
        When: Building submitted event topic,
        Then: Returns correctly formatted topic.
        """
        result = order_event_topic("kraken", "BTC-USD", "submitted")
        assert result == "orders.events.kraken.BTC-USD.submitted"

    def test_rejected_event(self) -> None:
        """Verify rejected event topic builds correctly.

        Given: Exchange and instrument,
        When: Building rejected event topic,
        Then: Returns correctly formatted topic.
        """
        result = order_event_topic("kraken", "BTC-USD", "rejected")
        assert result == "orders.events.kraken.BTC-USD.rejected"

    def test_fill_event(self) -> None:
        """Verify fill event topic builds correctly.

        Given: Exchange and instrument,
        When: Building fill event topic,
        Then: Returns correctly formatted topic.
        """
        result = order_event_topic("kraken", "BTC-USD", "executed")
        assert result == "orders.events.kraken.BTC-USD.executed"

    def test_cancelled_event(self) -> None:
        """Verify cancelled event topic builds correctly.

        Given: Exchange and instrument,
        When: Building cancelled event topic,
        Then: Returns correctly formatted topic.
        """
        result = order_event_topic("kraken", "BTC-USD", "cancelled")
        assert result == "orders.events.kraken.BTC-USD.cancelled"

    def test_expired_event(self) -> None:
        """Verify expired event topic builds correctly.

        Given: Exchange and instrument,
        When: Building expired event topic,
        Then: Returns correctly formatted topic.
        """
        result = order_event_topic("kraken", "BTC-USD", "expired")
        assert result == "orders.events.kraken.BTC-USD.expired"

    def test_replaced_event(self) -> None:
        """Verify replaced event topic builds correctly.

        Given: Exchange and instrument,
        When: Building replaced event topic,
        Then: Returns correctly formatted topic.
        """
        result = order_event_topic("kraken", "BTC-USD", "replaced")
        assert result == "orders.events.kraken.BTC-USD.replaced"

    def test_event_with_zonda_exchange(self) -> None:
        """Verify event topic with zonda exchange.

        Given: Zonda exchange name,
        When: Building event topic,
        Then: Returns correctly formatted topic.
        """
        result = order_event_topic("zonda", "BTC-PLN", "executed")
        assert result == "orders.events.zonda.BTC-PLN.executed"


class TestSignalTopic:
    """Tests for signal_topic builder function."""

    def test_live_signal_default(self) -> None:
        """Verify live signal topic is default.

        Given: Exchange and instrument without signal type,
        When: Building signal topic,
        Then: Returns topic with 'live' suffix.
        """
        result = signal_topic("kraken", "BTC-USD")
        assert result == "signals.kraken.BTC-USD.live"

    def test_custom_signal_type(self) -> None:
        """Verify custom signal type builds correctly.

        Given: Exchange, instrument and custom signal type,
        When: Building signal topic,
        Then: Returns topic with custom suffix.
        """
        result = signal_topic("paper", "ETH-USD", "momentum_strategy")
        assert result == "signals.paper.ETH-USD.momentum_strategy"

    def test_signal_with_paper_exchange(self) -> None:
        """Verify signal topic with paper exchange.

        Given: Paper exchange name,
        When: Building signal topic,
        Then: Returns correctly formatted topic.
        """
        result = signal_topic("paper", "BTC-USD", "backtest")
        assert result == "signals.paper.BTC-USD.backtest"


class TestHeartbeatTopic:
    """Tests for heartbeat_topic builder function."""

    def test_executor_heartbeat(self) -> None:
        """Verify executor heartbeat topic builds correctly.

        Given: Executor component and exchange name,
        When: Building heartbeat topic,
        Then: Returns correctly formatted topic.
        """
        result = heartbeat_topic("executor", "kraken")
        assert result == "system.heartbeats.executor.kraken"

    def test_feed_heartbeat(self) -> None:
        """Verify feed heartbeat topic builds correctly.

        Given: Feed component and provider name,
        When: Building heartbeat topic,
        Then: Returns correctly formatted topic.
        """
        result = heartbeat_topic("feed", "polygon")
        assert result == "system.heartbeats.feed.polygon"

    def test_strategy_heartbeat(self) -> None:
        """Verify strategy heartbeat topic builds correctly.

        Given: Strategy component and strategy name,
        When: Building heartbeat topic,
        Then: Returns correctly formatted topic.
        """
        result = heartbeat_topic("strategy", "macd_btc_1h")
        assert result == "system.heartbeats.strategy.macd_btc_1h"

    def test_executor_heartbeat_with_wallet_short_appends_5th_segment(self) -> None:
        """Phase 0c.7: per-wallet executor heartbeat appends wallet_short.

        Given: executor component + exchange + a 12-hex wallet_short,
        When: heartbeat_topic is built,
        Then: The topic gains a 5th segment so per-wallet instances
            publish on distinct heartbeat topics.
        """
        result = heartbeat_topic("executor", "kraken", wallet_short="019d6ca45f2e")
        assert result == "system.heartbeats.executor.kraken.019d6ca45f2e"

    def test_executor_heartbeat_empty_wallet_short_keeps_legacy_layout(self) -> None:
        """Empty wallet_short preserves the 4-segment legacy topic.

        The legacy single-wallet template path does not know its
        wallet at construction time and passes ``wallet_short=""``;
        the builder must not emit a trailing ``.`` segment in that
        case.
        """
        result = heartbeat_topic("executor", "kraken", wallet_short="")
        assert result == "system.heartbeats.executor.kraken"


class TestSystemTopic:
    """Tests for system_topic builder function."""

    def test_symbol_aliases_topic(self) -> None:
        """Verify symbol aliases topic builds correctly.

        Given: Symbol aliases type,
        When: Building system topic,
        Then: Returns correctly formatted topic.
        """
        result = system_topic("symbol_aliases")
        assert result == "system.symbol_aliases"

    def test_settings_topic(self) -> None:
        """Verify settings topic builds correctly.

        Given: Settings type,
        When: Building system topic,
        Then: Returns correctly formatted topic.
        """
        result = system_topic("settings")
        assert result == "system.settings"


class TestAdminTopic:
    """Tests for admin_topic builder function."""

    def test_command_topic(self) -> None:
        """Verify admin command topic builds correctly.

        Given: Command resource,
        When: Building admin topic,
        Then: Returns correctly formatted topic.
        """
        result = admin_topic("command")
        assert result == "admin.command"

    def test_users_topic(self) -> None:
        """Verify admin users topic builds correctly.

        Given: Users resource,
        When: Building admin topic,
        Then: Returns correctly formatted topic.
        """
        result = admin_topic("users")
        assert result == "admin.users"


class TestOrderPrefixes:
    """Tests for subscription prefix builder functions."""

    def test_commands_prefix_string_exchange(self) -> None:
        """Verify commands prefix with string exchange.

        Given: String exchange,
        When: Building commands prefix,
        Then: Returns correctly formatted prefix.
        """
        result = order_commands_prefix("kraken")
        assert result == "orders.commands.kraken."

    def test_commands_prefix_paper_exchange(self) -> None:
        """Verify commands prefix with paper exchange.

        Given: Paper exchange name,
        When: Building commands prefix,
        Then: Returns correctly formatted prefix.
        """
        result = order_commands_prefix("paper")
        assert result == "orders.commands.paper."

    def test_events_prefix_no_exchange(self) -> None:
        """Verify events prefix without exchange filter.

        Given: No exchange specified,
        When: Building events prefix,
        Then: Returns prefix for all exchanges.
        """
        result = order_events_prefix()
        assert result == "orders.events."

    def test_events_prefix_with_exchange(self) -> None:
        """Verify events prefix with exchange filter.

        Given: Specific exchange,
        When: Building events prefix,
        Then: Returns prefix for that exchange only.
        """
        result = order_events_prefix("kraken")
        assert result == "orders.events.kraken."

    def test_events_prefix_with_zonda_exchange(self) -> None:
        """Verify events prefix with zonda exchange.

        Given: Zonda exchange name,
        When: Building events prefix,
        Then: Returns correctly formatted prefix.
        """
        result = order_events_prefix("zonda")
        assert result == "orders.events.zonda."


class TestParseOrderCommandTopic:
    """Tests for parse_order_command_topic parser function."""

    def test_parse_valid_submit_topic(self) -> None:
        """Verify valid submit command topic is parsed correctly.

        Given: Valid submit command topic,
        When: Parsing the topic,
        Then: Returns ParsedOrderTopic with correct components.
        """
        result = parse_order_command_topic("orders.commands.kraken.BTC-USD.submit")
        assert result is not None
        assert result.exchange == "kraken"
        assert result.instrument == "BTC-USD"
        assert result.suffix == "submit"

    def test_parse_valid_cancel_topic(self) -> None:
        """Verify valid cancel command topic is parsed correctly.

        Given: Valid cancel command topic,
        When: Parsing the topic,
        Then: Returns ParsedOrderTopic with correct components.
        """
        result = parse_order_command_topic("orders.commands.paper.ETH-USD.cancel")
        assert result is not None
        assert result.exchange == "paper"
        assert result.instrument == "ETH-USD"
        assert result.suffix == "cancel"

    def test_parse_valid_replace_topic(self) -> None:
        """Verify valid replace command topic is parsed correctly.

        Given: Valid replace command topic,
        When: Parsing the topic,
        Then: Returns ParsedOrderTopic with correct components.
        """
        result = parse_order_command_topic("orders.commands.zonda.BTC-PLN.replace")
        assert result is not None
        assert result.exchange == "zonda"
        assert result.instrument == "BTC-PLN"
        assert result.suffix == "replace"

    def test_parse_malformed_short_topic(self) -> None:
        """Verify malformed topic with too few segments returns None.

        Given: Topic with fewer than 5 segments,
        When: Parsing the topic,
        Then: Returns None.
        """
        assert parse_order_command_topic("orders.commands.kraken") is None
        assert parse_order_command_topic("orders.commands") is None
        assert parse_order_command_topic("orders") is None

    def test_parse_wrong_category_returns_none(self) -> None:
        """Verify event topic returns None when parsed as command.

        Given: Event topic (not command),
        When: Parsing as command topic,
        Then: Returns None.
        """
        result = parse_order_command_topic("orders.events.kraken.BTC-USD.executed")
        assert result is None

    def test_parse_market_topic_returns_none(self) -> None:
        """Verify market topic returns None when parsed as command.

        Given: Market data topic,
        When: Parsing as command topic,
        Then: Returns None.
        """
        result = parse_order_command_topic("market.kraken.BTC-USD.ticks")
        assert result is None


class TestParseMarketTopic:
    """Tests for parse_market_topic parser function."""

    def test_parse_live_market_topic(self) -> None:
        """Verify live market topic parsing."""
        parsed = parse_market_topic("market.kraken.BTC-USD.candles.1m")
        assert parsed is not None
        assert parsed.exchange == "kraken"
        assert parsed.source_exchange is None
        assert parsed.instrument == "BTC-USD"
        assert parsed.data_type == "candles"
        assert parsed.timeframe == "1m"

    def test_parse_paper_market_topic_with_source_exchange(self) -> None:
        """Verify paper market topic with source exchange parsing."""
        parsed = parse_market_topic("market.paper.kraken.BTC-USD.trades")
        assert parsed is not None
        assert parsed.exchange == "paper"
        assert parsed.source_exchange == "kraken"
        assert parsed.instrument == "BTC-USD"
        assert parsed.data_type == "trades"
        assert parsed.timeframe is None

    def test_parse_legacy_paper_market_topic_returns_none(self) -> None:
        """Verify legacy paper market topic without source exchange is rejected."""
        parsed = parse_market_topic("market.paper.BTC-USD.ticks")
        assert parsed is None

    def test_parse_invalid_market_topic_returns_none(self) -> None:
        """Verify malformed market topic returns None."""
        assert parse_market_topic("market.paper.kraken.BTC-USD.candles") is None
        assert parse_market_topic("market.kraken.BTC-USD.ticks.1m") is None
        assert parse_market_topic("market.kraken.BTC-USD.unknown") is None
        assert parse_market_topic("signals.kraken.BTC-USD.live") is None
        assert parse_market_topic("market..BTC-USD.ticks") is None
        assert parse_market_topic("market.kraken.BTC-USD.ticks.extra.part") is None

    def test_parse_paper_market_topic_with_source_timeframe(self) -> None:
        """Verify paper market topic with source exchange and timeframe is parsed."""
        parsed = parse_market_topic("market.paper.kraken.BTC-USD.candles.1m")
        assert parsed is not None
        assert parsed.source_exchange == "kraken"
        assert parsed.instrument == "BTC-USD"
        assert parsed.data_type == "candles"
        assert parsed.timeframe == "1m"


class TestParseOrderEventTopic:
    """Tests for parse_order_event_topic parser function."""

    def test_parse_valid_fill_topic(self) -> None:
        """Verify valid fill event topic is parsed correctly.

        Given: Valid fill event topic,
        When: Parsing the topic,
        Then: Returns ParsedOrderTopic with correct components.
        """
        result = parse_order_event_topic("orders.events.kraken.BTC-USD.executed")
        assert result is not None
        assert result.exchange == "kraken"
        assert result.instrument == "BTC-USD"
        assert result.suffix == "executed"

    def test_parse_valid_accepted_topic(self) -> None:
        """Verify valid accepted event topic is parsed correctly.

        Given: Valid accepted event topic,
        When: Parsing the topic,
        Then: Returns ParsedOrderTopic with correct components.
        """
        result = parse_order_event_topic("orders.events.paper.ETH-USD.accepted")
        assert result is not None
        assert result.exchange == "paper"
        assert result.instrument == "ETH-USD"
        assert result.suffix == "accepted"

    def test_parse_valid_rejected_topic(self) -> None:
        """Verify valid rejected event topic is parsed correctly.

        Given: Valid rejected event topic,
        When: Parsing the topic,
        Then: Returns ParsedOrderTopic with correct components.
        """
        result = parse_order_event_topic("orders.events.zonda.BTC-PLN.rejected")
        assert result is not None
        assert result.exchange == "zonda"
        assert result.instrument == "BTC-PLN"
        assert result.suffix == "rejected"

    def test_parse_malformed_short_topic(self) -> None:
        """Verify malformed topic with too few segments returns None.

        Given: Topic with fewer than 5 segments,
        When: Parsing the topic,
        Then: Returns None.
        """
        assert parse_order_event_topic("orders.events.kraken") is None
        assert parse_order_event_topic("orders.events") is None

    def test_parse_wrong_category_returns_none(self) -> None:
        """Verify command topic returns None when parsed as event.

        Given: Command topic (not event),
        When: Parsing as event topic,
        Then: Returns None.
        """
        result = parse_order_event_topic("orders.commands.kraken.BTC-USD.submit")
        assert result is None


class TestIsOrderTopic:
    """Tests for is_order_topic helper function."""

    def test_order_event_topic_returns_true(self) -> None:
        """Verify order event topic is identified as order topic.

        Given: Valid order event topic,
        When: Checking if order topic,
        Then: Returns True.
        """
        assert is_order_topic("orders.events.kraken.BTC-USD.executed") is True
        assert is_order_topic("orders.events.paper.ETH-USD.accepted") is True

    def test_order_command_topic_returns_true(self) -> None:
        """Verify order command topic is identified as order topic.

        Given: Valid order command topic,
        When: Checking if order topic,
        Then: Returns True.
        """
        assert is_order_topic("orders.commands.kraken.BTC-USD.submit") is True
        assert is_order_topic("orders.commands.zonda.BTC-PLN.cancel") is True

    def test_market_topic_returns_false(self) -> None:
        """Verify market topic is not identified as order topic.

        Given: Market data topic,
        When: Checking if order topic,
        Then: Returns False.
        """
        assert is_order_topic("market.kraken.BTC-USD.ticks") is False
        assert is_order_topic("market.paper.kraken.ETH-USD.candles.1m") is False

    def test_signal_topic_returns_false(self) -> None:
        """Verify signal topic is not identified as order topic.

        Given: Signal topic,
        When: Checking if order topic,
        Then: Returns False.
        """
        assert is_order_topic("signals.kraken.BTC-USD.live") is False

    def test_system_topic_returns_false(self) -> None:
        """Verify system topic is not identified as order topic.

        Given: System topic,
        When: Checking if order topic,
        Then: Returns False.
        """
        assert is_order_topic("system.heartbeats.executor.kraken") is False
        assert is_order_topic("system.settings") is False

    def test_short_topic_returns_false(self) -> None:
        """Verify topic with too few segments returns False.

        Given: Topic with less than 2 segments,
        When: Checking if order topic,
        Then: Returns False.
        """
        assert is_order_topic("orders") is False
        assert is_order_topic("") is False


class TestParsedOrderTopicDataclass:
    """Tests for ParsedOrderTopic dataclass."""

    def test_frozen_dataclass(self) -> None:
        """Verify ParsedOrderTopic is immutable.

        Given: A ParsedOrderTopic instance,
        When: Attempting to modify it,
        Then: Raises FrozenInstanceError.
        """
        parsed = ParsedOrderTopic(exchange="kraken", instrument="BTC-USD", suffix="executed")
        with pytest.raises(AttributeError):
            parsed.exchange = "paper"

    def test_slots_dataclass(self) -> None:
        """Verify ParsedOrderTopic uses slots.

        Given: A ParsedOrderTopic instance,
        When: Checking __slots__,
        Then: Has expected attributes.
        """
        parsed = ParsedOrderTopic(exchange="kraken", instrument="BTC-USD", suffix="executed")
        assert hasattr(parsed, "__slots__")


class TestParseSignalTopic:
    """Tests for parse_signal_topic parser function."""

    def test_parse_signal_topic(self) -> None:
        """Verify valid signal topic is parsed."""
        parsed = parse_signal_topic("signals.kraken.BTC-USD.live")
        assert parsed is not None
        assert isinstance(parsed, ParsedSignalTopic)
        assert parsed.exchange == "kraken"
        assert parsed.instrument == "BTC-USD"
        assert parsed.signal_type == "live"

    def test_parse_signal_topic_invalid_returns_none(self) -> None:
        """Verify malformed signal topic returns None."""
        assert parse_signal_topic("signals.kraken.BTC-USD") is None
        assert parse_signal_topic("market.kraken.BTC-USD.ticks") is None
        assert parse_signal_topic("signals..BTC-USD.live") is None
